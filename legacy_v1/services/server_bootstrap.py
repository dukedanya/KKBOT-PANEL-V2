from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Protocol

from config import BASE_DIR, Config

logger = logging.getLogger(__name__)


SERVER_BUNDLE_FILES: tuple[str, ...] = (
    "server/app.py",
    "server/build_merged_subscription.py",
    "server/panel_client.py",
    "server/env_helpers.py",
    "server/happ_whitelist.py",
    "server/update_happ_whitelist.py",
    "server/aggregate_total_traffic.py",
    "server/sync_live_slots.py",
    "server/ensure_grace_inbounds.py",
    "server/configure_grace_ports.sh",
    "server/run_api_from_env.sh",
    "server/run_slot_sync_from_env.sh",
    "server/run_total_traffic_sync_from_env.sh",
    "server/install_systemd_units.sh",
    "server/vds.env.example",
    "deploy/systemd/lte-report-api.service",
    "deploy/systemd/lte-slot-sync.service",
    "deploy/systemd/lte-slot-sync.timer",
    "deploy/systemd/lte-total-traffic-sync.service",
    "deploy/systemd/lte-total-traffic-sync.timer",
    "deploy/systemd/lte-happ-whitelist-update.service",
    "deploy/systemd/lte-happ-whitelist-update.timer",
    "deploy/systemd/lte-relay-xray.service",
    "deploy/systemd/lte-grace-shaping.service",
)


class RemoteClient(Protocol):
    def exists(self, remote_path: str) -> bool: ...
    def makedirs(self, remote_path: str) -> None: ...
    def put_file(self, local_path: Path, remote_path: str, mode: int | None = None) -> None: ...
    def run(self, command: str, *, check: bool = True) -> tuple[int, str, str]: ...
    def close(self) -> None: ...


class ParamikoRemoteClient:
    def __init__(self, *, host: str, port: int, username: str, password: str) -> None:
        import paramiko

        self._paramiko = paramiko
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(
            hostname=host,
            port=port,
            username=username,
            password=password,
            look_for_keys=False,
            allow_agent=False,
            timeout=15,
        )
        self._sftp = self._ssh.open_sftp()

    def exists(self, remote_path: str) -> bool:
        try:
            self._sftp.stat(remote_path)
            return True
        except OSError:
            return False

    def makedirs(self, remote_path: str) -> None:
        normalized = remote_path.strip("/")
        current = "/"
        for part in normalized.split("/"):
            current = os.path.join(current, part)
            try:
                self._sftp.stat(current)
            except OSError:
                self._sftp.mkdir(current)

    def put_file(self, local_path: Path, remote_path: str, mode: int | None = None) -> None:
        parent = str(Path(remote_path).parent)
        self.makedirs(parent)
        self._sftp.put(str(local_path), remote_path)
        if mode is not None:
            self._sftp.chmod(remote_path, mode)

    def run(self, command: str, *, check: bool = True) -> tuple[int, str, str]:
        stdin, stdout, stderr = self._ssh.exec_command(command)
        if stdin:
            stdin.close()
        exit_status = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        if check and exit_status != 0:
            raise RuntimeError(f"Remote command failed ({exit_status}): {command}\n{err or out}")
        return exit_status, out, err

    def close(self) -> None:
        try:
            self._sftp.close()
        finally:
            self._ssh.close()


def default_server_bootstrap_source_root() -> Path:
    return (Path(BASE_DIR).resolve().parent / "WHITE LIST").resolve()


def resolve_server_bootstrap_source_root() -> Path:
    raw = (Config.SERVER_BOOTSTRAP_SOURCE_ROOT or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return default_server_bootstrap_source_root()


def collect_local_bundle_files(source_root: Path) -> tuple[list[Path], list[str]]:
    local_files: list[Path] = []
    missing: list[str] = []
    for relative in SERVER_BUNDLE_FILES:
        local_path = source_root / relative
        if local_path.exists():
            local_files.append(local_path)
        else:
            missing.append(relative)
    return local_files, missing


def upload_missing_bundle(remote: RemoteClient, *, source_root: Path, remote_root: str, missing_relatives: Iterable[str]) -> list[str]:
    uploaded: list[str] = []
    for relative in missing_relatives:
        local_path = source_root / relative
        remote_path = str(Path(remote_root) / relative)
        mode = 0o755 if local_path.suffix == ".sh" else None
        remote.put_file(local_path, remote_path, mode=mode)
        uploaded.append(relative)
    return uploaded


def install_remote_units_if_needed(remote: RemoteClient, *, remote_root: str) -> tuple[int, str, str]:
    command = (
        f"cd {remote_root} && "
        "chmod +x server/*.sh && "
        "bash server/install_systemd_units.sh && "
        "systemctl daemon-reload"
    )
    return remote.run(command, check=False)


async def ensure_server_bundle(
    *,
    remote_factory=None,
) -> dict:
    if not Config.ENABLE_SERVER_BOOTSTRAP:
        return {"status": "skipped", "reason": "disabled"}

    source_root = resolve_server_bootstrap_source_root()
    if not source_root.exists():
        return {
            "status": "fail",
            "reason": f"source root not found: {source_root}",
        }

    local_files, local_missing = collect_local_bundle_files(source_root)
    if local_missing:
        return {
            "status": "fail",
            "reason": "local bootstrap bundle incomplete",
            "missingLocalFiles": local_missing,
        }

    host = (Config.SERVER_BOOTSTRAP_SSH_HOST or "").strip()
    user = (Config.SERVER_BOOTSTRAP_SSH_USER or "").strip()
    password = Config.SERVER_BOOTSTRAP_SSH_PASSWORD
    port = int(Config.SERVER_BOOTSTRAP_SSH_PORT or 22)
    remote_root = (Config.SERVER_BOOTSTRAP_REMOTE_ROOT or "/root/lte-whitelist").strip().rstrip("/")
    if not host or not user or not password:
        return {
            "status": "warn",
            "reason": "missing ssh credentials",
            "sourceRoot": str(source_root),
        }

    def _run() -> dict:
        client = remote_factory() if remote_factory else ParamikoRemoteClient(
            host=host,
            port=port,
            username=user,
            password=password,
        )
        try:
            client.makedirs(remote_root)
            missing_remote = [
                relative
                for relative in SERVER_BUNDLE_FILES
                if not client.exists(str(Path(remote_root) / relative))
            ]
            uploaded: list[str] = []
            install_result = (0, "", "")
            if missing_remote:
                uploaded = upload_missing_bundle(
                    client,
                    source_root=source_root,
                    remote_root=remote_root,
                    missing_relatives=missing_remote,
                )
                if Config.SERVER_BOOTSTRAP_INSTALL_SYSTEMD:
                    install_result = install_remote_units_if_needed(client, remote_root=remote_root)

            return {
                "status": "ok",
                "sourceRoot": str(source_root),
                "remoteRoot": remote_root,
                "checkedFiles": len(SERVER_BUNDLE_FILES),
                "missingRemoteFiles": missing_remote,
                "uploadedFiles": uploaded,
                "systemdInstallExit": install_result[0],
            }
        finally:
            client.close()

    try:
        return await asyncio.to_thread(_run)
    except Exception as exc:
        logger.error("ensure_server_bundle failed: %s", exc)
        return {"status": "fail", "reason": str(exc), "sourceRoot": str(source_root)}
