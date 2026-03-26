import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.server_bootstrap import (
    SERVER_BUNDLE_FILES,
    collect_local_bundle_files,
    ensure_server_bundle,
)


class FakeRemoteClient:
    def __init__(self, existing=None):
        self.existing = set(existing or [])
        self.uploaded = []
        self.commands = []
        self.created_dirs = []

    def exists(self, remote_path: str) -> bool:
        return remote_path in self.existing

    def makedirs(self, remote_path: str) -> None:
        self.created_dirs.append(remote_path)

    def put_file(self, local_path: Path, remote_path: str, mode=None) -> None:
        self.uploaded.append((str(local_path), remote_path, mode))
        self.existing.add(remote_path)

    def run(self, command: str, *, check: bool = True):
        self.commands.append((command, check))
        return 0, "", ""

    def close(self) -> None:
        return None


class ServerBootstrapTests(unittest.IsolatedAsyncioTestCase):
    def test_collect_local_bundle_files_reports_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            existing_relative = SERVER_BUNDLE_FILES[0]
            existing_path = root / existing_relative
            existing_path.parent.mkdir(parents=True, exist_ok=True)
            existing_path.write_text("ok", encoding="utf-8")

            local_files, missing = collect_local_bundle_files(root)

        self.assertEqual(len(local_files), 1)
        self.assertEqual(local_files[0].name, Path(existing_relative).name)
        self.assertIn(SERVER_BUNDLE_FILES[1], missing)

    async def test_ensure_server_bundle_uploads_missing_files_and_installs_units(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source_root = Path(tmpdir)
            for relative in SERVER_BUNDLE_FILES:
                path = source_root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(relative, encoding="utf-8")

            fake_remote = FakeRemoteClient()

            with patch("services.server_bootstrap.Config.ENABLE_SERVER_BOOTSTRAP", True), \
                 patch("services.server_bootstrap.Config.SERVER_BOOTSTRAP_SOURCE_ROOT", str(source_root)), \
                 patch("services.server_bootstrap.Config.SERVER_BOOTSTRAP_REMOTE_ROOT", "/remote/project"), \
                 patch("services.server_bootstrap.Config.SERVER_BOOTSTRAP_SSH_HOST", "77.239.115.146"), \
                 patch("services.server_bootstrap.Config.SERVER_BOOTSTRAP_SSH_USER", "root"), \
                 patch("services.server_bootstrap.Config.SERVER_BOOTSTRAP_SSH_PASSWORD", "secret"), \
                 patch("services.server_bootstrap.Config.SERVER_BOOTSTRAP_SSH_PORT", 22), \
                 patch("services.server_bootstrap.Config.SERVER_BOOTSTRAP_INSTALL_SYSTEMD", True):
                report = await ensure_server_bundle(remote_factory=lambda: fake_remote)

        self.assertEqual(report["status"], "ok")
        self.assertEqual(len(report["missingRemoteFiles"]), len(SERVER_BUNDLE_FILES))
        self.assertEqual(len(fake_remote.uploaded), len(SERVER_BUNDLE_FILES))
        self.assertTrue(any("install_systemd_units.sh" in item[0] for item in fake_remote.commands))


if __name__ == "__main__":
    unittest.main()
