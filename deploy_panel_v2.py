from pathlib import Path
import textwrap

import paramiko


HOST = "77.239.115.146"
USER = "root"
PASSWORD = "zDhE6uRK16be"
LOCAL_TAR = Path("/Users/daniil/Documents/KKBOT PANEL/RELEASE V2.0/release_v2_remote.tar.gz")


SERVICE_TEXT = textwrap.dedent(
    """
    [Unit]
    Description=KKBOT PANEL V2.0 on panel VDS
    After=network.target postgresql.service
    Wants=postgresql.service

    [Service]
    Type=simple
    WorkingDirectory=/root/kkvpnbot
    ExecStart=/root/kkvpnbot/venv/bin/kkbot-v2
    Restart=always
    RestartSec=3
    User=root
    Environment=PYTHONUNBUFFERED=1
    Environment=PYTHONPATH=/root/kkvpnbot/src

    [Install]
    WantedBy=multi-user.target
    """
).lstrip()


ENV_TEXT = textwrap.dedent(
    """
    BOT_TOKEN=8657485502:AAEveICltTEuqAIn357E9AUOJQzFQLJElxI
    ADMIN_USER_IDS=794419497
    VERIFY_SSL=false
    PANEL_BASE=https://kakoivpn.ru/A8GGbEuizKONIMbY3e/
    SUB_PANEL_BASE=
    PANEL_LOGIN=dukedanya
    PANEL_PASSWORD=denisdanil23
    PANEL_EMAIL_DOMAIN=kakoitovpn
    APP_ENV=production
    LOG_LEVEL=INFO
    DATA_DIR=/data
    DATA_FILE=/data/users.db
    LEGACY_SQLITE_PATH=/data/users.db
    AUTO_MIGRATE_LEGACY=true
    LEGACY_IMPORT_BATCH_SIZE=1000
    DATABASE_URL=postgresql://kkbot:kkbot@127.0.0.1:5432/kkbot
    DATABASE_MIN_POOL=1
    DATABASE_MAX_POOL=5
    WEBHOOK_ENABLED=false
    WEBHOOK_HOST=http://77.239.115.146:8080/itpay/webhook
    WEBHOOK_PORT=8080
    SITE_URL=https://t.me/+XsoxseRgJa8yN2Ni
    TG_CHANNEL=https://t.me/+XsoxseRgJa8yN2Ni
    SUPPORT_URL=https://t.me/zeus_danya
    REF_BONUS_DAYS=7
    REF_PERCENT_LEVEL1=25
    REF_PERCENT_LEVEL2=10
    REF_PERCENT_LEVEL3=5
    MIN_WITHDRAW=300
    ITPAY_PUBLIC_ID=pk_51607_69c9f69a
    ITPAY_API_SECRET=c78e9b15-fd03-438d-bbfe-87c1b06a8758
    ITPAY_WEBHOOK_SECRET=c78e9b15-fd03-438d-bbfe-87c1b06a8758
    ITPAY_PUBLIC_BASE_FALLBACK=http://77.239.115.146
    TOTAL_TRAFFIC_STATE_URL=http://127.0.0.1:8787/state/total-traffic
    GRACE_STATE_URL=http://127.0.0.1:8787/state/grace
    MERGED_SUBSCRIPTION_API_BASE=https://connect.kakoivpn.ru
    HAPP_SUBSCRIPTION_API_BASE=https://connect.kakoivpn.ru
    """
).lstrip()


def run(cli: paramiko.SSHClient, cmd: str, *, timeout: int = 3600, allow_fail: bool = False) -> None:
    print(f"--- RUN: {cmd}")
    stdin, stdout, stderr = cli.exec_command(cmd, get_pty=True, timeout=timeout)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    if out:
        print(out)
    if err:
        print("--- STDERR ---")
        print(err)
    code = stdout.channel.recv_exit_status()
    print(f"--- EXIT: {code}")
    if code != 0 and not allow_fail:
        raise RuntimeError(f"failed: {cmd} -> {code}")


def main() -> int:
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(HOST, username=USER, password=PASSWORD, timeout=20)
    try:
        sftp = cli.open_sftp()
        sftp.put(str(LOCAL_TAR), "/root/release_v2_remote.tar.gz")
        with sftp.open("/root/kkvpnbot-v2.service", "w") as f:
            f.write(SERVICE_TEXT)
        with sftp.open("/root/kkvpnbot.v2.env", "w") as f:
            f.write(ENV_TEXT)
        sftp.close()

        run(cli, "apt-get update", timeout=2400)
        run(
            cli,
            "DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql python3-venv python3-pip build-essential libpq-dev",
            timeout=3600,
        )
        run(cli, "systemctl enable postgresql && systemctl start postgresql")
        run(
            cli,
            "su - postgres -c \"psql -tc \\\"SELECT 1 FROM pg_roles WHERE rolname='kkbot'\\\"\" | grep -q 1 || "
            "su - postgres -c \"psql -c \\\"CREATE ROLE kkbot WITH LOGIN PASSWORD 'kkbot' SUPERUSER;\\\"\"",
        )
        run(
            cli,
            "su - postgres -c \"psql -lqt\" | cut -d '|' -f 1 | tr -d ' ' | grep -qx kkbot || "
            "su - postgres -c \"createdb -O kkbot kkbot\"",
        )
        run(
            cli,
            "python3 - <<'INNER'\n"
            "from pathlib import Path\n"
            "import shutil\n"
            "from datetime import datetime, timezone\n"
            "base = Path('/root/kkvpnbot')\n"
            "if base.exists():\n"
            "    ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')\n"
            "    shutil.copytree(base, Path(f'/root/kkvpnbot.backup_{ts}'))\n"
            "    shutil.rmtree(base)\n"
            "base.mkdir(parents=True, exist_ok=True)\n"
            "INNER",
        )
        run(cli, "tar xzf /root/release_v2_remote.tar.gz -C /root/kkvpnbot")
        run(
            cli,
            "cp /root/kkvpnbot.v2.env /root/kkvpnbot/.env && "
            "cp /root/kkvpnbot-v2.service /etc/systemd/system/kkvpnbot-v2.service",
        )
        run(cli, "mkdir -p /root/kkvpnbot/src/data && cp /root/kakoi/data/tarifs.json /root/kkvpnbot/src/data/tarifs.json")
        run(cli, "python3 -m venv /root/kkvpnbot/venv")
        run(cli, ". /root/kkvpnbot/venv/bin/activate && python -m pip install --upgrade pip setuptools wheel", timeout=1800)
        run(cli, ". /root/kkvpnbot/venv/bin/activate && cd /root/kkvpnbot && python -m pip install -e .", timeout=3600)
        run(cli, "systemctl daemon-reload && systemctl enable kkvpnbot-v2.service && systemctl restart kkvpnbot-v2.service")
        run(cli, "sleep 8 && systemctl --no-pager --full status kkvpnbot-v2.service")
        run(cli, "journalctl -u kkvpnbot-v2.service -n 120 --no-pager", allow_fail=True)
        run(cli, "systemctl disable --now kakoivpn.service")
        run(cli, "systemctl --no-pager --full status kakoivpn.service", allow_fail=True)
        return 0
    finally:
        cli.close()


if __name__ == "__main__":
    raise SystemExit(main())
