# KKBOT PANEL V2.0 Runbook

Короткая шпаргалка для локальной работы и VDS.

## Локально

### Поднять PostgreSQL

```bash
cd "/Users/daniil/Documents/KKBOT PANEL/RELEASE V2.0"
docker compose up -d postgres
```

### Установить зависимости

```bash
cd "/Users/daniil/Documents/KKBOT PANEL/RELEASE V2.0"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e '.[dev]'
```

### Прогнать миграцию SQLite вручную

```bash
cd "/Users/daniil/Documents/KKBOT PANEL/RELEASE V2.0"
source .venv/bin/activate
PYTHONPATH=src python -m kkbot.tools.migrate_legacy
```

### Запустить runtime

```bash
cd "/Users/daniil/Documents/KKBOT PANEL/RELEASE V2.0"
source .venv/bin/activate
python -m kkbot
```

### Тесты

```bash
cd "/Users/daniil/Documents/KKBOT PANEL/RELEASE V2.0"
source .venv/bin/activate
PYTHONPATH=src python -m unittest discover -s tests -v
```

## Ботовая VDS

Текущая машина:
- host: `77.239.115.146`
- project dir: `/root/kkvpnbot`
- service: `kkvpnbot-v2.service`

### Проверить статус

```bash
systemctl status kkvpnbot-v2.service --no-pager --full
journalctl -u kkvpnbot-v2.service -n 100 --no-pager
```

### Перезапустить

```bash
systemctl restart kkvpnbot-v2.service
```

### Проверить PostgreSQL

```bash
systemctl status postgresql --no-pager
su - postgres -c "psql -d kkbot -c 'select count(*) from bot_users;'"
su - postgres -c "psql -d kkbot -c \"select key, value from app_meta where key='legacy_sqlite_import';\""
```

### Убедиться, что SQLite больше не используется runtime

```bash
pid=$(systemctl show -p MainPID --value kkvpnbot-v2.service)
ls -l /proc/$pid/fd | grep users.db
```

Ожидаемый результат: пусто.

## Healthy state

Нормальный healthy-state для `V2.0`:
- `kkvpnbot-v2.service` = `active (running)`
- в логах есть:
  - `PostgreSQL migrations applied`
  - `Bot runtime starting on PostgreSQL`
- в PostgreSQL есть данные в:
  - `bot_users`
  - `subscriptions`
  - `payment_intents`

## Частые проблемы

### Бот не стартует из-за PostgreSQL

```bash
systemctl status postgresql --no-pager
su - postgres -c "psql -lqt"
```

### Бот снова импортирует SQLite

```bash
su - postgres -c "psql -d kkbot -c \"select key, value from app_meta where key='legacy_sqlite_import';\""
```

Если `completed` пропал или `false`, значит сломан meta-state или подключение идёт не в ту базу.
