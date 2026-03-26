# KKBOT PANEL V2.0 Runbook

Короткая шпаргалка для локальной работы и для VDS.

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
python -m pip install -e '.[dev,legacy]'
```

### Прогнать миграцию legacy SQLite вручную

```bash
cd "/Users/daniil/Documents/KKBOT PANEL/RELEASE V2.0"
source .venv/bin/activate
PYTHONPATH=src python -m kkbot.tools.migrate_legacy
```

### Запустить новый runtime

```bash
cd "/Users/daniil/Documents/KKBOT PANEL/RELEASE V2.0"
source .venv/bin/activate
python -m kkbot
```

### Запустить legacy-compatible runtime

```bash
cd "/Users/daniil/Documents/KKBOT PANEL/RELEASE V2.0"
source .venv/bin/activate
PYTHONPATH=src python -m kkbot.tools.run_legacy_v1
```

### Тесты

```bash
cd "/Users/daniil/Documents/KKBOT PANEL/RELEASE V2.0"
source .venv/bin/activate
PYTHONPATH=src python -m unittest discover -s tests -v
```

## Ботовая VDS

Текущая целевая машина:

- host: `168.222.253.22`
- project dir: `/root/111`
- service: `kkbot-v2.service`

### Проверить статус

```bash
systemctl status kkbot-v2.service --no-pager --full
journalctl -u kkbot-v2.service -n 100 --no-pager
```

### Перезапустить

```bash
systemctl restart kkbot-v2.service
```

### Остановить

```bash
systemctl stop kkbot-v2.service
```

### Проверить PostgreSQL

```bash
systemctl status postgresql --no-pager
su - postgres -c "psql -d kkbot -c 'select count(*) from bot_users;'"
su - postgres -c "psql -d kkbot -c \"select key, value from app_meta where key='legacy_sqlite_import';\""
```

### Проверить, что импорт уже завершён

Ожидаем в `app_meta`:

- `legacy_sqlite_import.completed = true`

Если это так, повторный старт бота не должен снова тянуть SQLite в PostgreSQL.

## Бэкапы старой версии на VDS

Перед развёртыванием `V2.0` старый `/root/111` был сохранён в директории вида:

- `/root/111.backup_YYYYMMDDTHHMMSSZ`

### Быстрый откат

```bash
systemctl stop kkbot-v2.service
rm -rf /root/111
cp -a /root/111.backup_YYYYMMDDTHHMMSSZ /root/111
systemctl daemon-reload
systemctl restart kkbot-v2.service
```

Перед откатом проверьте, что в откатной папке лежит старая рабочая структура проекта.

## Что считать успешным состоянием

Нормальный healthy-state для `V2.0`:

- `kkbot-v2.service` = `active (running)`
- в логах есть:
  - `PostgreSQL migrations applied`
  - `Legacy import already completed`
  - `Run polling for bot`
- в PostgreSQL есть данные в:
  - `bot_users`
  - `subscriptions`
  - `payment_intents`

## Частые проблемы

### Бот не стартует из-за PostgreSQL

Проверьте:

```bash
systemctl status postgresql --no-pager
su - postgres -c "psql -lqt"
```

### Бот снова импортирует SQLite

Проверьте:

```bash
su - postgres -c "psql -d kkbot -c \"select key, value from app_meta where key='legacy_sqlite_import';\""
```

Если `completed` пропал или `false`, значит был сломан meta-state или подключение шло не в ту базу.

### Нужно понять, какая база реально используется

Проверьте `DATABASE_URL` в:

```bash
cat /root/111/.env
```

И отдельно убедитесь, что сервис читает именно `/root/111/.env`.
