# KKBOT PANEL V2.0

Новая версия бота работает только через PostgreSQL.

SQLite больше не используется как runtime-база:
- при первом запуске данные импортируются в PostgreSQL;
- исходный SQLite-файл можно сохранить как архив;
- дальше бот работает только с PostgreSQL.

## Что внутри

```text
src/kkbot/           Python runtime бота
src/app/             runtime orchestration и background jobs
src/services/        бизнес-сервисы
src/handlers/        Telegram handlers
src/db/              PostgreSQL runtime database layer
go/cmd/kkbotctl/     Go CLI для диагностики и operational checks
migrations/postgres/ SQL bootstrap и PostgreSQL migrations
tests/               unit tests и smoke-проверки runtime
```

## Архитектура

### Python

Используется там, где важны:
- Telegram bot runtime (`aiogram`)
- панельные API и HTTP-интеграции
- orchestration, startup, фоновые задачи
- импорт SQLite -> PostgreSQL

### Go

Используется там, где удобны:
- быстрые operational CLI
- диагностика окружения
- проверка доступности PostgreSQL / panel / API

## Запуск

### 1. Подготовка окружения

```bash
cp .env.example .env
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e '.[dev]'
```

### 2. Поднять PostgreSQL

```bash
docker compose up -d postgres
```

### 3. Первый запуск

```bash
python -m kkbot
```

Если заданы:
- `DATABASE_URL`
- `LEGACY_SQLITE_PATH`
- `AUTO_MIGRATE_LEGACY=true`

то бот при первом запуске:
1. применит PostgreSQL migrations,
2. проверит, был ли уже импорт SQLite,
3. если нет, импортирует данные в PostgreSQL,
4. отметит импорт как выполненный,
5. продолжит работу уже только с PostgreSQL.

## Что именно переносится из SQLite

- пользователи
- активные подписки и их legacy-мета
- pending payments и статусная история
- заявки на вывод
- support tickets и support messages

При этом в PostgreSQL дополнительно сохраняются `legacy_*_archive` таблицы, чтобы старая информация не терялась после нормализации новой схемы.

## Go CLI

```bash
cd go
go build ./cmd/kkbotctl
./kkbotctl doctor --database-url "$DATABASE_URL" --panel-url "$PANEL_BASE" --legacy-sqlite "$LEGACY_SQLITE_PATH"
```

## Runbook

- [RUNBOOK.md](/Users/daniil/Documents/KKBOT%20PANEL/RELEASE%20V2.0/docs/RUNBOOK.md)

## Текущее состояние

- PostgreSQL bootstrap готов
- SQLite import в новую схему готов
- runtime работает только через PostgreSQL
- operational Go CLI готов

Дальше проект можно чистить и развивать уже без возврата к SQLite runtime.
