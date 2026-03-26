# KKBOT PANEL V2.0

Новая версия бота собрана с нуля вокруг трёх принципов:

- `PostgreSQL-first`: рабочая база у бота одна, это PostgreSQL.
- `Legacy-safe`: при первом запуске старые данные из SQLite автоматически импортируются в PostgreSQL, но исходная SQLite-база не удаляется.
- `Mixed runtime`: основной бот и бизнес-логика на Python, быстрые операционные CLI и диагностика на Go.

Одновременно в `V2.0` уже встроен полный совместимый слой старого рабочего бота, чтобы переносить production-функции без остановки проекта.

## Что внутри

```text
src/kkbot/           Python runtime бота
src/app/             legacy V1-compatible runtime modules
src/services/        legacy business services
src/handlers/        legacy handlers
src/db/              legacy SQLite database layer
go/cmd/kkbotctl/     Go CLI для диагностики и operational checks
migrations/postgres/ SQL bootstrap и будущие PostgreSQL migrations
tests/               unit tests на конфиг и legacy migration
tests_legacy/        перенесённые тесты старого бота
```

## Архитектура

### Python

Используется там, где важны:

- Telegram bot runtime (`aiogram`)
- панельные API и HTTP-интеграции
- orchestration, startup, фоновые задачи
- миграция legacy SQLite -> PostgreSQL
- совместимый запуск старого полного контура

### Go

Используется там, где удобны:

- быстрые operational CLI
- диагностика окружения
- проверка доступности PostgreSQL / panel / API без поднятия всего бота

## Запуск

### 1. Подготовка окружения

```bash
cp .env.example .env
```

Рекомендуемый современный способ без `requirements.txt`:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e '.[dev]'
```

Если нужен и legacy-compatible runtime:

```bash
python -m pip install -e '.[dev,legacy]'
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
2. проверит, был ли уже импорт legacy SQLite,
3. если нет, импортирует данные из старой SQLite-базы в PostgreSQL,
4. отметит импорт как выполненный,
5. продолжит работу уже только с PostgreSQL.

SQLite-файл остаётся нетронутым.

## Два режима запуска

### Новый runtime V2.0

```bash
python -m kkbot
```

### Legacy-compatible runtime

```bash
PYTHONPATH=src python -m kkbot.tools.run_legacy_v1
```

Этот режим нужен, когда хочется сразу иметь весь старый рабочий функционал внутри нового проекта и переносить его в PostgreSQL-first слой без спешки.

### Что именно переносится из legacy SQLite

- пользователи
- активные подписки и их legacy-мета
- pending payments и статусная история
- заявки на вывод
- support tickets и support messages

При этом в PostgreSQL дополнительно сохраняются полные `legacy_*_archive` таблицы, чтобы старая информация не терялась даже после нормализации новой схемы.

## Go CLI

Сборка:

```bash
cd go
go build ./cmd/kkbotctl
```

Пример:

```bash
./kkbotctl doctor \
  --database-url "$DATABASE_URL" \
  --panel-url "$PANEL_BASE" \
  --legacy-sqlite "$LEGACY_SQLITE_PATH"
```

## Runbook

Операционные команды для локального окружения и VDS вынесены отдельно:

- [RUNBOOK.md](/Users/daniil/Documents/KKBOT%20PANEL/RELEASE%20V2.0/docs/RUNBOOK.md)

## Важное ограничение текущего шага

Эта версия — новый production-ready фундамент:

- PostgreSQL bootstrap готов,
- legacy import в новую доменную схему готов,
- новый runtime skeleton готов,
- operational Go CLI готов.

И дополнительно:

- старый рабочий бот целиком перенесён в совместимый слой внутри `V2.0`,
- можно запускать либо новый runtime, либо legacy-compatible runtime,
- перенос фич теперь можно делать пакетами, а не переписывать всё одним большим рисковым шагом.

Дальше в него можно переносить ваши конкретные бизнес-фичи бота без повторного таскания SQLite-логики.
