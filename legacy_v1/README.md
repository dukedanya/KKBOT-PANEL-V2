# kakoito

Telegram-бот на `aiogram 3` с оплатами через ITPAY или YooKassa, управлением подписками, реферальной системой, миграциями SQLite и health-monitoring.

## Что изменено в foundation-итерациях

Сейчас в проект уже внесены три слоя укрепления:

- `main.py` стал тонкой точкой входа;
- логирование, bootstrap, runtime, dispatcher, container и background jobs вынесены в `app/`;
- добавлены `.env.example`, `Dockerfile`, `docker-compose.yml`, `Makefile`;
- введён явный `APP_MODE`;
- background jobs включаются и выключаются через feature flags;
- конфиг разбит на typed-секции (`runtime`, `logging`, `jobs`, `limits`) без ломки существующего API `Config.*`;
- lifecycle приложения собран через `lifespan()` с централизованным закрытием ресурсов;
- webhook-режим больше не запускает Telegram polling внутри себя.

Бизнес-логика и маршруты при этом не переписывались радикально: задача этих итераций — укрепить фундамент и подготовить проект к дальнейшему рефакторингу.

## Структура

```text
app/            # bootstrap, runtime, logging, dispatcher, background jobs
handlers/       # aiogram routers
services/       # интеграции и бизнес-сервисы
db/             # SQLite database layer
middlewares/    # middleware
migrations/     # SQL migrations
tests/          # unit tests
main.py         # thin entrypoint
```

## Локальный запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

## VPN panel и Happ subscription

Для живой multi-inbound схемы бот берёт все параметры из `.env`:

- `PANEL_BASE` — URL 3x-ui панели
- `PANEL_LOGIN`
- `PANEL_PASSWORD`
- `PANEL_TARGET_INBOUND_IDS` — список inbound ID через запятую
- `PANEL_TARGET_INBOUND_COUNT` — `0` значит использовать все ID из списка
- `PANEL_EMAIL_DOMAIN` — домен email в панели, например `kakoitovpn`
- `PANEL_EMAIL_PREFIX` — опциональный префикс email в панели, можно оставить пустым
- `SUB_PANEL_BASE` — базовая 3x-ui subscription URL
- `MERGED_SUBSCRIPTION_API_BASE` — URL merged subscription API для Happ
- `MERGED_SUBSCRIPTION_FORMAT` — `base64` или `plain`
- `MERGED_SUBSCRIPTION_INCLUDE_BASE_URL` — подмешивать ли обычную 3x-ui subscription в merged endpoint
- `ITPAY_PUBLIC_BASE_FALLBACK` — запасной публичный base URL для webhook-ссылок ITPAY, если `SITE_URL` и `WEBHOOK_HOST` не заданы
- `BACKUP_DIR` — каталог бэкапов SQLite
- `BACKUP_KEEP` — сколько последних бэкапов хранить

При запуске бот теперь сам проверяет рабочий `.env`. Если каких-то переменных не хватает, он дописывает их из `.env.example` и профильных `.env.*.example`, не изменяя уже существующие значения.
- `LTE_REPORT_API_HEALTH_URL` — health endpoint серверной merged/LTE части
- `TOTAL_TRAFFIC_STATE_PATH` — путь до общего traffic-state JSON
- `GRACE_STATE_PATH` — путь до grace-state JSON
- `TOTAL_TRAFFIC_STATE_MAX_AGE_SEC` — насколько свежим считать общий traffic-state

Текущая схема email и подписки:

- email клиента в панели: `<telegram_id>@<PANEL_EMAIL_DOMAIN>`
- `subId`: `user<telegram_id>`
- пользователю отдаётся merged Happ subscription URL `/sub/<uuid>`, а не одиночный `vless://`

Бот в кабинете пользователя и health-check теперь ориентируется не только на `clientStats` панели, но и на общий traffic/grace state от серверной LTE-части. Это позволяет показывать реальный общий трафик и текущий режим доступа `normal / grace / disabled`.

## Тесты

```bash
python -m unittest discover -s tests -v
```

## Docker

```bash
docker compose up --build
```

По умолчанию контейнер использует polling для Telegram-бота. При `APP_MODE=webhook` приложение поднимает встроенный HTTP-сервер для webhook провайдера на `8080` и работает без Telegram polling.

## Следующий рекомендуемый шаг

После третьей итерации логично переходить к operational-quality слою:

- smoke/integration тесты на startup, migrations и recovery-сценарии;
- более строгий health/readiness probe;
- постепенное удаление скрытых глобальных зависимостей из helper-уровня;
- затем уже рефакторинг платёжного контура и подписок.


## Operational endpoints

When `APP_MODE=webhook` and `ENABLE_HEALTH_ENDPOINTS=true`, the aiohttp listener also exposes:

- `GET ${HEALTHCHECK_PATH}` — lightweight liveness probe
- `GET ${READINESS_PATH}` — readiness probe with database/panel/itpay snapshot

Useful env flags:

- `STARTUP_RECOVER_STALE_PROCESSING=true` — requeue stale `processing` payments on startup
- `STARTUP_FAIL_ON_PENDING_MIGRATIONS=false` — optionally fail startup when unapplied SQL migrations remain
- `STARTUP_FAIL_ON_SCHEMA_DRIFT=false` — optionally fail startup when required DB columns are missing
- `RELEASE_PROFILE_ENFORCED=true` — strict production guardrails for startup and config

`/readyz` now also includes `schema_issues` and `pending_migrations`.

## Stability middleware

В dispatcher включены дополнительные защитные middlewares:

- rate limit на команды и callback (`COMMAND_RATE_LIMIT_SEC`, `CALLBACK_RATE_LIMIT_SEC`);
- anti-double-click dedup callback (`CALLBACK_DEDUP_WINDOW_SEC`);
- глобальный `error_guard`, который логирует необработанные исключения и шлёт алерт админам с cooldown (`ERROR_ALERT_COOLDOWN_SEC`).

## Backup и restore-check

Создание backup:

```bash
make backup-db
```

Проверка восстановления backup:

```bash
make verify-backup BACKUP_FILE=/abs/path/to/users-backup-....sqlite3.gz
```

Также добавлен профиль безопасного прод-окружения: `.env.release.example`.


## Платёжные провайдеры

Поддерживаются два backend-а оплаты:

- `PAYMENT_PROVIDER=itpay`
- `PAYMENT_PROVIDER=yookassa`

### ITPAY

Нужны переменные:

- `ITPAY_PUBLIC_ID`
- `ITPAY_API_SECRET`
- `ITPAY_WEBHOOK_SECRET`
- `SITE_URL` для построения webhook URL в запросе создания платежа

### YooKassa

Нужны переменные:

- `YOOKASSA_SHOP_ID`
- `YOOKASSA_SECRET_KEY`
- `YOOKASSA_RETURN_URL` или `TG_CHANNEL`
- `YOOKASSA_WEBHOOK_PATH`

Для YooKassa приложение создаёт платеж через API и использует `confirmation_url` как ссылку для перехода к оплате. HTTP-уведомления для Basic Auth настраиваются в личном кабинете YooKassa и должны указывать на `https://<ваш-домен>${YOOKASSA_WEBHOOK_PATH}`.


## Telegram Stars

- `PAYMENT_PROVIDER=telegram_stars` включает оплату цифровых подписок через Telegram Stars. Для цифровых товаров Telegram требует использовать Stars с валютой `XTR`. Провайдер `provider_token` для таких инвойсов можно оставлять пустым, а подтверждение заказа приходит через `pre_checkout_query` и `successful_payment`.
- Сумма в Stars берётся из поля `price_stars` в тарифе, а если его нет — вычисляется как `price_rub * TELEGRAM_STARS_PRICE_MULTIPLIER`.
- Для Telegram Stars в проекте не нужен внешний webhook: активация происходит по входящему апдейту `successful_payment` от Bot API.


## Админ-диагностика платежей

- В админ-меню добавлен пункт **«💳 Диагностика платежей»**.
- Команда `/paydiag PAYMENT_ID` показывает локальный статус, внешний `provider_payment_id`, историю переходов и последние dedup-события.
- Команда `/payactions` показывает последние admin actions по платежам, `/payops` — pending refund/cancel, а `/payattention` — очередь платежей, требующих внимания (stale processing, зависшие refund/cancel, mismatch между webhook и локальным статусом).
- Для активного провайдера доступны admin actions:
  - **YooKassa**: `refund` для `succeeded`, `cancel` для `waiting_for_capture`;
  - **Telegram Stars**: `refundStarPayment` по `telegram_payment_charge_id`.
- Все действия пишутся в `payment_status_history` как audit trail.


## Payment attention auto-resolver

- Команда: `\/payresolve`
- Background job: `ENABLE_PAYMENT_ATTENTION_RESOLVER_JOB=true`
- Интервал: `PAYMENT_ATTENTION_RESOLVE_INTERVAL_SEC`
- Лимит за проход: `PAYMENT_ATTENTION_RESOLVE_LIMIT`


## Payment attention retry policy

Auto-resolver uses exponential backoff based on recorded admin actions. Configure with `PAYMENT_ATTENTION_RETRY_BASE_MIN`, `PAYMENT_ATTENTION_RETRY_BACKOFF_MULTIPLIER`, and `PAYMENT_ATTENTION_RETRY_MAX_ATTEMPTS`.


## Новые админ-инструменты

В админ-меню доступны bulk-операции:
- **📣 Рассылка всем** — отправка сообщения всем пользователям;
- **📣 Рассылка активным** — отправка только пользователям с активной подпиской;
- **⏱ Продлить всем активным** — массовое добавление дней к активным подпискам.

Для bulk-операций предусмотрено подтверждение перед запуском.


## Полировка платежей и UX

- После выбора тарифа бот сначала показывает экран выбора способа оплаты.
- Если включён Telegram Stars, в списках тарифов дополнительно показывается цена в ⭐.
- В подписках и личном кабинете навигация упрощена: меньше повторов и единая inline-логика.

## Деплой и выкладка

В проект добавлены:

- `.env.polling.example`
- `.env.webhook.example`
- `deploy/run_polling.sh`
- `deploy/run_webhook.sh`
- `deploy/systemd/*.service`
- `docs/DEPLOY.md`

Для первого запуска безопаснее использовать polling-режим. На webhook имеет смысл переходить только после настройки домена и HTTPS.
