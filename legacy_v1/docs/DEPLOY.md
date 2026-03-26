# Deploy

## Быстрый старт через polling
1. Скопируйте `.env.polling.example` в `.env`
2. Заполните токены и доступы
3. Создайте каталоги `DATA_DIR` и `BACKUP_DIR`
4. Установите зависимости и выполните `python main.py`

## Быстрый старт через webhook
1. Скопируйте `.env.webhook.example` в `.env`
2. Настройте `WEBHOOK_HOST` на HTTPS-домен
3. Проксируйте запросы на `WEBHOOK_BIND_HOST:WEBHOOK_PORT`
4. Запустите `python main.py`

## Перед выкладкой
- Проверьте `PAYMENT_PROVIDERS`
- Проверьте `PUBLIC_BASE_URL`
- Убедитесь, что health/readiness отвечают
- Сделайте backup базы
