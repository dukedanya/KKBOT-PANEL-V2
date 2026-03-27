from string import Formatter
from typing import Dict, List, Tuple

TEMPLATES: Dict[str, Dict[str, object]] = {
    "main_message": {"title": "Главное сообщение", "text": "👋 <b>Добро пожаловать в Kakoito VPN</b>\n\nНадёжный VPN для комфортной работы и отдыха.\nВыберите нужный раздел ниже 👇", "allow_photo": True},
    "support_menu": {"title": "Меню поддержки", "text": "🆘 <b>Тех. поддержка</b>\n\nОпишите свой вопрос одним сообщением.", "allow_photo": True},
    "instruction_menu": {"title": "Инструкция", "text": "📖 <b>Инструкция по подключению VPN</b>\n\n1. Скачайте приложение Happ.\n2. Скопируйте URL для подключения из личного кабинета.\n3. В приложении нажмите «Импорт» и вставьте ссылку.\n4. Включите VPN и пользуйтесь.\n\nЕсли у вас возникли трудности, обратитесь в поддержку.", "allow_photo": True},
    "channel_menu": {"title": "Наш канал", "text": "📢 <b>Наш канал</b>\n\nПодписывайтесь, чтобы быть в курсе новостей и акций!", "allow_photo": True},
    "reviews_menu": {"title": "Отзывы", "text": "💬 <b>Отзывы о нашем сервисе</b>\n\nЧитайте отзывы и оставляйте свои впечатления!", "allow_photo": True},
    "referral_menu": {"title": "Реферальная система", "text": "🤝 <b>Реферальная система</b>\n\nПриглашайте друзей и получайте процент на баланс за их оплаты.\n\n🎁 Что получает приглашённый пользователь:\n• <b>15% скидка</b> на первую оплату тарифа\n• <b>+5 дней</b> к первому тарифу после оплаты\n\nВаш реферальный баланс можно использовать для вывода средств.", "allow_photo": True},
    "partner_cabinet": {"title": "Партнёрский кабинет", "text": "🤝 <b>Партнёрский кабинет</b>", "allow_photo": True},
    "support_sent_user": {"title": "Поддержка: сообщение отправлено", "text": "✅ Сообщение отправлено в тех. поддержку.", "allow_photo": False},
    "support_sent_admin": {"title": "Поддержка: ответ отправлен", "text": "✅ Ответ отправлен пользователю.", "allow_photo": False},
    "support_reply_title": {"title": "Поддержка: заголовок ответа", "text": "🛠 <b>Ответ от Тех. Поддержки</b>", "allow_photo": False},
    "support_closed_by_admin_user": {"title": "Поддержка: вопрос закрыт", "text": "✅ <b>Ваш вопрос закрыт тех. поддержкой.</b>", "allow_photo": False},
    "payment_success_user": {"title": "Оплата: успешная", "text": "✅ <b>Платёж подтверждён!</b>\n\n📦 Тариф: <b>{plan_name}</b>\n📱 Устройств: <b>до {ip_limit}</b>\n⏳ Срок: <b>{duration}</b>\n{connection_info}\n\nСпасибо за покупку! 🎉", "allow_photo": False},
    "payment_rejected_user": {"title": "Оплата: отклонена", "text": "❌ <b>Платёж не был завершён.</b>\n\nЕсли деньги всё же списались, напишите в поддержку — мы проверим вручную.", "allow_photo": False},
    "referral_bonus_user": {"title": "Реферальный бонус: пользователю", "text": "🎁 Вам начислено <b>+{bonus_days} дней</b> бесплатно по реферальной программе!", "allow_photo": False},
    "withdraw_request_created_user": {"title": "Вывод: запрос создан", "text": "✅ Запрос на вывод <b>{amount:.2f} ₽</b> отправлен администратору. Ожидайте подтверждения.", "allow_photo": False},
    "withdraw_request_exists_user": {"title": "Вывод: уже есть активный", "text": "⏳ У вас уже есть активный запрос на вывод.\n\n🆔 ID запроса: <code>{request_id}</code>\n💰 Сумма: <b>{amount:.2f} ₽</b>\nДождитесь решения администратора.", "allow_photo": False},
    "payment_provider_label_itpay": {"title": "Оплата: название ITPAY", "text": "ITPAY", "allow_photo": False},
    "payment_provider_label_yookassa": {"title": "Оплата: название ЮKassa", "text": "ЮKassa", "allow_photo": False},
    "payment_provider_label_telegram_stars": {"title": "Оплата: название Telegram Stars", "text": "Telegram Stars", "allow_photo": False},
    "payment_provider_label_balance": {"title": "Оплата: название Баланс", "text": "Баланс", "allow_photo": False},
    "payment_provider_button_itpay": {"title": "Оплата: кнопка ITPAY", "text": "💳 ITPAY", "allow_photo": False},
    "payment_provider_button_yookassa": {"title": "Оплата: кнопка ЮKassa", "text": "💳 ЮKassa", "allow_photo": False},
    "payment_provider_button_telegram_stars": {"title": "Оплата: кнопка Telegram Stars", "text": "⭐ Telegram Stars", "allow_photo": False},
    "payment_provider_button_balance": {"title": "Оплата: кнопка Баланс", "text": "💰 С Баланса", "allow_photo": False},
}

def template_title(key: str) -> str:
    return str(TEMPLATES.get(key, {}).get("title", key))

def template_default_text(key: str) -> str:
    return str(TEMPLATES.get(key, {}).get("text", ""))

def template_allow_photo(key: str) -> bool:
    return bool(TEMPLATES.get(key, {}).get("allow_photo", False))

async def get_template_content(db, key: str) -> Tuple[str, str]:
    text = await db.get_setting(f"template:{key}:text", template_default_text(key)) if hasattr(db, "get_setting") else template_default_text(key)
    photo = await db.get_setting(f"template:{key}:photo", "") if hasattr(db, "get_setting") else ""
    return text or template_default_text(key), photo or ""

async def render_template(db, key: str, **fmt) -> Tuple[str, str]:
    text, photo = await get_template_content(db, key)
    try:
        return text.format(**fmt), photo
    except Exception:
        return text, photo


def template_variables(key: str) -> List[str]:
    text = template_default_text(key)
    vars_found: List[str] = []
    seen = set()
    for _, field_name, _, _ in Formatter().parse(text):
        if not field_name:
            continue
        base_name = str(field_name).split("!", 1)[0].split(":", 1)[0].strip()
        if not base_name or base_name in seen:
            continue
        seen.add(base_name)
        vars_found.append(base_name)
    return vars_found


def template_variables_map() -> Dict[str, List[str]]:
    return {key: template_variables(key) for key in TEMPLATES.keys()}


async def show_template_message(message_obj, db, key: str, reply_markup=None, parse_mode: str = "HTML", **fmt):
    from utils.telegram_ui import smart_edit_message
    text, photo = await render_template(db, key, **fmt)
    if photo:
        try:
            sent = await message_obj.answer_photo(photo, caption=text, reply_markup=reply_markup, parse_mode=parse_mode)
            try:
                await message_obj.delete()
            except Exception:
                pass
            return sent
        except Exception:
            pass
    return await smart_edit_message(message_obj, text, reply_markup=reply_markup, parse_mode=parse_mode)
