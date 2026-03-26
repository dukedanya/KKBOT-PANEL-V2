import logging

from aiogram import Router
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
)

from db import Database
from utils.helpers import get_ref_link

logger = logging.getLogger(__name__)
router = Router()


@router.inline_query()
async def inline_ref_link(query: InlineQuery, db: Database):
    user_id = query.from_user.id

    user = await db.get_user(user_id)
    if not user:
        await db.add_user(user_id)
        user = await db.get_user(user_id)

    bot_username = getattr(query.bot, "username", None) or ""

    ref_code = await db.ensure_ref_code(user_id)
    system_type = 2
    link = get_ref_link(ref_code, system_type, bot_username=bot_username)
    bonus_text = "💸 По моей ссылке — скидка 15% на первую оплату и +5 дней к тарифу."

    share_text = (
        "🔒 <b>Надёжный VPN-сервис</b>\n\n"
        "Подключайся по моей реферальной ссылке:\n"
        f"{link}\n\n"
        f"{bonus_text}"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🚀 Подключиться", url=link)]]
    )

    results = [
        InlineQueryResultArticle(
            id="ref_link",
            title="🔗 Отправить реферальную ссылку",
            description=link,
            input_message_content=InputTextMessageContent(
                message_text=share_text,
                parse_mode="HTML",
            ),
            reply_markup=keyboard,
            thumbnail_url="https://cdn-icons-png.flaticon.com/512/2716/2716051.png",
        ),
        InlineQueryResultArticle(
            id="ref_link_short",
            title="📨 Краткое приглашение",
            description="Короткое сообщение с кнопкой",
            input_message_content=InputTextMessageContent(
                message_text=f"👋 Присоединяйся к нашему VPN!\n{bonus_text}",
                parse_mode="HTML",
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🚀 Присоединиться", url=link)]]
            ),
        ),
    ]

    await query.answer(results, cache_time=30, is_personal=True)
