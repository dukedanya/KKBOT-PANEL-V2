import logging
from html import escape

from aiogram import Router
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
)

from config import Config
from db import Database
from utils.helpers import get_ref_link
from utils.onboarding import ANDROID_URL, IOS_URL, WINDOWS_URL

logger = logging.getLogger(__name__)
router = Router()


def _matches(query_text: str, *keywords: str) -> bool:
    if not query_text:
        return True
    lowered = query_text.lower()
    return any(keyword in lowered for keyword in keywords)


@router.inline_query()
async def inline_share_menu(query: InlineQuery, db: Database):
    user_id = query.from_user.id
    user = await db.get_user(user_id)
    if not user:
        await db.add_user(user_id)

    bot_username = getattr(query.bot, "username", None) or ""
    bot_link = f"https://t.me/{bot_username}" if bot_username else "https://t.me/"

    ref_code = await db.ensure_ref_code(user_id)
    ref_link = get_ref_link(ref_code, 2, bot_username=bot_username)
    discount_pct = int(float(getattr(Config, "REF_FIRST_PAYMENT_DISCOUNT_PERCENT", 15) or 15))
    bonus_days = int(getattr(Config, "REFERRED_BONUS_DAYS", 5) or 5)
    bonus_text = f"💸 По моей ссылке — скидка {discount_pct}% на первую оплату и +{bonus_days} дней к тарифу."

    query_text = (query.query or "").strip().lower()
    raw_query_text = (query.query or "").strip()
    results = []

    if raw_query_text.startswith("https://t.me/") and "start=gift_" in raw_query_text:
        gift_link = raw_query_text
        results.append(
            InlineQueryResultArticle(
                id="gift_link_ready",
                title="🎀 Подарочная ссылка",
                description="Отправить готовую подарочную ссылку",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        "🎁 <b>Для тебя подарок</b>\n\n"
                        "Лови доступ к Какой-то VPN 🪬\n\n"
                        "Что внутри:\n"
                        "• обход белых списков\n"
                        "• свободный интернет\n"
                        "• скорость до 10 ГБ/сек\n\n"
                        "Сними оковы и пользуйся свободным интернетом."
                    ),
                    parse_mode="HTML",
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="🎁 Открыть подарок", url=gift_link)]]
                ),
            )
        )

    if _matches(query_text, "ref", "реф", "ссылка", "invite", "share", "vpn", ""):
        results.append(
            InlineQueryResultArticle(
                id="ref_link_full",
                title="🔗 Реферальная ссылка",
                description="Отправить полное приглашение со ссылкой и бонусом",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        "🔒 <b>Надёжный VPN-сервис</b>\n\n"
                        "Подключайся по моей реферальной ссылке:\n"
                        f"<blockquote>{ref_link}</blockquote>\n\n"
                        f"{bonus_text}"
                    ),
                    parse_mode="HTML",
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="🚀 Подключиться", url=ref_link)]]
                ),
            )
        )

    if _matches(query_text, "short", "крат", "invite", "реф", "vpn", ""):
        results.append(
            InlineQueryResultArticle(
                id="ref_link_short",
                title="📨 Короткое приглашение",
                description="Короткое сообщение с кнопкой на бота",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        "👋 Подключайся к VPN по моей ссылке.\n"
                        f"{bonus_text}"
                    ),
                    parse_mode="HTML",
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="🚀 Открыть бота", url=ref_link)]]
                ),
            )
        )

    if _matches(query_text, "gift", "подар", ""):
        results.append(
            InlineQueryResultArticle(
                id="gift_prompt",
                title="🎁 Сделать подарок",
                description="Поделиться кнопкой на бота для оформления подарка",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        "🎁 <b>Для тебя подарок</b>\n\n"
                        "Лови доступ к Какой-то VPN 🪬\n\n"
                        "Что внутри:\n"
                        "• обход белых списков\n"
                        "• свободный интернет\n"
                        "• скорость до 10 ГБ/сек\n\n"
                        "Сними оковы и пользуйся свободным интернетом."
                    ),
                    parse_mode="HTML",
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="🎁 Получить подарок", url=bot_link)]]
                ),
            )
        )

    if _matches(query_text, "guide", "инструк", "help", "happ", "подключ", ""):
        results.append(
            InlineQueryResultArticle(
                id="instruction",
                title="📱 Инструкция по подключению",
                description="Поделиться инструкцией и ссылками на Happ",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        "📱 <b>Как подключиться к VPN</b>\n\n"
                        "1. Установите Happ на своё устройство.\n"
                        "2. Получите ссылку подключения в боте.\n"
                        "3. Откройте ссылку в Happ.\n"
                        "4. Включите VPN.\n\n"
                        "Если что-то не работает, напишите в поддержку."
                    ),
                    parse_mode="HTML",
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(text="🍎 iPhone / Mac", url=IOS_URL),
                            InlineKeyboardButton(text="🤖 Android", url=ANDROID_URL),
                        ],
                        [InlineKeyboardButton(text="🪟 Windows", url=WINDOWS_URL)],
                        [InlineKeyboardButton(text="🤖 Открыть бота", url=bot_link)],
                    ]
                ),
            )
        )

    await query.answer(results[:10], cache_time=30, is_personal=True)
