import logging
from html import escape
from typing import Optional

from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove

from config import Config
from db import Database
from keyboards import instruction_keyboard, support_menu_keyboard, user_dashboard_keyboard
from tariffs import get_by_id
from utils.helpers import replace_message, notify_admins, notify_user
from utils.telegram_ui import smart_edit_message
from utils.templates import render_template, show_template_message
from services.panel import PanelAPI
from kkbot.services.subscriptions import create_subscription
from kkbot.services.subscriptions import get_subscription_status
from services.antifraud import evaluate_referral_link
from utils.onboarding import (
    help_keyboard,
    help_text,
    onboarding_keyboard,
    onboarding_platform_keyboard,
    onboarding_platform_text,
    onboarding_text,
)

logger = logging.getLogger(__name__)
router = Router()


HELP_SCENARIOS = {
    "connect": (
        "🔌 <b>Не подключается</b>\n\n"
        "1. Откройте ссылку подключения ещё раз.\n"
        "2. Проверьте, что профиль импортирован в Happ.\n"
        "3. Перезапустите приложение.\n"
        "4. Если не помогло, отправьте сообщение в поддержку."
    ),
    "slow": (
        "🐢 <b>Медленно работает</b>\n\n"
        "1. Переключите мобильный интернет / Wi‑Fi.\n"
        "2. Перезапустите VPN-клиент.\n"
        "3. Попробуйте снова через 1–2 минуты.\n"
        "4. Если проблема остаётся, напишите в поддержку и укажите страну/оператора."
    ),
    "sites": (
        "🌐 <b>Не открываются сайты</b>\n\n"
        "1. Полностью выключите и снова включите VPN.\n"
        "2. Проверьте, открываются ли сайты без VPN.\n"
        "3. Если не открываются только отдельные сайты, пришлите список в поддержку."
    ),
    "payment": (
        "💳 <b>Оплатил, но доступа нет</b>\n\n"
        "1. Нажмите кнопку проверки оплаты.\n"
        "2. Откройте личный кабинет и проверьте ссылку подключения.\n"
        "3. Если доступ не появился в течение пары минут, напишите в поддержку с временем оплаты."
    ),
}


def _pending_ref_key(user_id: int) -> str:
    return f"ref:pending:{int(user_id)}"


async def _store_pending_referrer(db: Database, user_id: int, referrer_id: int) -> None:
    if hasattr(db, "set_setting"):
        await db.set_setting(_pending_ref_key(user_id), str(int(referrer_id)))


async def _load_pending_referrer(db: Database, user_id: int) -> int:
    if not hasattr(db, "get_setting"):
        return 0
    raw = str(await db.get_setting(_pending_ref_key(user_id), "") or "").strip()
    try:
        return int(raw)
    except ValueError:
        return 0


async def _clear_pending_referrer(db: Database, user_id: int) -> None:
    if hasattr(db, "set_setting"):
        await db.set_setting(_pending_ref_key(user_id), "")


async def _maybe_apply_pending_referrer(*, db: Database, user_id: int, bot: Bot | None = None) -> bool:
    user = await db.get_user(user_id) or {}
    if user.get("ref_by"):
        await _clear_pending_referrer(db, user_id)
        return False
    referrer_id = await _load_pending_referrer(db, user_id)
    if referrer_id <= 0:
        return False
    is_allowed, reason = await evaluate_referral_link(user_id, referrer_id, db=db, bot=bot)
    if is_allowed:
        await db.set_ref_by(user_id, referrer_id)
        await _clear_pending_referrer(db, user_id)
        logger.info("pending referral restored user=%s referrer=%s", user_id, referrer_id)
        return True
    logger.warning("pending referral blocked user=%s referrer=%s reason=%s", user_id, referrer_id, reason)
    return False


def _gift_claim_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Активировать подарок", callback_data=f"gift:claim:{token}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")],
        ]
    )


async def _activate_gift_token(*, token: str, user_id: int, db: Database, panel: PanelAPI, bot: Bot, message_target) -> bool:
    gift = await db.get_gift_link(token) if hasattr(db, "get_gift_link") else None
    if not gift:
        await message_target.answer("❌ Подарочная ссылка не найдена или уже недоступна.", parse_mode="HTML")
        return False
    if gift.get("claimed_by_user_id"):
        claimed_by = int(gift.get("claimed_by_user_id") or 0)
        if claimed_by == user_id:
            await message_target.answer("ℹ️ Этот подарок уже был активирован на ваш аккаунт.", parse_mode="HTML")
        else:
            await message_target.answer("❌ Эта подарочная ссылка уже активирована другим пользователем.", parse_mode="HTML")
        return False
    intended_recipient_id = int(gift.get("recipient_user_id") or 0)
    if intended_recipient_id and intended_recipient_id != user_id:
        await message_target.answer("❌ Эта подарочная ссылка предназначена для другого пользователя.", parse_mode="HTML")
        return False
    plan_id = str(gift.get("plan_id") or "")
    plan = get_by_id(plan_id)
    if not plan:
        await message_target.answer("❌ Не удалось определить тариф для подарка.", parse_mode="HTML")
        return False
    custom_duration_days = int(gift.get("custom_duration_days") or 0)
    gift_plan = dict(plan)
    if custom_duration_days > 0:
        gift_plan["duration_days"] = custom_duration_days
    claimed = await db.claim_gift_link(token, user_id) if hasattr(db, "claim_gift_link") else False
    if not claimed:
        await message_target.answer("❌ Не удалось активировать подарок. Возможно, ссылка уже использована.", parse_mode="HTML")
        return False
    vpn_url = await create_subscription(
        user_id,
        gift_plan,
        db=db,
        panel=panel,
        preserve_active_days=True,
        plan_suffix=" (подарок)",
    )
    if not vpn_url:
        await message_target.answer("❌ Не удалось активировать подарочную подписку. Попробуйте позже или напишите в поддержку.", parse_mode="HTML")
        return False
    buyer_user_id = int(gift.get("buyer_user_id") or 0)
    admin_gift_referrer_id = int(getattr(Config, "ADMIN_GIFT_REFERRER_ID", 794419497))
    if buyer_user_id == admin_gift_referrer_id:
        await db.update_user(user_id, trial_used=1, trial_declined=1)
    gift_note = str(gift.get("note") or "").strip()
    recipient_user = await db.get_user(user_id)
    if buyer_user_id and buyer_user_id != user_id and not (recipient_user or {}).get("ref_by"):
        is_allowed, reason = await evaluate_referral_link(user_id, buyer_user_id, db=db, bot=bot)
        if is_allowed:
            await db.set_ref_by(user_id, buyer_user_id)
        else:
            logger.warning("gift referral blocked user=%s buyer=%s reason=%s", user_id, buyer_user_id, reason)
    duration_days = int(gift_plan.get("duration_days") or plan.get("duration_days") or 0)
    ip_limit = int(gift_plan.get("ip_limit") or plan.get("ip_limit") or 0)
    sender_label = "Администратора" if buyer_user_id == int(getattr(Config, "ADMIN_GIFT_REFERRER_ID", 794419497)) else (f"<code>{buyer_user_id}</code>" if buyer_user_id else "")
    gift_text = (
        "🎁 <b>Подарочная подписка активирована</b>\n\n"
        + (f"Срок: <b>{duration_days}</b> дней\n" if duration_days > 0 else "")
        + (f"Устройств: <b>{ip_limit}</b>\n" if ip_limit > 0 else "")
        + (f"От: <b>{sender_label}</b>\n" if sender_label else "")
        + (f"Комментарий: <i>{escape(gift_note)}</i>\n" if gift_note else "")
        + "\n"
        f"{onboarding_text()}"
    )
    await message_target.answer(gift_text, parse_mode="HTML", reply_markup=onboarding_keyboard())
    if buyer_user_id and buyer_user_id != admin_gift_referrer_id:
        await notify_user(
            buyer_user_id,
            f"🎁 Ваш подарок активирован пользователем <code>{user_id}</code>.\n📦 Тариф: <b>{plan.get('name', plan_id)}</b>",
            bot=bot,
        )
    else:
        await notify_admins(
            f"🎁 <b>Подарок активирован</b>\n"
            f"👤 Получатель: <code>{user_id}</code>\n"
            f"🧾 Покупатель: <code>{buyer_user_id}</code>\n"
            f"📦 {plan.get('name', plan_id)}",
            bot=bot,
        )
    return True


def _help_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔌 Не подключается", callback_data="help:auto:connect")],
            [InlineKeyboardButton(text="🐢 Медленно работает", callback_data="help:auto:slow")],
            [InlineKeyboardButton(text="🌐 Не открываются сайты", callback_data="help:auto:sites")],
            [InlineKeyboardButton(text="💳 Оплатил, но нет доступа", callback_data="help:auto:payment")],
            [InlineKeyboardButton(text="✉️ Написать в поддержку", callback_data="support:start")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_menu:main")],
        ]
    )


async def render_user_dashboard_text(db: Database) -> str:
    text, _ = await render_template(db, "main_message")
    return text


async def show_main_menu(user_id: int, db: Database, bot: Optional[Bot] = None, delete_user_msg: Optional[Message] = None):
    bot = bot or (delete_user_msg.bot if delete_user_msg else None)
    text, photo_id = await render_template(db, "main_message")
    is_admin = user_id in Config.ADMIN_USER_IDS
    if photo_id and bot:
        from utils.helpers import user_last_msg
        msg = await bot.send_photo(user_id, photo=photo_id, caption=text, reply_markup=user_dashboard_keyboard(is_admin), parse_mode="HTML")
        prev = user_last_msg.get(user_id)
        if prev:
            try:
                await bot.delete_message(user_id, prev)
            except Exception as exc:
                logger.debug("show_main_menu: failed to delete previous message for user %s: %s", user_id, exc)
        if delete_user_msg:
            try:
                await delete_user_msg.delete()
            except Exception as exc:
                logger.debug("show_main_menu: failed to delete user trigger message for user %s: %s", user_id, exc)
        user_last_msg[user_id] = msg.message_id
        return
    await replace_message(
        user_id,
        text,
        reply_markup=user_dashboard_keyboard(is_admin),
        delete_user_msg=delete_user_msg,
        bot=bot,
    )


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext, db: Database, panel: PanelAPI):
    await state.clear()
    user_id = message.from_user.id
    await db.add_user(user_id)
    await db.ensure_ref_code(user_id)

    parts = message.text.strip().split(maxsplit=1) if message.text else []
    ref_param = parts[1] if len(parts) > 1 else ""
    existing_user = await db.get_user(user_id)
    if ref_param.startswith("gift_"):
        token = ref_param[5:].strip()
        gift = await db.get_gift_link(token) if hasattr(db, "get_gift_link") else None
        if not gift:
            await message.answer("❌ Подарочная ссылка не найдена или уже недоступна.", parse_mode="HTML")
            await show_main_menu(user_id, db=db, bot=message.bot, delete_user_msg=message)
            return
        if gift.get("claimed_by_user_id"):
            claimed_by = int(gift.get("claimed_by_user_id") or 0)
            if claimed_by == user_id:
                await message.answer("ℹ️ Этот подарок уже был активирован на ваш аккаунт.", parse_mode="HTML")
            else:
                await message.answer("❌ Эта подарочная ссылка уже активирована другим пользователем.", parse_mode="HTML")
            await show_main_menu(user_id, db=db, bot=message.bot, delete_user_msg=message)
            return
        intended_recipient_id = int(gift.get("recipient_user_id") or 0)
        if intended_recipient_id and intended_recipient_id != user_id:
            await message.answer("❌ Эта подарочная ссылка предназначена для другого пользователя.", parse_mode="HTML")
            await show_main_menu(user_id, db=db, bot=message.bot, delete_user_msg=message)
            return
        plan_id = str(gift.get("plan_id") or "")
        plan = get_by_id(plan_id)
        if not plan:
            await message.answer("❌ Не удалось определить тариф для подарка.", parse_mode="HTML")
            await show_main_menu(user_id, db=db, bot=message.bot, delete_user_msg=message)
            return
        gift_note = str(gift.get("note") or "").strip()
        custom_duration_days = int(gift.get("custom_duration_days") or 0)
        preview_text = (
            "🎁 <b>Для тебя подарок</b>\n\n"
            f"<b>{escape(gift_note or 'Тебе отправили доступ к VPN')}</b>\n\n"
            f"Тариф: <b>{plan.get('name', plan_id)}</b>\n"
            + (f"Срок: <b>{custom_duration_days}</b> дней\n" if custom_duration_days > 0 else "")
            + "\nНажми кнопку ниже, чтобы активировать подарок на свой аккаунт."
        )
        await message.answer(preview_text, parse_mode="HTML", reply_markup=_gift_claim_keyboard(token))
        return
    if ref_param and not (existing_user or {}).get("ref_by"):
        ref_user = None
        if ref_param.startswith("ref1_") or ref_param.startswith("ref2_"):
            _prefix, code = ref_param.split("_", 1)
            ref_user = await db.get_user_by_ref_code(code)
        else:
            ref_user = await db.get_user_by_ref_code(ref_param)
        if ref_user:
            referrer_id = int(ref_user.get("user_id"))
            await _store_pending_referrer(db, user_id, referrer_id)
            is_allowed, reason = await evaluate_referral_link(user_id, referrer_id, db=db, bot=message.bot)
            if is_allowed:
                await db.set_ref_by(user_id, referrer_id)
                await _clear_pending_referrer(db, user_id)
            else:
                logger.warning("referral link blocked user=%s referrer=%s reason=%s", user_id, referrer_id, reason)
    elif not ref_param and not (existing_user or {}).get("ref_by"):
        await _maybe_apply_pending_referrer(db=db, user_id=user_id, bot=message.bot)

    cleanup = await message.answer("⌨️ Нижнее меню скрыто.", reply_markup=ReplyKeyboardRemove())
    try:
        await cleanup.delete()
    except Exception as exc:
        logger.debug("cmd_start: failed to delete cleanup message for user %s: %s", user_id, exc)
    await show_main_menu(user_id, db=db, bot=message.bot, delete_user_msg=message)


@router.callback_query(F.data.startswith("gift:claim:"))
async def gift_claim_callback(callback: CallbackQuery, db: Database, panel: PanelAPI):
    user_id = callback.from_user.id
    token = callback.data.split(":", 2)[-1].strip()
    ok = await _activate_gift_token(
        token=token,
        user_id=user_id,
        db=db,
        panel=panel,
        bot=callback.bot,
        message_target=callback.message,
    )
    if ok:
        await callback.answer("Подарок активирован")
    else:
        await callback.answer()


@router.message(Command("menu"))
@router.message(F.text == "🏠 Меню")
async def open_main_menu(message: Message, db: Database):
    cleanup = await message.answer("⌨️ Нижнее меню скрыто.", reply_markup=ReplyKeyboardRemove())
    try:
        await cleanup.delete()
    except Exception as exc:
        logger.debug("open_main_menu: failed to delete cleanup message for user %s: %s", message.from_user.id, exc)
    await show_main_menu(message.from_user.id, db=db, bot=message.bot, delete_user_msg=message)


@router.callback_query(F.data == "user_menu:main")
@router.callback_query(F.data == "main_menu")
async def main_menu_callback(callback: CallbackQuery, db: Database):
    text = await render_user_dashboard_text(db)
    is_admin = callback.from_user.id in Config.ADMIN_USER_IDS
    await smart_edit_message(callback.message, text, reply_markup=user_dashboard_keyboard(is_admin), parse_mode="HTML")
    await callback.answer()


@router.message(F.text == "👤 Личный кабинет")
async def profile_menu(message: Message, db: Database, panel: PanelAPI):
    from handlers.profile import show_profile_menu

    user_id = message.from_user.id
    await db.add_user(user_id)
    await show_profile_menu(user_id, db=db, panel=panel, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "user_menu:profile")
async def profile_menu_callback(callback: CallbackQuery, db: Database, panel: PanelAPI):
    from handlers.profile import render_profile_text
    from keyboards import profile_inline_keyboard

    status = await get_subscription_status(callback.from_user.id, db=db, panel=panel)
    text = await render_profile_text(callback.from_user.id, status=status, panel=panel, db=db)
    await smart_edit_message(callback.message, 
        text,
        reply_markup=profile_inline_keyboard(status["active"], is_frozen=status["is_frozen"], is_admin=callback.from_user.id in Config.ADMIN_USER_IDS),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(F.text == "🆘 Поддержка")
async def support_menu(message: Message, db: Database):
    text, _ = await render_template(db, "support_menu")
    await replace_message(message.from_user.id, text, reply_markup=support_menu_keyboard(), delete_user_msg=message, bot=message.bot)


@router.callback_query(F.data == "user_menu:support")
async def support_menu_callback(callback: CallbackQuery, db: Database):
    await show_template_message(callback.message, db, "support_menu", reply_markup=support_menu_keyboard())
    await callback.answer()


@router.message(F.text == "Помощь")
async def instruction_menu(message: Message, db: Database):
    await replace_message(
        message.from_user.id,
        help_text(),
        reply_markup=help_keyboard(),
        delete_user_msg=message,
        bot=message.bot,
    )


@router.callback_query(F.data == "user_menu:instruction")
async def instruction_menu_callback(callback: CallbackQuery, db: Database):
    await smart_edit_message(callback.message, help_text(), reply_markup=help_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data == "onboarding:start")
async def onboarding_start(callback: CallbackQuery):
    text = f"{onboarding_text()}\n\nВыберите устройство ниже:"
    await smart_edit_message(callback.message, text, reply_markup=onboarding_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data.startswith("onboarding:platform:"))
async def onboarding_platform(callback: CallbackQuery, db: Database, panel: PanelAPI):
    platform = callback.data.rsplit(":", 1)[-1]
    status = await get_subscription_status(callback.from_user.id, db=db, panel=panel)
    user_data = status.get("user") or {}
    legacy_user = await db.get_user(callback.from_user.id) or {}
    subscription_url = str(user_data.get("vpn_url") or legacy_user.get("vpn_url") or "").strip()
    await smart_edit_message(
        callback.message,
        onboarding_platform_text(platform=platform, subscription_url=subscription_url),
        reply_markup=onboarding_platform_keyboard(platform=platform, subscription_url=subscription_url),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "help:auto:start")
async def auto_help_start(callback: CallbackQuery):
    await smart_edit_message(
        callback.message,
        "🛠 <b>Автопомощь</b>\n\nВыберите, с чем у вас возникла проблема.",
        reply_markup=_help_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("help:auto:"))
async def auto_help_scenario(callback: CallbackQuery):
    key = callback.data.split(":")[-1]
    text = HELP_SCENARIOS.get(key)
    if not text:
        await callback.answer("Сценарий не найден", show_alert=True)
        return
    await smart_edit_message(callback.message, text, reply_markup=_help_keyboard(), parse_mode="HTML")
    await callback.answer()


@router.message(F.text == "📢 Наш канал")
async def channel_link(message: Message, db: Database):
    text = "📢 <b>Наш канал</b>\n\nПодписывайтесь, чтобы быть в курсе новостей и акций!"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Перейти в канал", url=Config.TG_CHANNEL)],
            [InlineKeyboardButton(text="Главное меню", callback_data="user_menu:main")]
        ]
    )
    await replace_message(message.from_user.id, text, reply_markup=keyboard, delete_user_msg=message, bot=message.bot)


@router.message(F.text == "💬 Отзывы")
async def reviews_link(message: Message, db: Database):
    text = "💬 <b>Отзывы о нашем сервисе</b>\n\nЧитайте отзывы и оставляйте свои впечатления!"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Перейти к отзывам", url=Config.TG_CHANNEL)],
            [InlineKeyboardButton(text="Главное меню", callback_data="user_menu:main")]
        ]
    )
    await replace_message(message.from_user.id, text, reply_markup=keyboard, delete_user_msg=message, bot=message.bot)


@router.callback_query(F.data == "user_menu:reviews")
async def reviews_link_callback(callback: CallbackQuery, db: Database):
    text = "💬 <b>Отзывы о нашем сервисе</b>\n\nЧитайте отзывы и оставляйте свои впечатления!"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Перейти к отзывам", url=Config.TG_CHANNEL)],
            [InlineKeyboardButton(text="Главное меню", callback_data="user_menu:main")]
        ]
    )
    await show_template_message(callback.message, db, "reviews_menu", reply_markup=keyboard)
    await callback.answer()
