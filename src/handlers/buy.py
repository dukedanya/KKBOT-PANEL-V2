import asyncio
import logging
import secrets
import time
from typing import Optional

from aiogram import Router, F, Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery, Message, PreCheckoutQuery,
    InlineKeyboardMarkup, InlineKeyboardButton, LabeledPrice,
)

from config import Config
from tariffs import (
    get_by_id, is_trial_plan, format_duration,
    build_buy_text,
)
from keyboards import back_keyboard
from utils.helpers import (
    replace_message, get_visible_plans,
)
from utils.payments import get_provider_payment_id
from utils.templates import get_template_content
from utils.subscription_links import render_connection_info
from utils.telegram_ui import smart_edit_message, smart_edit_by_ids
from kkbot.services.subscriptions import create_subscription
from kkbot.services.payment_flow import process_successful_payment, reject_pending_payment
from services.antifraud import guard_payment_creation
from services.telegram_stars import TelegramStarsAPI
from services.payment_gateway import (
    build_payment_gateway,
    get_enabled_payment_providers,
    get_provider_label,
    get_provider_button_label,
)

logger = logging.getLogger(__name__)
router = Router()
_user_payment_locks: dict[int, asyncio.Lock] = {}


class BuyFlowFSM(StatesGroup):
    waiting_gift_recipient = State()
    waiting_promo_code = State()
    waiting_gift_note = State()


def _get_user_payment_lock(user_id: int) -> asyncio.Lock:
    lock = _user_payment_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _user_payment_locks[user_id] = lock
    return lock


def _build_payment_id(prefix: str, user_id: int) -> str:
    return f"{prefix}_{user_id}_{time.time_ns()}_{secrets.token_hex(2)}"


def _build_gift_token() -> str:
    return secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:12]


def _plan_badge_for_button(plan: dict) -> str:
    duration = int(plan.get("duration_days", 30) or 30)
    ip_limit = int(plan.get("ip_limit", 0) or 0)
    if ip_limit >= 10:
        return "👨‍👩‍👧‍👦"
    if duration >= 180:
        return "💎"
    if duration >= 90:
        return "🔥"
    return "⚡"


def _plan_discount_badge(plan: dict) -> str:
    price = float(plan.get("price_rub") or 0.0)
    old_price = float(plan.get("old_price_rub") or 0.0)
    if price <= 0 or old_price <= price + 1e-9:
        return ""
    percent = int(round((old_price - price) * 100 / old_price))
    if percent <= 0:
        return ""
    return f" (-{percent}%)"


def _plan_button_text(plan: dict) -> str:
    name = str(plan.get("name") or plan.get("id") or "Тариф")
    price_value = float(plan.get("price_rub") or 0.0)
    price = str(int(price_value) if price_value.is_integer() else price_value)
    badge = _plan_badge_for_button(plan)
    discount = _plan_discount_badge(plan)
    return f"{badge} {name} · {price} ₽{discount}"


def _format_promo_discount_text(*, code: str, discount_type: str, discount_percent: float, fixed_amount: float) -> str:
    if not code:
        return ""
    if discount_type == "fixed" and fixed_amount > 0:
        return f"{code} (-{fixed_amount:.0f} ₽)"
    return f"{code} (-{discount_percent:.0f}%)"


def _promo_offer_line(offer: dict, *, prefix: str = "🏷 Активный промокод") -> str:
    code = str(offer.get("promo_code") or "")
    if not code:
        return ""
    discount_type = str(offer.get("promo_discount_type") or "percent")
    discount_percent = float(offer.get("promo_discount_percent") or 0.0)
    fixed_amount = float(offer.get("promo_fixed_amount") or 0.0)
    return f"{prefix}: <b>{_format_promo_discount_text(code=code, discount_type=discount_type, discount_percent=discount_percent, fixed_amount=fixed_amount)}</b>"


def _payment_provider_hidden_key(provider_name: str) -> str:
    return f"payment:hidden:{(provider_name or '').strip().lower()}"


async def _is_payment_provider_hidden(db, provider_name: str) -> bool:
    if not hasattr(db, "get_setting"):
        return False
    raw = str(await db.get_setting(_payment_provider_hidden_key(provider_name), "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


async def _set_payment_provider_hidden(db, provider_name: str, hidden: bool) -> None:
    if hasattr(db, "set_setting"):
        await db.set_setting(_payment_provider_hidden_key(provider_name), "1" if hidden else "0")


async def _filter_available_payment_providers(db, providers: list[str]) -> list[str]:
    filtered: list[str] = []
    for provider in providers:
        if provider == "balance":
            filtered.append(provider)
            continue
        if await _is_payment_provider_hidden(db, provider):
            continue
        filtered.append(provider)
    return filtered


def _should_hide_provider_after_error(provider_name: str, gateway_error: str) -> bool:
    provider_name = (provider_name or "").strip().lower()
    error_text = (gateway_error or "").strip().lower()
    if provider_name == "itpay":
        return "терминал заблокирован" in error_text or "прием платежей невозможен" in error_text
    return False


async def _get_pending_gift_note(db, user_id: int) -> str:
    if not hasattr(db, "get_setting"):
        return ""
    return str(await db.get_setting(f"gift:note:{int(user_id)}", "") or "").strip()


async def _set_pending_gift_note(db, user_id: int, note: str) -> None:
    if hasattr(db, "set_setting"):
        await db.set_setting(f"gift:note:{int(user_id)}", note[:180].strip())


async def _clear_pending_gift_note(db, user_id: int) -> None:
    await _set_pending_gift_note(db, user_id, "")


@router.callback_query(F.data.in_(["dismiss_notice", "dismiss_payment_notice"]))
async def dismiss_payment_notice(callback: CallbackQuery):
    try:
        await callback.message.delete()
        await callback.answer("Уведомление убрано")
    except TelegramBadRequest:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        await callback.answer("Не удалось удалить, убрал кнопки", show_alert=True)


@router.message(Command("promoadd"))
async def admin_promo_add(message: Message, db):
    if message.from_user.id not in Config.ADMIN_USER_IDS:
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Формат: /promoadd CODE DISCOUNT_PERCENT [MAX_USES]")
        return
    code = parts[1].strip().upper()
    try:
        discount = float(parts[2].replace(",", "."))
        max_uses = int(parts[3]) if len(parts) > 3 else 0
    except ValueError:
        await message.answer("❌ Неверный формат скидки или лимита.")
        return
    if discount <= 0 or discount >= 100:
        await message.answer("❌ Скидка должна быть больше 0 и меньше 100.")
        return
    await db.create_or_update_promo_code(
        code,
        title=f"Promo {code}",
        description=f"Создано админом {message.from_user.id}",
        discount_percent=discount,
        max_uses=max_uses,
        active=True,
    )
    await message.answer(f"✅ Промокод <b>{code}</b> сохранён. Скидка: <b>{discount:.0f}%</b>.", parse_mode="HTML")


@router.message(Command("promolist"))
async def admin_promo_list(message: Message, db):
    if message.from_user.id not in Config.ADMIN_USER_IDS:
        return
    rows = await db.list_promo_codes(limit=20) if hasattr(db, "list_promo_codes") else []
    if not rows:
        await message.answer("Промокодов пока нет.")
        return
    lines = ["🏷 <b>Промокоды</b>", ""]
    for row in rows:
        lines.append(
            f"• <code>{row['code']}</code> — <b>{float(row.get('discount_percent') or 0):.0f}%</b> "
            f"/ uses {int(row.get('used_count') or 0)}"
            + (f"/{int(row.get('max_uses'))}" if int(row.get('max_uses') or 0) > 0 else "")
            + (" / active" if int(row.get("active") or 0) == 1 else " / off")
        )
    await message.answer("\n".join(lines), parse_mode="HTML")


async def _provider_label(db, provider: str) -> str:
    provider_key = (provider or "").strip().lower()
    if provider_key == "balance":
        default = "Баланс"
    else:
        default = get_provider_label(provider_key)
    text, _ = await get_template_content(db, f"payment_provider_label_{provider_key}")
    text = (text or "").strip()
    return text or default


async def _provider_button_label(db, provider: str, *, balance: Optional[float] = None) -> str:
    provider_key = (provider or "").strip().lower()
    if provider_key == "balance":
        default = "💰 С Баланса"
    else:
        default = get_provider_button_label(provider_key)
    text, _ = await get_template_content(db, f"payment_provider_button_{provider_key}")
    base = (text or "").strip() or default
    if provider_key == "balance" and balance is not None:
        return f"{base} ({balance:.2f} ₽)"
    return base


async def get_referral_first_payment_offer(user_id: int, db, plan: dict) -> dict:
    user = await db.get_user(user_id)
    ref_origin = str((user or {}).get("ref_origin") or "").strip().lower()
    eligible = bool(
        user
        and user.get("ref_by")
        and not user.get("ref_rewarded")
        and ref_origin not in {"gift", "gift_purchase", "admin_gift", "manual"}
    )
    discount_pct = float(getattr(Config, "REF_FIRST_PAYMENT_DISCOUNT_PERCENT", 15) or 0)
    bonus_days = int(getattr(Config, "REFERRED_BONUS_DAYS", 5) or 0)
    base_amount = float(plan.get("price_rub", 0) or 0)
    discounted_amount = base_amount
    if eligible and discount_pct > 0 and base_amount > 0:
        discounted_amount = max(1.0, round(base_amount * (100.0 - discount_pct) / 100.0, 2))
    return {
        "eligible": eligible,
        "discount_pct": discount_pct,
        "bonus_days": bonus_days,
        "base_amount": base_amount,
        "amount": discounted_amount if eligible else base_amount,
    }


async def get_purchase_offer(user_id: int, db, plan: dict, *, target_user_id: Optional[int] = None) -> dict:
    target_id = int(target_user_id or user_id)
    referral_offer = await get_referral_first_payment_offer(target_id, db, plan)
    promo_code = await db.get_active_user_promo_code(user_id) if hasattr(db, "get_active_user_promo_code") else ""
    promo = (
        await db.validate_promo_code(promo_code, user_id=target_id, plan_id=str(plan.get("id") or ""))
        if promo_code and hasattr(db, "validate_promo_code")
        else None
    )
    if promo_code and not promo and hasattr(db, "clear_active_user_promo_code"):
        await db.clear_active_user_promo_code(user_id)
        promo_code = ""
    discount_percent = float((promo or {}).get("discount_percent") or 0.0)
    discount_type = str((promo or {}).get("discount_type") or "percent").strip().lower()
    fixed_amount = float((promo or {}).get("fixed_amount") or 0.0)
    amount = float(referral_offer["amount"])
    if discount_type == "fixed" and fixed_amount > 0 and amount > 0:
        amount = max(1.0, round(amount - fixed_amount, 2))
    elif discount_percent > 0 and amount > 0:
        amount = max(1.0, round(amount * (100.0 - discount_percent) / 100.0, 2))
    return {
        **referral_offer,
        "target_user_id": target_id,
        "promo_code": promo_code,
        "promo_discount_percent": discount_percent,
        "promo_discount_type": discount_type,
        "promo_fixed_amount": fixed_amount,
        "promo_title": str((promo or {}).get("title") or ""),
        "amount": amount,
    }


async def _get_safe_mode_reason(db) -> str:
    if not hasattr(db, "get_setting"):
        return ""
    return str(await db.get_setting("system:safe_mode_reason", "") or "").strip()


async def _ensure_purchases_available(*, db, callback: Optional[CallbackQuery] = None, message: Optional[Message] = None) -> bool:
    if not hasattr(db, "get_setting"):
        return True
    safe_mode = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"
    if not safe_mode:
        return True
    reason = await _get_safe_mode_reason(db)
    text = "⚠️ Покупка временно недоступна: система в safe mode. Мы уже уведомили администраторов."
    if reason:
        text += f"\n\nПричина: {reason[:300]}"
    if callback is not None:
        await callback.answer(text, show_alert=True)
    elif message is not None:
        await message.answer(text, parse_mode="HTML")
    return False


def format_amount_line(amount: float, duration: int) -> str:
    amount_display = int(amount) if float(amount).is_integer() else amount
    return f"{amount_display} ₽ / мес." if duration == 30 else f"{amount_display} ₽ / {duration} дн."


def _format_offer_price_block(offer: dict, duration: int) -> str:
    amount = float(offer.get("amount") or 0.0)
    base_amount = float(offer.get("base_amount") or amount)
    discounted = amount > 0 and base_amount > amount + 1e-9
    current_line = format_amount_line(amount, duration)
    if not discounted:
        return f"<b>{current_line}</b>"
    old_line = format_amount_line(base_amount, duration)
    return f"<s>{old_line}</s>\n<b>{current_line}</b>"


def _build_referral_offer_line(offer: dict) -> str:
    if not bool(offer.get("eligible")):
        return ""
    discount_pct = int(float(offer.get("discount_pct") or 0))
    bonus_days = int(offer.get("bonus_days") or 0)
    parts = []
    if discount_pct > 0:
        parts.append(f"-{discount_pct}%")
    if bonus_days > 0:
        parts.append(f"+{bonus_days} дней")
    if not parts:
        return ""
    return f"🤝 Реферальная скидка: <b>{' и '.join(parts)}</b>"


def _format_offer_duration_block(offer: dict, duration: int) -> str:
    bonus_days = int(offer.get("bonus_days") or 0) if bool(offer.get("eligible")) else 0
    if bonus_days <= 0:
        return f"<b>{format_duration(duration)}</b>"
    base_label = format_duration(duration)
    final_label = format_duration(duration + bonus_days)
    return f"<s>{base_label}</s>\n<b>{final_label}</b>"


def _insufficient_balance_text(*, balance: float, amount: float) -> str:
    return f"❌ Недостаточно средств на балансе. Доступно: {balance:.2f} ₽, нужно: {amount:.2f} ₽."


def _payment_methods_back_markup(plan_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ К способам оплаты", callback_data=f"buy:{plan_id}")]]
    )


def _gift_plans_back_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ К тарифам", callback_data="open_buy_menu")]]
    )


async def show_plans_list(
    user_id: int,
    db,
    bot,
    message_id: Optional[int] = None,
    user_msg: Optional[Message] = None,
    callback_message: Optional[Message] = None,
):
    plans = await get_visible_plans(user_id, for_admin=False, db=db)
    active_promo = await db.get_active_user_promo_code(user_id) if hasattr(db, "get_active_user_promo_code") else ""
    if not plans:
        text = "❌ Нет доступных тарифов."
        if callback_message is not None:
            await smart_edit_message(callback_message, text, reply_markup=back_keyboard())
        elif message_id:
            try:
                await smart_edit_by_ids(bot, chat_id=user_id, message_id=message_id, text=text)
            except Exception:
                await bot.send_message(user_id, text, reply_markup=back_keyboard())
        else:
            await replace_message(user_id, text, reply_markup=back_keyboard(), delete_user_msg=user_msg, bot=bot)
        return

    text = build_buy_text(plans)
    if active_promo:
        promo = await db.validate_promo_code(active_promo, user_id=user_id) if hasattr(db, "validate_promo_code") else None
        if promo:
            text += "\n\n" + _promo_offer_line({
                "promo_code": active_promo,
                "promo_discount_type": promo.get("discount_type"),
                "promo_discount_percent": promo.get("discount_percent"),
                "promo_fixed_amount": promo.get("fixed_amount"),
            })
        else:
            text += f"\n\n🏷 Активный промокод: <b>{active_promo}</b>"
    keyboard = []
    for plan in plans:
        keyboard.append([InlineKeyboardButton(text=_plan_button_text(plan), callback_data=f"buy:{plan.get('id')}")])
    keyboard.append([
        InlineKeyboardButton(text="🎁 Подарить подписку", callback_data="buy:gift_prompt"),
        InlineKeyboardButton(text="🏷 Промокод", callback_data="buy:promo_prompt"),
    ])
    if active_promo:
        keyboard.append([InlineKeyboardButton(text="🧹 Убрать промокод", callback_data="buy:promo_clear")])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_subscriptions")])

    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    if callback_message is not None:
        await smart_edit_message(callback_message, text, reply_markup=markup, parse_mode="HTML")
    elif message_id:
        try:
            await smart_edit_by_ids(bot, chat_id=user_id, message_id=message_id, text=text, reply_markup=markup, parse_mode="HTML")
        except Exception:
            await bot.send_message(user_id, text, reply_markup=markup, parse_mode="HTML")
    else:
        await replace_message(user_id, text, reply_markup=markup, delete_user_msg=user_msg, bot=bot)


async def _show_gift_plans_list(user_id: int, recipient_user_id: int, *, db, bot, callback_message: Message) -> None:
    plans = await get_visible_plans(user_id, for_admin=False, db=db)
    if not plans:
        await smart_edit_message(callback_message, "❌ Нет доступных тарифов.", reply_markup=back_keyboard())
        return
    lines = [
        "🎁 <b>Подарочная подписка</b>",
        "",
        f"Получатель: <code>{recipient_user_id}</code>",
        "",
        "Выберите тариф, который хотите подарить:",
    ]
    keyboard = []
    for plan in plans:
        keyboard.append([InlineKeyboardButton(text=plan.get("name", plan.get("id")), callback_data=f"buygift:{recipient_user_id}:{plan.get('id')}")])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="open_buy_menu")])
    await smart_edit_message(
        callback_message,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "buy:gift_prompt")
async def gift_purchase_prompt(callback: CallbackQuery, state: FSMContext, db):
    if not await _ensure_purchases_available(db=db, callback=callback):
        return
    await smart_edit_message(
        callback.message,
        (
            "🎁 <b>Подарочная подписка</b>\n\n"
            "Выберите, как хотите оформить подарок:\n"
            "• по ссылке, которую можно переслать человеку\n"
            "• напрямую по Telegram ID"
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Подарок по ссылке", callback_data="buy:gift_link_plans")],
            [InlineKeyboardButton(text="🆔 По Telegram ID", callback_data="buy:gift_manual_prompt")],
            [InlineKeyboardButton(text="⬅️ К тарифам", callback_data="open_buy_menu")],
        ]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "buy:gift_manual_prompt")
async def gift_purchase_manual_prompt(callback: CallbackQuery, state: FSMContext, db):
    if not await _ensure_purchases_available(db=db, callback=callback):
        return
    await state.set_state(BuyFlowFSM.waiting_gift_recipient)
    await smart_edit_message(
        callback.message,
        "🎁 <b>Подарочная подписка</b>\n\nОтправьте Telegram ID получателя следующим сообщением.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="buy:gift_prompt")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "buy:gift_link_plans")
async def gift_link_plans(callback: CallbackQuery, db):
    if not await _ensure_purchases_available(db=db, callback=callback):
        return
    await _render_gift_link_plans(callback.message, user_id=callback.from_user.id, db=db)
    await callback.answer()


async def _render_gift_link_plans(message_obj: Message, *, user_id: int, db) -> None:
    plans = await get_visible_plans(user_id, for_admin=False, db=db)
    if not plans:
        await smart_edit_message(message_obj, "❌ Нет доступных тарифов.", reply_markup=back_keyboard())
        return
    gift_note = await _get_pending_gift_note(db, user_id)
    keyboard = [[InlineKeyboardButton(text=plan.get("name", plan.get("id")), callback_data=f"buygiftlink:{plan.get('id')}")] for plan in plans]
    keyboard.append([InlineKeyboardButton(text="✍️ Добавить подпись", callback_data="buy:gift_note_prompt")])
    if gift_note:
        keyboard.append([InlineKeyboardButton(text="🧹 Убрать подпись", callback_data="buy:gift_note_clear")])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="buy:gift_prompt")])
    await smart_edit_message(
        message_obj,
        (
            "🔗 <b>Подарок по ссылке</b>\n\n"
            "Выберите тариф. После оплаты бот сразу пришлёт ссылку, которую можно отправить получателю."
            + (f"\n\n✍️ Подпись к подарку: <i>{gift_note}</i>" if gift_note else "")
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "buy:gift_note_prompt")
async def gift_note_prompt(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyFlowFSM.waiting_gift_note)
    await smart_edit_message(
        callback.message,
        "✍️ <b>Подпись к подарку</b>\n\nОтправьте короткий текст, который увидит получатель при активации подарка.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="buy:gift_link_plans")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "buy:gift_note_clear")
async def gift_note_clear(callback: CallbackQuery, db, state: FSMContext):
    await _clear_pending_gift_note(db, callback.from_user.id)
    await state.clear()
    await _render_gift_link_plans(callback.message, user_id=callback.from_user.id, db=db)
    await callback.answer("Подпись убрана")


@router.message(BuyFlowFSM.waiting_gift_note)
async def gift_note_input(message: Message, state: FSMContext, db):
    note = (message.text or "").strip()
    if not note:
        await message.answer("❌ Отправьте текст подписи.")
        return
    await _set_pending_gift_note(db, message.from_user.id, note)
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    sent = await message.answer("✅ Подпись сохранена.", parse_mode="HTML")
    await _render_gift_link_plans(sent, user_id=message.from_user.id, db=db)


@router.message(BuyFlowFSM.waiting_gift_recipient)
async def gift_purchase_recipient_input(message: Message, state: FSMContext, db):
    raw = (message.text or "").strip()
    try:
        recipient_user_id = int(raw)
        if recipient_user_id <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Отправьте корректный числовой Telegram ID.")
        return
    await db.add_user(recipient_user_id)
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    sent = await message.answer("🎁 Открываю выбор подарочного тарифа...", parse_mode="HTML")
    await _show_gift_plans_list(message.from_user.id, recipient_user_id, db=db, bot=message.bot, callback_message=sent)


@router.callback_query(F.data == "buy:promo_prompt")
async def promo_prompt(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyFlowFSM.waiting_promo_code)
    await smart_edit_message(
        callback.message,
        "🏷 <b>Введите промокод</b>\n\nОтправьте код следующим сообщением. Скидка применится к следующей покупке.",
        reply_markup=_gift_plans_back_markup(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(BuyFlowFSM.waiting_promo_code)
async def promo_input(message: Message, state: FSMContext, db):
    code = (message.text or "").strip().upper()
    promo = await db.validate_promo_code(code, user_id=message.from_user.id) if hasattr(db, "validate_promo_code") else None
    if not promo:
        await message.answer("❌ Промокод не найден или уже недоступен.")
        return
    await db.set_active_user_promo_code(message.from_user.id, code)
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    if str(promo.get("discount_type") or "percent") == "fixed":
        discount_line = f"Скидка: <b>{float(promo.get('fixed_amount') or 0):.0f} ₽</b>"
    else:
        discount_line = f"Скидка: <b>{float(promo.get('discount_percent') or 0):.0f}%</b>"
    sent = await message.answer(
        f"✅ Промокод <b>{code}</b> активирован. {discount_line}.",
        parse_mode="HTML",
    )
    await show_plans_list(message.from_user.id, db=db, bot=message.bot, message_id=sent.message_id, callback_message=sent)


@router.callback_query(F.data == "buy:promo_clear")
async def promo_clear(callback: CallbackQuery, db):
    if hasattr(db, "clear_active_user_promo_code"):
        await db.clear_active_user_promo_code(callback.from_user.id)
    await show_plans_list(callback.from_user.id, db=db, bot=callback.bot, message_id=callback.message.message_id, callback_message=callback.message)
    await callback.answer("Промокод убран")


@router.message(F.text.in_(["💰 Оформить подписку", "💰 Продлить подписку"]))
async def buy_subscription_menu(message: Message, db):
    if not await _ensure_purchases_available(db=db, message=message):
        return
    await show_plans_list(message.from_user.id, db=db, bot=message.bot, user_msg=message)


@router.callback_query(F.data == "open_buy_menu")
async def open_buy_menu_callback(callback: CallbackQuery, db):
    if not await _ensure_purchases_available(db=db, callback=callback):
        return
    await show_plans_list(callback.from_user.id, db=db, bot=callback.bot, message_id=callback.message.message_id, callback_message=callback.message)
    await callback.answer()


@router.callback_query(F.data.startswith("buy:"))
async def buy_plan(callback: CallbackQuery, db):
    if not await _ensure_purchases_available(db=db, callback=callback):
        return
    user_id = callback.from_user.id
    plan_id = callback.data.split(":", 1)[1]
    plan = get_by_id(plan_id)

    if not plan or not plan.get("active", True):
        await callback.answer("❌ Тариф не найден или недоступен", show_alert=True)
        return
    if is_trial_plan(plan):
        await callback.answer("⚠️ Пробный тариф оформляется отдельно.", show_alert=True)
        return

    providers = await _filter_available_payment_providers(db, get_enabled_payment_providers())
    balance = float(await db.get_balance(user_id))
    if balance > 0:
        providers = providers + ["balance"]
    if not providers:
        await callback.answer("❌ Нет включённых способов оплаты", show_alert=True)
        return

    duration = int(plan.get("duration_days", 30))
    offer = await get_purchase_offer(user_id, db, plan)
    price_block = _format_offer_price_block(offer, duration)
    duration_block = _format_offer_duration_block(offer, duration)
    referral_line = _build_referral_offer_line(offer)
    banking = [provider for provider in providers if provider in {"itpay", "yookassa"}]
    telegram = [provider for provider in providers if provider == "telegram_stars"]
    provider_lines = []
    if banking:
        provider_lines.append("<b>Банковские способы</b>")
        for provider in banking:
            provider_lines.append(f"• {await _provider_label(db, provider)}")
    if telegram:
        if provider_lines:
            provider_lines.append("")
        provider_lines.append("<b>Telegram</b>")
        for provider in telegram:
            provider_lines.append(f"• {await _provider_label(db, provider)}")
    if "balance" in providers:
        if provider_lines:
            provider_lines.append("")
        provider_lines.append("<b>Баланс аккаунта</b>")
        provider_lines.append(f"• Доступно: <b>{balance:.2f} ₽</b>")
    provider_lines = "\n".join(provider_lines)
    text = (
        "💳 <b>Почти готово</b>\n\n"
        f"📦 Тариф: <b>{plan.get('name', plan_id)}</b>\n"
        "Что вы получите:\n"
        "• безлимитный трафик\n"
        f"• до <b>{int(plan.get('ip_limit', 0) or 0)}</b> устройств\n"
        f"• срок:\n{duration_block}\n\n"
        f"Стоимость:\n{price_block}\n\n"
        "Выберите удобный способ оплаты:\n"
        f"{provider_lines}\n\n"
        "После оплаты бот автоматически активирует подписку и сразу переведёт вас к подключению."
    )
    if referral_line:
        text += f"\n\n{referral_line}"
    promo_line = _promo_offer_line(offer)
    if promo_line:
        text += f"\n\n{promo_line}"

    keyboard = []
    for provider in providers:
        button_text = await _provider_button_label(db, provider, balance=balance)
        keyboard.append([InlineKeyboardButton(text=button_text, callback_data=f"buy_provider:{plan_id}:{provider}")])
    keyboard.append([InlineKeyboardButton(text="⬅️ К тарифам", callback_data="back_to_subscriptions")])

    await smart_edit_message(callback.message, 
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("buygift:"))
@router.callback_query(F.data.startswith("buygiftlink:"))
async def buy_gift_plan(callback: CallbackQuery, db):
    if not await _ensure_purchases_available(db=db, callback=callback):
        return
    user_id = callback.from_user.id
    recipient_user_id: Optional[int] = None
    if callback.data.startswith("buygiftlink:"):
        _, plan_id = callback.data.split(":", 1)
    else:
        _, recipient_raw, plan_id = callback.data.split(":", 2)
        recipient_user_id = int(recipient_raw)
    plan = get_by_id(plan_id)
    if not plan or not plan.get("active", True):
        await callback.answer("❌ Тариф не найден или недоступен", show_alert=True)
        return
    providers = await _filter_available_payment_providers(db, get_enabled_payment_providers())
    balance = float(await db.get_balance(user_id))
    if balance > 0:
        providers = providers + ["balance"]
    if not providers:
        await callback.answer("❌ Нет включённых способов оплаты", show_alert=True)
        return
    duration = int(plan.get("duration_days", 30))
    offer = await get_purchase_offer(user_id, db, plan, target_user_id=recipient_user_id or user_id)
    price_line = format_amount_line(offer["amount"], duration)
    if recipient_user_id is None:
        gift_note = await _get_pending_gift_note(db, user_id)
        text = (
            "🔗 <b>Оплата подарочной ссылки</b>\n\n"
            f"Тариф: <b>{plan.get('name', plan_id)}</b>\n"
            f"Стоимость: <b>{price_line}</b>\n\n"
            "После оплаты вы получите ссылку, которую можно отправить любому человеку."
        )
        if gift_note:
            text += f"\n\n✍️ Подпись: <i>{gift_note}</i>"
    else:
        text = (
            "🎁 <b>Оплата подарочной подписки</b>\n\n"
            f"Получатель: <code>{recipient_user_id}</code>\n"
            f"Тариф: <b>{plan.get('name', plan_id)}</b>\n"
            f"Стоимость: <b>{price_line}</b>\n"
        )
    promo_line = _promo_offer_line(offer, prefix="🏷 Промокод")
    if promo_line:
        text += f"\n{promo_line}"
    keyboard = []
    for provider in providers:
        button_text = await _provider_button_label(db, provider, balance=balance)
        if recipient_user_id is None:
            keyboard.append([InlineKeyboardButton(text=button_text, callback_data=f"buygiftlink_provider:{plan_id}:{provider}")])
        else:
            keyboard.append([InlineKeyboardButton(text=button_text, callback_data=f"buygift_provider:{recipient_user_id}:{plan_id}:{provider}")])
    back_target = "buy:gift_link_plans" if recipient_user_id is None else "buy:gift_prompt"
    keyboard.append([InlineKeyboardButton(text="⬅️ К выбору тарифа", callback_data=back_target)])
    await smart_edit_message(
        callback.message,
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
        parse_mode="HTML",
    )
    await callback.answer()


async def _resolve_payment_gateway(provider_name: str, default_gateway):
    active_provider = getattr(default_gateway, "provider_name", Config.PAYMENT_PROVIDER)
    if active_provider == provider_name:
        return default_gateway, False
    return build_payment_gateway(provider_name), True


def _resolve_payment_success_url(bot: Bot | None) -> str | None:
    username = str(getattr(bot, "username", "") or "").strip().lstrip("@")
    if username:
        return f"https://t.me/{username}"
    if Config.YOOKASSA_RETURN_URL:
        return Config.YOOKASSA_RETURN_URL
    if Config.TG_CHANNEL:
        return Config.TG_CHANNEL
    return None


async def _pay_plan_with_balance(
    callback: CallbackQuery,
    *,
    db,
    panel,
    user_id: int,
    plan: dict,
    plan_id: str,
    recipient_user_id: Optional[int] = None,
    gift_token: str = "",
    gift_note: str = "",
) -> None:
    target_user_id = int(recipient_user_id or user_id)
    offer = await get_purchase_offer(user_id, db, plan, target_user_id=target_user_id)
    amount = float(offer["amount"])
    if amount <= 0:
        await callback.answer("❌ Некорректная сумма для оплаты", show_alert=True)
        return

    balance_before = float(await db.get_balance(user_id))
    if balance_before < amount:
        await callback.answer(_insufficient_balance_text(balance=balance_before, amount=amount), show_alert=True)
        return

    existing_payment = await db.get_user_pending_payment(user_id, plan_id=plan_id, statuses=["pending", "processing"])
    if existing_payment:
        existing_status = str(existing_payment.get("status") or "").strip().lower()
        existing_provider = str(existing_payment.get("provider") or "").strip().lower()
        if existing_status == "processing":
            await callback.answer("⏳ Этот платёж уже обрабатывается. Подождите немного.", show_alert=True)
            return
        await reject_pending_payment(
            payment=existing_payment,
            db=db,
            bot=callback.bot,
            admin_context=f"User switched provider {existing_provider or 'unknown'}->balance for user {user_id}",
        )

    deducted = await db.subtract_balance(user_id, amount)
    if not deducted:
        balance_now = float(await db.get_balance(user_id))
        await callback.answer(_insufficient_balance_text(balance=balance_now, amount=amount), show_alert=True)
        return

    payment_id = _build_payment_id("pay_balance", user_id)
    gift_label = ""
    if gift_token:
        gift_label = f"giftlink:{gift_token}"
    elif target_user_id != user_id:
        gift_label = f"user:{target_user_id}"
    pending_created = await db.add_pending_payment(
        payment_id=payment_id,
        user_id=user_id,
        plan_id=plan_id,
        amount=amount,
        msg_id=callback.message.message_id,
        provider="balance",
        recipient_user_id=None if target_user_id == user_id else target_user_id,
        promo_code=offer.get("promo_code", ""),
        promo_discount_percent=offer.get("promo_discount_percent", 0.0),
        gift_label=gift_label,
    )
    if not pending_created:
        await db.add_balance(user_id, amount)
        await callback.answer("❌ Не удалось создать внутренний платёж. Попробуйте снова.", show_alert=True)
        return

    result = await process_successful_payment(
        payment={
            "payment_id": payment_id,
            "user_id": user_id,
            "recipient_user_id": None if target_user_id == user_id else target_user_id,
            "plan_id": plan_id,
            "amount": amount,
            "msg_id": callback.message.message_id,
            "provider": "balance",
            "promo_code": offer.get("promo_code", ""),
            "promo_discount_percent": offer.get("promo_discount_percent", 0.0),
            "gift_label": gift_label,
            "gift_note": gift_note,
        },
        db=db,
        panel=panel,
        bot=callback.bot,
        admin_context=f"Balance payment by user {user_id}",
        apply_referral=False,
    )
    if result.get("ok"):
        balance_after = float(await db.get_balance(user_id))
        await callback.answer(
            f"✅ Оплата с баланса прошла успешно. Списано {amount:.2f} ₽. Остаток: {balance_after:.2f} ₽.",
            show_alert=True,
        )
        if gift_token:
            await _clear_pending_gift_note(db, user_id)
        return

    await db.add_balance(user_id, amount)
    await db.update_payment_status(
        payment_id,
        "rejected",
        allowed_current_statuses=["pending", "processing"],
        source=f"balance_revert:{user_id}",
        reason=result.get("reason", "balance_payment_failed"),
        metadata=f"amount={amount}",
    )
    await callback.answer("❌ Не удалось провести оплату с баланса. Средства возвращены.", show_alert=True)


@router.callback_query(F.data.startswith("buy_provider:"))
@router.callback_query(F.data.startswith("buygift_provider:"))
@router.callback_query(F.data.startswith("buygiftlink_provider:"))
async def buy_plan_with_provider(callback: CallbackQuery, db, payment_gateway, panel, bot: Bot):
    if not await _ensure_purchases_available(db=db, callback=callback):
        return
    user_id = callback.from_user.id
    payment_lock = _get_user_payment_lock(user_id)
    async with payment_lock:
        await _buy_plan_with_provider_locked(
            callback,
            db=db,
            payment_gateway=payment_gateway,
            panel=panel,
            bot=bot,
        )


async def _buy_plan_with_provider_locked(callback: CallbackQuery, db, payment_gateway, panel, bot: Bot):
    user_id = callback.from_user.id
    recipient_user_id: Optional[int] = None
    gift_token = ""
    gift_note = ""
    if callback.data.startswith("buygift_provider:"):
        _, recipient_raw, plan_id, provider_name = callback.data.split(":", 3)
        recipient_user_id = int(recipient_raw)
    elif callback.data.startswith("buygiftlink_provider:"):
        _, plan_id, provider_name = callback.data.split(":", 2)
        gift_token = _build_gift_token()
        gift_note = await _get_pending_gift_note(db, user_id)
    else:
        _, plan_id, provider_name = callback.data.split(":", 2)
    plan = get_by_id(plan_id)
    target_user_id = int(recipient_user_id or user_id)

    enabled_providers = await _filter_available_payment_providers(db, get_enabled_payment_providers())
    if provider_name != "balance" and provider_name not in enabled_providers:
        await callback.answer("❌ Этот способ оплаты сейчас недоступен", show_alert=True)
        return
    if not plan or not plan.get("active", True):
        await callback.answer("❌ Тариф не найден или недоступен", show_alert=True)
        return
    if is_trial_plan(plan):
        await callback.answer("⚠️ Пробный тариф оформляется отдельно.", show_alert=True)
        return

    if provider_name == "balance":
        await _pay_plan_with_balance(
            callback,
            db=db,
            panel=panel,
            user_id=user_id,
            plan=plan,
            plan_id=plan_id,
            recipient_user_id=recipient_user_id,
            gift_token=gift_token,
            gift_note=gift_note,
        )
        return

    gateway, should_close_gateway = await _resolve_payment_gateway(provider_name, payment_gateway)
    try:
        existing_payment = await db.get_user_pending_payment(user_id, plan_id=plan_id, statuses=["pending", "processing"])
        if existing_payment:
            existing_status = existing_payment.get("status")
            existing_provider = (existing_payment.get("provider") or provider_name or Config.PAYMENT_PROVIDER).strip().lower()
            if existing_provider == "balance":
                await callback.answer("⏳ Внутренний платёж с баланса уже обрабатывается. Попробуйте чуть позже.", show_alert=True)
                return
            if existing_provider != provider_name and existing_status == "pending":
                await reject_pending_payment(
                    payment=existing_payment,
                    db=db,
                    bot=callback.bot,
                    admin_context=f"User switched provider {existing_provider}->{provider_name} for user {user_id}",
                )
                existing_payment = None
            if existing_payment is None:
                existing_status = None
            existing_gateway, close_existing_gateway = await _resolve_payment_gateway(existing_provider, payment_gateway)
            try:
                if existing_payment and existing_status == "processing":
                    await callback.answer("⏳ Этот платёж уже обрабатывается. Подождите немного.", show_alert=True)
                    return

                if existing_payment and existing_provider == "telegram_stars":
                    promo_existing = await get_purchase_offer(user_id, db, plan, target_user_id=target_user_id)
                    stars_amount = TelegramStarsAPI.resolve_stars_amount(amount_rub=promo_existing["amount"], plan=plan)
                    await bot.send_invoice(
                        chat_id=user_id,
                        title=f"VPN — {plan.get('name', plan_id)}",
                        description=f"Оплата подписки Telegram Stars: {plan.get('name', plan_id)}",
                        payload=TelegramStarsAPI.build_invoice_payload(payment_id=existing_payment["payment_id"], user_id=user_id, plan_id=plan_id),
                        currency="XTR",
                        prices=[LabeledPrice(label=plan.get("name", plan_id), amount=stars_amount)],
                        start_parameter=f"vpn-{plan_id}",
                    )
                    text = (
                        "⭐ <b>Счёт в Telegram Stars отправлен</b>\n\n"
                        f"📦 Тариф: <b>{plan.get('name', plan_id)}</b>\n"
                        f"⭐ Стоимость: <b>{stars_amount} Stars</b>\n\n"
                        "Откройте инвойс ниже в чате и завершите оплату внутри Telegram."
                    )
                    await smart_edit_message(callback.message, 
                        text,
                        reply_markup=_payment_methods_back_markup(plan_id),
                        parse_mode="HTML",
                    )
                    await callback.answer("Инвойс отправлен в чат")
                    return

                provider_payment_id = get_provider_payment_id(existing_payment) if existing_payment else ""
                pay_url = ""
                if provider_payment_id:
                    remote_payment = await existing_gateway.get_payment(provider_payment_id)
                    pay_url = existing_gateway.get_checkout_url(remote_payment)

                    if remote_payment and existing_gateway.is_success_status(remote_payment):
                        result = await process_successful_payment(
                            payment=existing_payment,
                            db=db,
                            panel=panel,
                            bot=callback.bot,
                            admin_context=f"Manual status check by user {user_id}",
                        )
                        if result.get("ok"):
                            await callback.answer("✅ Оплата уже получена. Подписка активирована.", show_alert=True)
                            return
                    elif remote_payment and existing_gateway.is_failed_status(remote_payment):
                        await reject_pending_payment(
                            payment=existing_payment,
                            db=db,
                            bot=callback.bot,
                            admin_context=f"User reopened expired payment {user_id}",
                        )
                        existing_payment = None

                if existing_payment:
                    duration_existing = int(plan.get("duration_days", 30))
                    promo_existing = await get_purchase_offer(user_id, db, plan, target_user_id=target_user_id)
                    price_line_existing = format_amount_line(promo_existing["amount"], duration_existing)
                    text = (
                        "💳 <b>У вас уже есть незавершённый платёж</b>\n\n"
                        f"📦 Тариф: <b>{plan.get('name', plan_id)}</b>\n"
                        f"💰 Сумма: <b>{price_line_existing}</b>\n"
                        f"🏦 Способ оплаты: <b>{await _provider_label(db, existing_provider)}</b>\n"
                        f"🧾 ID: <code>{existing_payment['payment_id']}</code>\n\n"
                        "Используйте текущую ссылку оплаты или проверьте статус после оплаты."
                    )
                    inline = []
                    if pay_url:
                        inline.append([InlineKeyboardButton(text="💳 Открыть оплату", url=pay_url)])
                    inline.append([InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_payment:{existing_payment['payment_id']}")])
                    inline.append([InlineKeyboardButton(text="⬅️ К способам оплаты", callback_data=f"buy:{plan_id}")])
                    await smart_edit_message(callback.message, 
                        text,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=inline),
                        parse_mode="HTML",
                    )
                    await callback.answer()
                    return
            finally:
                if close_existing_gateway:
                    await existing_gateway.close()

        can_create, guard_reason = await guard_payment_creation(user_id, db=db, bot=callback.bot)
        if not can_create:
            await callback.answer(f"⚠️ {guard_reason}", show_alert=True)
            return

        promo = await get_purchase_offer(user_id, db, plan, target_user_id=target_user_id)
        amount = promo["amount"]
        payment_id = _build_payment_id("pay", user_id)
        plan_name = plan.get("name", plan_id)

        gateway_payment = await gateway.create_payment(
            amount=amount,
            client_payment_id=payment_id,
            user_id=user_id,
            plan_id=plan_id,
            description=f"Подписка: {plan_name}",
            success_url=_resolve_payment_success_url(bot),
            plan=plan,
        )
        if not gateway_payment:
            gateway_error = str(getattr(gateway, "last_error_message", "") or "").strip()
            if _should_hide_provider_after_error(provider_name, gateway_error):
                await _set_payment_provider_hidden(db, provider_name, True)
            if gateway_error:
                await callback.answer(f"❌ {gateway_error}", show_alert=True)
            else:
                await callback.answer("❌ Ошибка создания платежа, попробуйте позже", show_alert=True)
            return

        await db.add_pending_payment(
            payment_id=payment_id,
            user_id=user_id,
            plan_id=plan_id,
            amount=amount,
            msg_id=None if provider_name == "telegram_stars" else callback.message.message_id,
            provider=provider_name,
            recipient_user_id=None if target_user_id == user_id else target_user_id,
            promo_code=promo.get("promo_code", ""),
            promo_discount_percent=promo.get("promo_discount_percent", 0.0),
            gift_label=f"giftlink:{gift_token}" if gift_token else (f"user:{target_user_id}" if target_user_id != user_id else ""),
            gift_note=gift_note,
        )

        provider_payment_id = gateway_payment.get("id", "")
        if provider_payment_id:
            bound = await db.set_pending_payment_provider_id(payment_id, provider_name, provider_payment_id)
            if not bound:
                await db.update_payment_status(
                    payment_id,
                    "rejected",
                    allowed_current_statuses=["pending"],
                    source=f"create_payment:{provider_name}",
                    reason="provider_payment_id_conflict",
                    metadata=f"provider_payment_id={provider_payment_id}",
                )
                await callback.answer("⚠️ Платёж уже зарегистрирован. Откройте предыдущий незавершённый платёж.", show_alert=True)
                return

        duration = int(plan.get("duration_days", 30))
        price_line = format_amount_line(amount, duration)
        if gift_token:
            gift_line = "\n🔗 После оплаты вы получите подарочную ссылку для активации."
        else:
            gift_line = f"\n🎁 Получатель: <code>{target_user_id}</code>" if target_user_id != user_id else ""
        promo_line = f"\n{_promo_offer_line(promo, prefix='🏷 Промокод')}" if promo.get("promo_code") else ""

        if provider_name == "telegram_stars":
            stars_amount = int(gateway_payment.get("stars_amount", 0) or 0)
            await bot.send_invoice(
                chat_id=user_id,
                title=f"VPN — {plan_name}",
                description=f"Оплата подписки Telegram Stars: {plan_name}",
                payload=gateway_payment["invoice_payload"],
                currency="XTR",
                prices=[LabeledPrice(label=plan_name, amount=stars_amount)],
                start_parameter=f"vpn-{plan_id}",
            )
            text = (
                "⭐ <b>Счёт готов</b>\n\n"
                f"📦 Тариф: <b>{plan_name}</b>\n"
                f"⭐ Стоимость: <b>{stars_amount} Stars</b>\n"
                f"💼 Эквивалент тарифа: <b>{price_line}</b>\n\n"
                "Оплата пройдёт прямо внутри Telegram.\n"
                "После подтверждения подписка активируется автоматически, и бот сразу покажет, как подключиться."
            )
            text += f"{gift_line}{promo_line}"
            inline = _payment_methods_back_markup(plan_id).inline_keyboard
        else:
            pay_url = gateway.get_checkout_url(gateway_payment)
            text = (
                f"💳 <b>Оплата через {await _provider_label(db, provider_name)}</b>\n\n"
                f"📦 Тариф: <b>{plan_name}</b>\n"
                f"💰 Сумма: <b>{price_line}</b>\n\n"
                "Нажмите кнопку ниже для перехода к оплате.\n"
                "После оплаты подписка активируется <b>автоматически</b>.\n"
                "Обычно это занимает <b>до 1 минуты</b>, после чего бот сразу проведёт вас к подключению.\n"
                "Если нужно, статус можно проверить вручную."
            )
            text += f"{gift_line}{promo_line}"
            inline = []
            if pay_url:
                inline.append([InlineKeyboardButton(text=f"💳 Оплатить через {await _provider_label(db, provider_name)}", url=pay_url)])
            inline.append([InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_payment:{payment_id}")])
            inline.append([InlineKeyboardButton(text="⬅️ К способам оплаты", callback_data=f"buy:{plan_id}")])

        await smart_edit_message(callback.message, 
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=inline),
            parse_mode="HTML",
        )
        if gift_token:
            await _clear_pending_gift_note(db, user_id)
        await callback.answer()
    finally:
        if should_close_gateway:
            await gateway.close()

@router.callback_query(F.data == "cancel_payment")
async def cancel_payment(callback: CallbackQuery, db):
    await show_plans_list(callback.from_user.id, db=db, bot=callback.bot, message_id=callback.message.message_id, callback_message=callback.message)
    await callback.answer()


@router.callback_query(F.data.startswith("check_payment:"))
async def check_payment_status(callback: CallbackQuery, db, payment_gateway, panel):
    if not await _ensure_purchases_available(db=db, callback=callback):
        return
    user_lock = _get_user_payment_lock(callback.from_user.id)
    async with user_lock:
        payment_id = callback.data.split(":", 1)[1]
        payment = await db.get_pending_payment(payment_id)
        if not payment:
            await callback.answer("❌ Платёж не найден", show_alert=True)
            return

        owner_id = int(payment.get("user_id", 0) or 0)
        if callback.from_user.id != owner_id and callback.from_user.id not in Config.ADMIN_USER_IDS:
            await callback.answer("⛔ Этот платёж вам не принадлежит", show_alert=True)
            return

        status = payment.get("status")
        if status == "accepted":
            await callback.answer("✅ Платёж уже подтверждён", show_alert=True)
            return
        if status == "rejected":
            await callback.answer("❌ Платёж уже отклонён", show_alert=True)
            return
        if status == "processing":
            await callback.answer("⏳ Платёж сейчас обрабатывается. Мы продолжаем автоматическую проверку, обычно это занимает до 1 минуты.", show_alert=True)
            return

        if getattr(payment_gateway, "provider_name", Config.PAYMENT_PROVIDER) == "telegram_stars":
            await callback.answer(
                "⭐ Для Telegram Stars ручная проверка не нужна: подписка активируется после successful_payment от Telegram.",
                show_alert=True,
            )
            return

        provider_payment_id = get_provider_payment_id(payment)
        if not provider_payment_id:
            await callback.answer("⚠️ Для этого платежа ещё не получен внешний ID", show_alert=True)
            return

        remote_payment = await payment_gateway.get_payment(provider_payment_id)
        if not remote_payment:
            await callback.answer("⏳ Платёж ещё не найден в системе оплаты. Попробуйте чуть позже.", show_alert=True)
            return

        remote_status = payment_gateway.extract_status(remote_payment)
        if payment_gateway.is_success_status(remote_payment):
            result = await process_successful_payment(
                payment=payment,
                db=db,
                panel=panel,
                bot=callback.bot,
                admin_context=f"Manual status check by user {callback.from_user.id}",
            )
            if result.get("ok"):
                await callback.answer("✅ Оплата подтверждена, подписка активирована.", show_alert=True)
                return
            await callback.answer(f"⏳ Платёж получен, но активация ещё не завершена: {result.get('reason', 'unknown')}", show_alert=True)
            return

        if payment_gateway.is_failed_status(remote_payment):
            await reject_pending_payment(
                payment=payment,
                db=db,
                bot=callback.bot,
                admin_context=f"Manual failed status check by user {callback.from_user.id}: {remote_status}",
            )
            await callback.answer("❌ Платёж не был завершён.", show_alert=True)
            return

        await callback.answer(f"⏳ Платёж ещё ожидает подтверждения ({remote_status or 'pending'}).", show_alert=True)

@router.pre_checkout_query()
async def pre_checkout_query_handler(pre_checkout_query: PreCheckoutQuery, db):
    payload_data = TelegramStarsAPI.parse_invoice_payload(pre_checkout_query.invoice_payload or "")
    if not payload_data:
        await pre_checkout_query.answer(ok=False, error_message="Некорректный invoice payload")
        return
    payment = await db.get_pending_payment(payload_data["payment_id"])
    if not payment or payment.get("status") not in {"pending", "processing"}:
        await pre_checkout_query.answer(ok=False, error_message="Платёж уже недоступен")
        return
    if payment.get("provider") != "telegram_stars":
        await pre_checkout_query.answer(ok=False, error_message="Неверный платёжный провайдер")
        return
    await pre_checkout_query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment_handler(message: Message, db, panel):
    payment_info = message.successful_payment
    if not payment_info or payment_info.currency != "XTR":
        return
    payload_data = TelegramStarsAPI.parse_invoice_payload(payment_info.invoice_payload or "")
    if not payload_data:
        return

    event_key = f"telegram_stars:successful_payment:{payment_info.telegram_payment_charge_id}:{payload_data['payment_id']}"
    registered = await db.register_payment_event(
        event_key,
        payment_id=payload_data["payment_id"],
        source="telegram/successful_payment",
        event_type="successful_payment",
        payload_excerpt=f"charge_id={payment_info.telegram_payment_charge_id};provider_charge={payment_info.provider_payment_charge_id or ''}",
    )
    if not registered:
        return

    payment = await db.get_pending_payment(payload_data["payment_id"])
    if not payment:
        return
    if payment.get("provider") != "telegram_stars":
        return

    await db.set_pending_payment_provider_id(
        payload_data["payment_id"],
        "telegram_stars",
        payment_info.telegram_payment_charge_id,
    )

    result = await process_successful_payment(
        payment=payment,
        db=db,
        panel=panel,
        bot=message.bot,
        admin_context="Telegram Stars successful_payment",
    )
    if not result.get("ok") and result.get("reason") != "already_processing":
        logger.error(
            "telegram_stars successful_payment activation failed payment=%s reason=%s",
            payload_data["payment_id"],
            result.get("reason"),
        )
