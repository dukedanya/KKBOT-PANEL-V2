from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from db import Database
from handlers.admin_promos_helpers import (
    _build_promo_detail_text,
    _build_promo_menu_text,
    _promo_detail_keyboard,
    _promo_menu_keyboard,
)
from handlers.admin import (
    PromoCodeFSM,
    is_admin,
)
from utils.telegram_ui import smart_edit_message

router = Router()


@router.callback_query(F.data == "admin:promo_menu")
async def admin_promo_menu(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    text, rows = await _build_promo_menu_text(db)
    await smart_edit_message(callback.message, text, reply_markup=_promo_menu_keyboard(rows), parse_mode="HTML")
    await callback.answer()


@router.callback_query(F.data.startswith("admin:promo_view:"))
async def admin_promo_view(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    code = callback.data.split(":", 2)[2].strip().upper()
    promo = await db.get_promo_code(code)
    if not promo:
        await callback.answer("Промокод не найден", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        _build_promo_detail_text(promo),
        reply_markup=_promo_detail_keyboard(code, promo),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:promo_add_prompt")
async def admin_promo_add_prompt(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.set_state(PromoCodeFSM.content)
    await smart_edit_message(
        callback.message,
        (
            "🏷 <b>Новый промокод</b>\n\n"
            "Отправьте строку в формате:\n"
            "<code>CODE | TYPE | VALUE | MAX_USES | USER_LIMIT | PLAN_IDS | NEW_ONLY | TITLE</code>\n\n"
            "Где:\n"
            "• TYPE: <code>percent</code> или <code>fixed</code>\n"
            "• VALUE: процент скидки или фиксированная сумма в рублях\n"
            "• USER_LIMIT: сколько раз один пользователь может применить код\n"
            "• PLAN_IDS: список тарифов через запятую или пусто\n"
            "• NEW_ONLY: <code>1</code> только для новых, <code>0</code> для всех\n\n"
            "Пример:\n"
            "<code>SPRING25 | percent | 25 | 100 | 1 | basic,pro | 0 | Весенняя акция</code>"
        ),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin:promo_menu")]]),
    )
    await callback.answer()


@router.message(PromoCodeFSM.content)
async def admin_promo_add_save(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw_text = (message.text or "").strip()
    if "|" in raw_text:
        parts = [part.strip() for part in raw_text.split("|")]
        if len(parts) < 8:
            await message.answer("❌ Формат: CODE | TYPE | VALUE | MAX_USES | USER_LIMIT | PLAN_IDS | NEW_ONLY | TITLE")
            return
        code = parts[0].upper()
        discount_type = parts[1].lower()
        plan_ids = parts[5]
        only_new_users = parts[6] in {"1", "true", "yes", "да"}
        title = parts[7] or f"Промокод {code}"
        try:
            value = float(parts[2].replace(",", "."))
            max_uses = int(parts[3])
            user_limit = int(parts[4])
        except ValueError:
            await message.answer("❌ Не удалось разобрать VALUE, MAX_USES или USER_LIMIT.")
            return
        if discount_type not in {"percent", "fixed"}:
            await message.answer("❌ TYPE должен быть percent или fixed.")
            return
        if value <= 0:
            await message.answer("❌ VALUE должен быть больше 0.")
            return
        discount = value if discount_type == "percent" else 0.0
        fixed_amount = value if discount_type == "fixed" else 0.0
        if discount_type == "percent" and discount >= 100:
            await message.answer("❌ Процентная скидка должна быть меньше 100.")
            return
    else:
        parts = raw_text.split(maxsplit=3)
        if len(parts) < 3:
            await message.answer("❌ Формат: CODE DISCOUNT MAX_USES [TITLE]")
            return
        code = parts[0].strip().upper()
        title = parts[3].strip() if len(parts) > 3 else f"Промокод {code}"
        try:
            discount = float(parts[1].replace(",", "."))
            max_uses = int(parts[2])
        except ValueError:
            await message.answer("❌ Не удалось разобрать скидку или лимит.")
            return
        if discount <= 0 or discount >= 100:
            await message.answer("❌ Скидка должна быть больше 0 и меньше 100.")
            return
        discount_type = "percent"
        fixed_amount = 0.0
        only_new_users = False
        plan_ids = ""
        user_limit = 0
    await db.create_or_update_promo_code(
        code,
        title=title,
        description=f"Создан администратором {message.from_user.id}",
        discount_percent=discount,
        discount_type=discount_type,
        fixed_amount=fixed_amount,
        only_new_users=only_new_users,
        plan_ids=plan_ids,
        user_limit=user_limit,
        max_uses=max_uses,
        active=True,
    )
    await state.clear()
    text, rows = await _build_promo_menu_text(db)
    await message.answer(text, reply_markup=_promo_menu_keyboard(rows), parse_mode="HTML")


@router.callback_query(F.data.startswith("admin:promo_toggle:"))
async def admin_promo_toggle(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    code = callback.data.split(":", 2)[2].strip().upper()
    promo = await db.get_promo_code(code)
    if not promo:
        await callback.answer("Промокод не найден", show_alert=True)
        return
    await db.create_or_update_promo_code(
        code,
        title=str(promo.get("title") or ""),
        description=str(promo.get("description") or ""),
        discount_percent=float(promo.get("discount_percent") or 0.0),
        discount_type=str(promo.get("discount_type") or "percent"),
        fixed_amount=float(promo.get("fixed_amount") or 0.0),
        only_new_users=int(promo.get("only_new_users") or 0) == 1,
        plan_ids=str(promo.get("plan_ids") or ""),
        user_limit=int(promo.get("user_limit") or 0),
        max_uses=int(promo.get("max_uses") or 0),
        active=int(promo.get("active") or 0) != 1,
        expires_at=promo.get("expires_at"),
    )
    updated = await db.get_promo_code(code)
    await smart_edit_message(callback.message, _build_promo_detail_text(updated), reply_markup=_promo_detail_keyboard(code, updated), parse_mode="HTML")
    await callback.answer("Статус промокода обновлён")


@router.callback_query(F.data.startswith("admin:promo_edit:"))
async def admin_promo_edit_prompt(callback: CallbackQuery, state: FSMContext, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    _, _, field, code = callback.data.split(":", 3)
    promo = await db.get_promo_code(code.upper())
    if not promo:
        await callback.answer("Промокод не найден", show_alert=True)
        return
    await state.set_state(PromoCodeFSM.edit_value)
    await state.update_data(promo_code=code.upper(), promo_field=field)
    field_hint = {
        "expires_at": "Отправьте дату в формате YYYY-MM-DD или слово <code>none</code>.",
        "user_limit": "Отправьте число. <code>0</code> = без лимита на пользователя.",
        "max_uses": "Отправьте число. <code>0</code> = без общего лимита.",
    }.get(field, "Отправьте новое значение.")
    await smart_edit_message(
        callback.message,
        f"✏️ <b>Редактирование {code.upper()}</b>\n\n{field_hint}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin:promo_view:{code.upper()}")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(PromoCodeFSM.edit_value)
async def admin_promo_edit_save(message: Message, state: FSMContext, db: Database):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    code = str(data.get("promo_code") or "").upper()
    field = str(data.get("promo_field") or "")
    promo = await db.get_promo_code(code)
    if not promo:
        await state.clear()
        await message.answer("❌ Промокод не найден.")
        return
    raw = (message.text or "").strip()
    kwargs = dict(
        title=str(promo.get("title") or ""),
        description=str(promo.get("description") or ""),
        discount_percent=float(promo.get("discount_percent") or 0.0),
        discount_type=str(promo.get("discount_type") or "percent"),
        fixed_amount=float(promo.get("fixed_amount") or 0.0),
        only_new_users=int(promo.get("only_new_users") or 0) == 1,
        plan_ids=str(promo.get("plan_ids") or ""),
        user_limit=int(promo.get("user_limit") or 0),
        max_uses=int(promo.get("max_uses") or 0),
        active=int(promo.get("active") or 0) == 1,
        expires_at=promo.get("expires_at"),
    )
    try:
        if field == "expires_at":
            kwargs["expires_at"] = None if raw.lower() in {"none", "0", "-"} else f"{raw}T23:59:59+00:00"
        elif field == "user_limit":
            kwargs["user_limit"] = max(0, int(raw))
        elif field == "max_uses":
            kwargs["max_uses"] = max(0, int(raw))
        else:
            await message.answer("❌ Неизвестное поле.")
            return
    except ValueError:
        await message.answer("❌ Не удалось разобрать значение.")
        return
    await db.create_or_update_promo_code(code, **kwargs)
    await state.clear()
    updated = await db.get_promo_code(code)
    await message.answer(_build_promo_detail_text(updated), reply_markup=_promo_detail_keyboard(code, updated), parse_mode="HTML")


@router.callback_query(F.data.startswith("admin:promo_usage:"))
async def admin_promo_usage(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    code = callback.data.split(":", 2)[2].strip().upper()
    rows = await db.get_promo_code_usage_details(code, limit=20) if hasattr(db, "get_promo_code_usage_details") else []
    lines = [f"👁 <b>Использование промокода {code}</b>", ""]
    if not rows:
        lines.append("Этот промокод пока никто не использовал.")
    else:
        for row in rows:
            lines.append(
                f"• <code>{row.get('user_id')}</code> — {int(row.get('used_count') or 0)} раз, последнее: <code>{row.get('last_used_at') or '-'}</code>"
            )
    await smart_edit_message(
        callback.message,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К карточке промокода", callback_data=f"admin:promo_view:{code}")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin:promo_stats")
async def admin_promo_stats(callback: CallbackQuery, db: Database):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    gift_stats = await db.get_gift_links_stats() if hasattr(db, "get_gift_links_stats") else {"total": 0, "claimed": 0, "unclaimed": 0}
    promo_stats = await db.get_promo_code_stats() if hasattr(db, "get_promo_code_stats") else []
    lines = [
        "📊 <b>Статистика промокодов и подарков</b>",
        "",
        f"🎁 Подарочных ссылок создано: <b>{int(gift_stats.get('total') or 0)}</b>",
        f"✅ Активировано: <b>{int(gift_stats.get('claimed') or 0)}</b>",
        f"⏳ Ещё не активировано: <b>{int(gift_stats.get('unclaimed') or 0)}</b>",
        "",
        "🏷 <b>Промокоды</b>",
    ]
    if not promo_stats:
        lines.append("Статистика по промокодам пока пустая.")
    else:
        for row in promo_stats[:10]:
            if str(row.get("discount_type") or "percent") == "fixed":
                discount_label = f"{float(row.get('fixed_amount') or 0):.0f} ₽"
            else:
                discount_label = f"{float(row.get('discount_percent') or 0):.0f}%"
            lines.append(
                f"• <code>{row.get('code')}</code> — {discount_label}, "
                f"покупок: <b>{int(row.get('payments_count') or 0)}</b>, "
                f"оборот: <b>{float(row.get('total_amount') or 0):.2f} ₽</b>, "
                f"использований: <b>{int(row.get('used_count') or 0)}</b>"
            )
    await smart_edit_message(
        callback.message,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К промокодам", callback_data="admin:promo_menu")]]),
        parse_mode="HTML",
    )
    await callback.answer()
