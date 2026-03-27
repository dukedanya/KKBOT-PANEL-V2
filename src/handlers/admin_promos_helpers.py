from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from db import Database


def _promo_menu_keyboard(rows: list[dict]) -> InlineKeyboardMarkup:
    keyboard_rows = [
        [InlineKeyboardButton(text="➕ Добавить промокод", callback_data="admin:promo_add_prompt")],
        [InlineKeyboardButton(text="📊 Статистика промо и подарков", callback_data="admin:promo_stats")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin:promo_menu")],
    ]
    for row in rows[:10]:
        state_label = "🟢" if int(row.get("active") or 0) == 1 else "⚪️"
        keyboard_rows.append([
            InlineKeyboardButton(
                text=f"{state_label} {row.get('code')}",
                callback_data=f"admin:promo_view:{row.get('code')}",
            )
        ])
    keyboard_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_admin")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


async def _build_promo_menu_text(db: Database) -> tuple[str, list[dict]]:
    rows = await db.list_promo_codes(limit=20) if hasattr(db, "list_promo_codes") else []
    lines = [
        "🏷 <b>Промокоды</b>",
        "",
        "Создание форматом:",
        "<code>CODE | TYPE | VALUE | MAX_USES | USER_LIMIT | PLAN_IDS | NEW_ONLY | TITLE</code>",
        "TYPE: <code>percent</code> или <code>fixed</code>",
        "Пример: <code>SPRING25 | percent | 25 | 100 | 1 | basic,pro | 0 | Весенняя акция</code>",
        "Пример: <code>WELCOME500 | fixed | 500 | 1 | 1 |  | 1 | Только новым</code>",
        "",
    ]
    if not rows:
        lines.append("Промокодов пока нет.")
    else:
        for row in rows:
            discount_type = str(row.get("discount_type") or "percent")
            if discount_type == "fixed":
                discount_label = f"{float(row.get('fixed_amount') or 0):.0f} ₽"
            else:
                discount_label = f"{float(row.get('discount_percent') or 0):.0f}%"
            flags = []
            if int(row.get("only_new_users") or 0) == 1:
                flags.append("только новым")
            if str(row.get("plan_ids") or "").strip():
                flags.append(f"тарифы: {row.get('plan_ids')}")
            lines.append(
                f"• <code>{row.get('code')}</code> — <b>{discount_label}</b> "
                f"/ использовано <b>{int(row.get('used_count') or 0)}</b>"
                + (f" из {int(row.get('max_uses') or 0)}" if int(row.get('max_uses') or 0) > 0 else "")
                + f" / {'активен' if int(row.get('active') or 0) == 1 else 'выключен'}"
                + (f" / {', '.join(flags)}" if flags else "")
            )
    return "\n".join(lines), rows


def _promo_detail_keyboard(code: str, promo: dict) -> InlineKeyboardMarkup:
    is_active = int(promo.get("active") or 0) == 1
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔴 Выключить" if is_active else "🟢 Включить", callback_data=f"admin:promo_toggle:{code}")],
        [InlineKeyboardButton(text="📆 Срок действия", callback_data=f"admin:promo_edit:expires_at:{code}")],
        [InlineKeyboardButton(text="👤 Лимит на пользователя", callback_data=f"admin:promo_edit:user_limit:{code}")],
        [InlineKeyboardButton(text="🔢 Общий лимит", callback_data=f"admin:promo_edit:max_uses:{code}")],
        [InlineKeyboardButton(text="👁 Кто использовал", callback_data=f"admin:promo_usage:{code}")],
        [InlineKeyboardButton(text="⬅️ К промокодам", callback_data="admin:promo_menu")],
    ])


def _build_promo_detail_text(promo: dict) -> str:
    discount_type = str(promo.get("discount_type") or "percent")
    discount_label = f"{float(promo.get('fixed_amount') or 0):.0f} ₽" if discount_type == "fixed" else f"{float(promo.get('discount_percent') or 0):.0f}%"
    expires_at = str(promo.get("expires_at") or "").strip() or "не ограничен"
    plan_ids = str(promo.get("plan_ids") or "").strip() or "все тарифы"
    user_limit = int(promo.get("user_limit") or 0)
    return (
        f"🏷 <b>Промокод {promo.get('code')}</b>\n\n"
        f"Название: <b>{promo.get('title') or '-'}</b>\n"
        f"Скидка: <b>{discount_label}</b>\n"
        f"Статус: <b>{'активен' if int(promo.get('active') or 0) == 1 else 'выключен'}</b>\n"
        f"Общий лимит: <b>{int(promo.get('max_uses') or 0) or 'без лимита'}</b>\n"
        f"Лимит на пользователя: <b>{user_limit or 'без лимита'}</b>\n"
        f"Только новым: <b>{'да' if int(promo.get('only_new_users') or 0) == 1 else 'нет'}</b>\n"
        f"Тарифы: <b>{plan_ids}</b>\n"
        f"Срок действия: <b>{expires_at}</b>\n"
        f"Использовано: <b>{int(promo.get('used_count') or 0)}</b>\n"
        f"Описание: <i>{promo.get('description') or '-'}</i>"
    )
