from __future__ import annotations

from html import escape
from typing import Any, Dict

from aiogram import Bot

from db import Database
from handlers.admin_user_card_helpers import _resolve_repairable_payment, _resolve_user_current_plan
from kkbot.services.subscriptions import create_subscription, panel_base_email
from kkbot.services.payment_flow import process_successful_payment
from services.payment_flow import apply_referral_reward
from tariffs import get_by_id
from utils.helpers import notify_user


async def grant_custom_tariff_days(
    *,
    actor_user_id: int,
    target_user_id: int,
    plan_id: str,
    days: int,
    db: Database,
    panel,
    bot: Bot,
) -> tuple[bool, str]:
    plan = get_by_id(plan_id)
    if not plan:
        return False, "Тариф не найден"
    custom_days = max(1, int(days))
    custom_plan = dict(plan)
    custom_plan["duration_days"] = custom_days
    vpn_url = await create_subscription(
        target_user_id,
        custom_plan,
        db=db,
        panel=panel,
        plan_suffix=" (выдан админом)",
        preserve_active_days=True,
    )
    if not vpn_url:
        return False, "Не удалось выдать тариф"
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(target_user_id, actor_user_id, "grant_tariff_custom", f"plan_id={plan_id} days={custom_days}")
    plan_name = str(plan.get("name") or plan_id)
    await notify_user(
        bot,
        target_user_id,
        "🎁 <b>Вам выдан тариф</b>\n\n"
        f"Тариф: <b>{escape(plan_name)}</b>\n"
        f"Срок: <b>{custom_days}</b> дней\n"
        "Подключение уже готово в личном кабинете.",
    )
    return True, f"✅ Тариф выдан вручную на <b>{custom_days}</b> дней.\n\n"


async def rebuild_user_subscription(
    *,
    actor_user_id: int,
    target_user_id: int,
    db: Database,
    panel,
    bot: Bot,
) -> tuple[bool, str]:
    user = await db.get_user(target_user_id)
    if not user:
        return False, "Пользователь не найден"
    plan = await _resolve_user_current_plan(db, target_user_id)
    if not plan:
        return False, "Не удалось определить тариф пользователя"
    vpn_url = await create_subscription(target_user_id, plan, db=db, panel=panel, preserve_active_days=True)
    if not vpn_url:
        return False, "Не удалось пересобрать подписку"
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(target_user_id, actor_user_id, "rebuild_subscription", f"plan_id={plan.get('id')}")
    await notify_user(
        bot,
        target_user_id,
        "♻️ <b>Ссылка подключения пересобрана администратором</b>\n\nОткройте личный кабинет и используйте обновлённую подписку.",
    )
    return True, "✅ Ссылка и подписка пересобраны.\n\n"


async def repair_user_payment(
    *,
    actor_user_id: int,
    target_user_id: int,
    db: Database,
    panel,
    bot: Bot,
) -> tuple[bool, str]:
    payment = await _resolve_repairable_payment(db, target_user_id)
    if not payment:
        return False, "Подходящий платёж не найден"
    status = str(payment.get("status") or "").lower()
    result: Dict[str, Any]
    if status in {"pending", "processing"}:
        result = await process_successful_payment(
            payment=payment,
            db=db,
            panel=panel,
            bot=bot,
            admin_context=f"Ручной repair payment из карточки {actor_user_id}",
        )
    else:
        plan = get_by_id(str(payment.get("plan_id") or ""))
        if not plan:
            return False, "Тариф платежа не найден"
        vpn_url = await create_subscription(target_user_id, plan, db=db, panel=panel, preserve_active_days=True)
        if not vpn_url:
            return False, "Не удалось пересобрать доступ по платежу"
        user_data = await db.get_user(target_user_id)
        await apply_referral_reward(target_user_id, float(payment.get("amount") or 0), user_data, db, panel)
        result = {"ok": True, "manual_repair": True}
    if not result.get("ok"):
        return False, f"Repair не выполнен: {result.get('reason', 'unknown')}"
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(
            target_user_id,
            actor_user_id,
            "repair_payment",
            f"payment_id={payment.get('payment_id')} status={payment.get('status')}",
        )
    return True, "🩺 Платёж обработан вручную.\n\n"


async def extend_user_tariff(
    *,
    actor_user_id: int,
    target_user_id: int,
    plan_id: str,
    db: Database,
    panel,
    bot: Bot,
) -> tuple[bool, str]:
    user = await db.get_user(target_user_id)
    if not user:
        return False, "Пользователь не найден"
    plan = get_by_id(plan_id)
    if not plan:
        return False, "Тариф не найден"
    vpn_url = await create_subscription(target_user_id, plan, db=db, panel=panel, preserve_active_days=True)
    if not vpn_url:
        return False, "Не удалось продлить тариф"
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(target_user_id, actor_user_id, "extend_tariff", f"plan_id={plan_id}")
    await notify_user(
        bot,
        target_user_id,
        "⏫ <b>Подписка продлена</b>\n\n"
        f"Тариф: <b>{escape(str(plan.get('name') or plan_id))}</b>\n"
        "Новый срок уже применён в личном кабинете.",
    )
    return True, "✅ Тариф продлён.\n\n"


async def change_user_tariff(
    *,
    actor_user_id: int,
    target_user_id: int,
    plan_id: str,
    db: Database,
    panel,
    bot: Bot,
) -> tuple[bool, str]:
    user = await db.get_user(target_user_id)
    if not user:
        return False, "Пользователь не найден"
    plan = get_by_id(plan_id)
    if not plan:
        return False, "Тариф не найден"
    vpn_url = await create_subscription(target_user_id, plan, db=db, panel=panel, preserve_active_days=True)
    if not vpn_url:
        return False, "Не удалось сменить тариф"
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(target_user_id, actor_user_id, "change_tariff", f"plan_id={plan_id}")
    await notify_user(
        bot,
        target_user_id,
        "🔄 <b>Тариф обновлён администратором</b>\n\n"
        f"Новый тариф: <b>{escape(str(plan.get('name') or plan_id))}</b>\n"
        "Срок действия сохранён.",
    )
    return True, "✅ Тариф изменён.\n\n"


async def add_user_bonus_days(
    *,
    actor_user_id: int,
    target_user_id: int,
    bonus_days: int,
    db: Database,
    panel,
) -> tuple[bool, str]:
    user = await db.get_user(target_user_id)
    if not user:
        return False, "Пользователь не найден"
    base_email = await panel_base_email(target_user_id, db)
    if not base_email:
        return False, "Не найден email клиента в панели"
    ok = await panel.extend_client_expiry(base_email, bonus_days)
    if not ok:
        return False, "Не удалось добавить бонусные дни"
    await db.reset_expiry_notifications(target_user_id)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(target_user_id, actor_user_id, "add_bonus_days", f"days={bonus_days}")
    return True, f"✅ Добавлено <b>{bonus_days}</b> дней.\n\n"


async def grant_user_tariff(
    *,
    actor_user_id: int,
    target_user_id: int,
    plan_id: str,
    db: Database,
    panel,
    bot: Bot,
) -> tuple[bool, str]:
    user = await db.get_user(target_user_id)
    if not user:
        return False, "Пользователь не найден"
    plan = get_by_id(plan_id)
    if not plan:
        return False, "Тариф не найден"
    vpn_url = await create_subscription(target_user_id, plan, db=db, panel=panel, plan_suffix=" (выдан админом)", preserve_active_days=True)
    if not vpn_url:
        return False, "Не удалось выдать тариф"
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(target_user_id, actor_user_id, "grant_tariff", f"plan_id={plan_id}")
    plan_name = str(plan.get("name") or plan_id)
    await notify_user(
        bot,
        target_user_id,
        "🎁 <b>Вам выдан тариф</b>\n\n"
        f"Тариф: <b>{escape(plan_name)}</b>\n"
        "Подключение уже готово в личном кабинете.",
    )
    return True, "✅ Тариф выдан вручную.\n\n"


async def reset_user_trial(
    *,
    actor_user_id: int,
    target_user_id: int,
    db: Database,
) -> tuple[bool, str]:
    user = await db.get_user(target_user_id)
    if not user:
        return False, "Пользователь не найден"
    await db.update_user(target_user_id, trial_used=0, trial_declined=0)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(target_user_id, actor_user_id, "reset_trial", "trial_used=0 trial_declined=0")
    return True, "♻️ Возможность активировать пробный период сброшена.\n\n"


async def save_user_balance_adjustment(
    *,
    actor_user_id: int,
    target_user_id: int,
    amount: float,
    reason: str,
    db: Database,
) -> str:
    await db.add_user(target_user_id)
    await db.add_referral_balance_adjustment(target_user_id, actor_user_id, amount, reason)
    return "✅ Баланс обновлён.\n\n"


async def save_user_partner_rates(
    *,
    actor_user_id: int,
    target_user_id: int,
    l1,
    l2,
    l3,
    status: str,
    note: str,
    db: Database,
) -> str:
    await db.set_partner_rates(target_user_id, l1, l2, l3, status=status, note=note)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(
            target_user_id,
            actor_user_id,
            "partner_rates_update",
            f"l1={l1};l2={l2};l3={l3};status={status};note={note}",
        )
    return "✅ Специальные условия обновлены.\n\n"


async def save_user_referrer(
    *,
    actor_user_id: int,
    target_user_id: int,
    referrer_id: int,
    db: Database,
) -> str:
    await db.set_ref_by(target_user_id, referrer_id)
    if referrer_id <= 0:
        await db.update_user(target_user_id, ref_rewarded=0)
    if hasattr(db, "add_admin_user_action"):
        await db.add_admin_user_action(target_user_id, actor_user_id, "rebind_referrer", f"ref_by={referrer_id}")
    return "✅ Реферальная привязка обновлена.\n\n" if referrer_id > 0 else "✅ Реферальная привязка снята.\n\n"
