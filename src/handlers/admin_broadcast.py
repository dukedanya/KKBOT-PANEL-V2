import logging

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import Config
from db import Database
from keyboards import main_menu_keyboard
from services.admin_bulk import enqueue_broadcast_job, enqueue_extend_job, get_bulk_job, list_bulk_jobs
from services.panel import PanelAPI
from utils.helpers import replace_message
from utils.telegram_ui import smart_edit_message

logger = logging.getLogger(__name__)
router = Router()


class BroadcastFSM(StatesGroup):
    audience_text = State()
    extend_days = State()


def _is_admin(user_id: int) -> bool:
    return user_id in Config.ADMIN_USER_IDS


def _confirm_keyboard(action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"bulk_confirm:{action}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="bulk_cancel"),
        ]
    ])


def _menu_text() -> str:
    return "🛠️ <b>Админ панель</b>\n\nВыберите действие:"


_AUDIENCE_LABELS = {
    "all": "всем пользователям",
    "active": "пользователям с активной подпиской",
    "inactive": "пользователям без активной подписки",
    "with_balance": "пользователям с балансом",
    "referred": "пользователям из реферальной системы",
    "expired": "пользователям с истекшей подпиской",
    "trial_only": "пользователям с trial без покупки",
}


def _job_detail_keyboard(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить статус", callback_data=f"bulk:job:{job_id}")],
            [InlineKeyboardButton(text="📋 Очередь задач", callback_data="bulk:jobs")],
        ]
    )


def _job_status_ru(status: str) -> str:
    return {
        "queued": "в очереди",
        "running": "выполняется",
        "done": "завершена",
        "failed": "ошибка",
    }.get((status or "").lower(), status or "неизвестно")


def _job_detail_text(job) -> str:
    audience_label = _AUDIENCE_LABELS.get(job.audience or "", job.audience or "-")
    lines = [
        "🧵 <b>Статус фоновой задачи</b>",
        "",
        f"ID: <code>{job.job_id}</code>",
        f"Тип: <b>{'Рассылка' if job.kind == 'broadcast' else 'Продление активных'}</b>",
        f"Статус: <b>{_job_status_ru(job.status)}</b>",
        f"Прогресс: <b>{job.processed}/{job.total}</b>",
    ]
    if job.kind == "broadcast":
        lines.append(f"Аудитория: <b>{audience_label}</b>")
        lines.append(f"Доставлено: <b>{job.sent}</b>")
        lines.append(f"Заблокировали бота: <b>{job.blocked}</b>")
        lines.append(f"Ошибки: <b>{job.failed}</b>")
    else:
        lines.append(f"Добавляем дней: <b>{job.add_days}</b>")
        lines.append(f"Успешно продлено: <b>{job.extended}</b>")
        lines.append(f"Ошибки: <b>{job.failed}</b>")
    if job.error:
        lines.extend(["", f"Ошибка: <code>{job.error}</code>"])
    return "\n".join(lines)


@router.message(F.text == "📣 Рассылка всем")
async def broadcast_all_start(message: Message, state: FSMContext, bot: Bot):
    if not _is_admin(message.from_user.id):
        return
    await state.clear()
    await state.set_state(BroadcastFSM.audience_text)
    await state.update_data(audience="all")
    await replace_message(
        message.from_user.id,
        "📣 <b>Рассылка всем пользователям</b>\n\nПришлите текст сообщения. Можно использовать HTML-разметку Telegram.",
        reply_markup=main_menu_keyboard(True),
        delete_user_msg=message,
        bot=bot,
    )


@router.message(F.text == "📣 Рассылка активным")
async def broadcast_active_start(message: Message, state: FSMContext, bot: Bot):
    if not _is_admin(message.from_user.id):
        return
    await state.clear()
    await state.set_state(BroadcastFSM.audience_text)
    await state.update_data(audience="active")
    await replace_message(
        message.from_user.id,
        "📣 <b>Рассылка пользователям с активной подпиской</b>\n\nПришлите текст сообщения. Можно использовать HTML-разметку Telegram.",
        reply_markup=main_menu_keyboard(True),
        delete_user_msg=message,
        bot=bot,
    )


@router.message(F.text == "⏱ Продлить всем активным")
async def extend_active_start(message: Message, state: FSMContext, bot: Bot):
    if not _is_admin(message.from_user.id):
        return
    await state.clear()
    await state.set_state(BroadcastFSM.extend_days)
    await replace_message(
        message.from_user.id,
        "⏱ <b>Массовое продление активных подписок</b>\n\nПришлите количество дней, которое нужно добавить всем активным подпискам.",
        reply_markup=main_menu_keyboard(True),
        delete_user_msg=message,
        bot=bot,
    )


@router.callback_query(F.data == "bulk:prompt:all")
async def broadcast_all_start_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.clear()
    await state.set_state(BroadcastFSM.audience_text)
    await state.update_data(audience="all")
    await smart_edit_message(callback.message, 
        "📣 <b>Рассылка всем пользователям</b>\n\nПришлите текст сообщения. Можно использовать HTML-разметку Telegram.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "bulk:prompt:active")
async def broadcast_active_start_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.clear()
    await state.set_state(BroadcastFSM.audience_text)
    await state.update_data(audience="active")
    await smart_edit_message(callback.message, 
        "📣 <b>Рассылка пользователям с активной подпиской</b>\n\nПришлите текст сообщения. Можно использовать HTML-разметку Telegram.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "bulk:prompt:extend")
async def extend_active_start_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    await state.clear()
    await state.set_state(BroadcastFSM.extend_days)
    await smart_edit_message(callback.message, 
        "⏱ <b>Массовое продление активных подписок</b>\n\nПришлите количество дней, которое нужно добавить всем активным подпискам.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")]]),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bulk:prompt:broadcast:"))
async def broadcast_segment_start_callback(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    audience = callback.data.split(":")[-1]
    audience_label = _AUDIENCE_LABELS.get(audience)
    if not audience_label:
        await callback.answer("Неизвестный сегмент", show_alert=True)
        return
    await state.clear()
    await state.set_state(BroadcastFSM.audience_text)
    await state.update_data(audience=audience)
    await smart_edit_message(
        callback.message,
        f"📣 <b>Рассылка для сегмента</b>\n\nАудитория: <b>{audience_label}</b>\n\nПришлите текст сообщения. Можно использовать HTML-разметку Telegram.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К дашборду", callback_data="adminmenu:bulk")]]),
        parse_mode="HTML",
    )
    await callback.answer()


async def _store_broadcast_preview(message: Message, state: FSMContext, audience: str):
    text = (message.html_text or message.text or "").strip()
    if not text:
        await message.answer("❌ Пустое сообщение отправить нельзя.")
        return
    await state.update_data(broadcast_text=text, audience=audience)
    audience_label = _AUDIENCE_LABELS.get(audience, audience)
    await message.answer(
        f"📨 <b>Предпросмотр рассылки</b>\n\nАудитория: <b>{audience_label}</b>\n\n{text}",
        reply_markup=_confirm_keyboard(f"broadcast:{audience}"),
    )


@router.message(BroadcastFSM.audience_text)
async def broadcast_collect(message: Message, state: FSMContext):
    if not _is_admin(message.from_user.id):
        return
    data = await state.get_data()
    await _store_broadcast_preview(message, state, str(data.get("audience") or "all"))


@router.message(BroadcastFSM.extend_days)
async def extend_active_collect(message: Message, state: FSMContext):
    if not _is_admin(message.from_user.id):
        return
    try:
        add_days = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Нужно прислать целое число дней.")
        return
    if add_days <= 0 or add_days > 3650:
        await message.answer("❌ Укажите число дней от 1 до 3650.")
        return
    await state.update_data(add_days=add_days)
    await message.answer(
        f"⏱ <b>Подтверждение массового продления</b>\n\nБудет добавлено <b>{add_days}</b> дн. всем активным подпискам.",
        reply_markup=_confirm_keyboard("extend_active"),
    )


@router.callback_query(F.data == "bulk_cancel")
async def bulk_cancel(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if not _is_admin(callback.from_user.id):
        await callback.answer()
        return
    await state.clear()
    await callback.answer("Отменено")
    try:
        await callback.message.delete()
    except Exception:
        pass
    await replace_message(callback.from_user.id, _menu_text(), reply_markup=main_menu_keyboard(True), bot=bot)


@router.callback_query(F.data.startswith("bulk_confirm:"))
async def bulk_confirm(
    callback: CallbackQuery,
    state: FSMContext,
    db: Database,
    panel: PanelAPI,
    bot: Bot,
):
    if not _is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return

    action = callback.data.split(":", 1)[1]
    data = await state.get_data()
    await state.clear()

    if action.startswith("broadcast:"):
        audience = action.split(":", 1)[1]
        text = (data.get("broadcast_text") or "").strip()
        if not text:
            await callback.answer("❌ Нет текста рассылки", show_alert=True)
            return
        users = await db.get_users_for_broadcast_segment(audience) if hasattr(db, "get_users_for_broadcast_segment") else await (db.get_all_subscribers() if audience == "active" else db.get_all_users())
        user_ids = [int(u.get("user_id", 0) or 0) for u in users if int(u.get("user_id", 0) or 0) > 0]
        audience_label = audience
        job = await enqueue_broadcast_job(
            bot=bot,
            user_ids=user_ids,
            text=text,
            audience=audience_label,
            initiator_id=callback.from_user.id,
        )
        await callback.answer("Рассылка поставлена в очередь")
        await callback.message.answer(
            "📣 <b>Рассылка поставлена в очередь</b>\n\n"
            f"ID задачи: <code>{job.job_id}</code>\n"
            f"Аудитория: <b>{audience_label}</b>\n"
            f"Получателей: <b>{job.total}</b>",
            parse_mode="HTML",
            reply_markup=_job_detail_keyboard(job.job_id),
        )
        return

    if action == "extend_active":
        add_days = int(data.get("add_days") or 0)
        if add_days <= 0:
            await callback.answer("❌ Нет данных для продления", show_alert=True)
            return
        job = await enqueue_extend_job(
            db=db,
            panel=panel,
            add_days=add_days,
            initiator_id=callback.from_user.id,
        )
        await callback.answer("Продление поставлено в очередь")
        await callback.message.answer(
            "⏱ <b>Продление поставлено в очередь</b>\n\n"
            f"ID задачи: <code>{job.job_id}</code>\n"
            f"Добавить дней: <b>{add_days}</b>",
            parse_mode="HTML",
            reply_markup=_job_detail_keyboard(job.job_id),
        )
        return

    await callback.answer("Неизвестное действие", show_alert=True)


@router.callback_query(F.data == "bulk:jobs")
async def bulk_jobs_list(callback: CallbackQuery):
    if not _is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    jobs = list_bulk_jobs(limit=20)
    if not jobs:
        await smart_edit_message(
            callback.message,
            "🧵 <b>Очередь фоновых задач</b>\n\nСписок пока пуст.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Обновить", callback_data="bulk:jobs")],
                    [InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")],
                ]
            ),
        )
        await callback.answer()
        return

    lines = ["🧵 <b>Очередь фоновых задач</b>", "", "Последние задачи:"]
    keyboard_rows = []
    for job in jobs:
        if job.kind == "broadcast":
            audience_label = _AUDIENCE_LABELS.get(job.audience or "", job.audience or "-")
            lines.append(
                f"\n• <code>{job.job_id}</code> — 📣 {audience_label} — <b>{_job_status_ru(job.status)}</b> "
                f"({job.processed}/{job.total})"
            )
        else:
            lines.append(
                f"\n• <code>{job.job_id}</code> — ⏱ +{job.add_days} дн. — <b>{_job_status_ru(job.status)}</b> "
                f"({job.processed}/{job.total})"
            )
        keyboard_rows.append([InlineKeyboardButton(text=f"🔎 {job.job_id}", callback_data=f"bulk:job:{job.job_id}")])
    keyboard_rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="bulk:jobs")])
    keyboard_rows.append([InlineKeyboardButton(text="⬅️ К дашборду", callback_data="admin_dashboard")])
    await smart_edit_message(
        callback.message,
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_rows[:12]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bulk:job:"))
async def bulk_job_detail(callback: CallbackQuery):
    if not _is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    job_id = callback.data.split(":", 2)[2]
    job = get_bulk_job(job_id)
    if not job:
        await callback.answer("Задача не найдена", show_alert=True)
        return
    await smart_edit_message(
        callback.message,
        _job_detail_text(job),
        parse_mode="HTML",
        reply_markup=_job_detail_keyboard(job_id),
    )
    await callback.answer()
