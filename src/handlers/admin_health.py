from aiogram import F, Router
from aiogram.types import Message

from config import Config
from db import Database
from services.health import collect_health_snapshot, format_health_text

router = Router()


def is_admin(user_id: int) -> bool:
    return user_id in Config.ADMIN_USER_IDS


@router.message(F.text == "🩺 Health")
@router.message(F.text == "/health")
async def health_command(message: Message, db: Database, panel, payment_gateway):
    if not is_admin(message.from_user.id):
        return
    snapshot = await collect_health_snapshot(db, panel, payment_gateway)
    await message.answer(await format_health_text(snapshot))


@router.message(F.text == "/dbstatus")
async def dbstatus_command(message: Message, db: Database):
    if not is_admin(message.from_user.id):
        return
    version = await db.get_schema_version()
    applied = await db.get_applied_migration_versions()
    latest = max(applied) if applied else version
    status = "OK" if version >= latest else "OUTDATED"
    await message.answer(
        "🗄 <b>DB status</b>\n\n"
        f"Current schema version: <code>{version}</code>\n"
        f"Latest applied migration: <code>{latest}</code>\n"
        f"Status: <b>{status}</b>"
    )
