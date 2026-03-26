from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.types import Message

from keyboards import back_keyboard

router = Router()

KNOWN_COMMANDS = {
    "start",
    "menu",
    "admin",
    "admindash",
    "paydiag",
    "payactions",
    "payops",
    "payattention",
    "payresolve",
    "periodreport",
    "dailyreport",
}


def _extract_command_name(text: str) -> str:
    raw = (text or "").strip()
    if not raw.startswith("/"):
        return ""
    token = raw.split(maxsplit=1)[0][1:]
    return token.split("@", 1)[0].lower()


@router.message(StateFilter(None), F.text.startswith("/"))
async def unknown_command(message: Message):
    if _extract_command_name(message.text or "") in KNOWN_COMMANDS:
        return
    await message.answer(
        "❌ Неизвестная команда.\n\nНажмите кнопку ниже, чтобы перейти в главное меню.",
        reply_markup=back_keyboard(),
    )


@router.message(StateFilter(None), F.text)
async def unknown_message(message: Message):
    await message.answer(
        "❌ Неизвестная команда.\n\nНажмите кнопку ниже, чтобы перейти в главное меню.",
        reply_markup=back_keyboard(),
    )
