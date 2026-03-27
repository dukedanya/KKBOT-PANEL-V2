from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

from config import Config
from keyboards import main_menu_keyboard, admin_menu_keyboard
from utils.helpers import replace_message

router = Router()


class TariffEditFSM(StatesGroup):
    value = State()


class StarsSettingsFSM(StatesGroup):
    multiplier = State()


class ReferralSettingsFSM(StatesGroup):
    field = State()


class PartnerSettingsFSM(StatesGroup):
    rates = State()
    balance = State()

class MainMessageFSM(StatesGroup):
    content = State()


class TemplateEditFSM(StatesGroup):
    content = State()
    confirm = State()


class PromoCodeFSM(StatesGroup):
    content = State()
    edit_value = State()


def is_admin(user_id: int) -> bool:
    return user_id in Config.ADMIN_USER_IDS


@router.message(F.text == "🛠️ Админ меню")
@router.message(Command("admin"))
async def admin_menu(message: Message, bot: Bot):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await replace_message(
            user_id,
            "⛔ У вас нет прав администратора.",
            reply_markup=main_menu_keyboard(False),
            delete_user_msg=message,
            bot=bot,
        )
        return
    await replace_message(
        user_id,
        (
            "🛠️ <b>Админ панель</b>\n\n"
            "Разделы собраны по задачам:\n"
            "• 🧭 Сводка\n"
            "• 👥 Пользователи\n"
            "• 💳 Платежи\n"
            "• 📈 Аналитика\n"
            "• 📝 Контент и продажи\n"
            "• ⚙️ Система и панель"
        ),
        reply_markup=admin_menu_keyboard(),
        delete_user_msg=message,
        bot=bot,
    )
