from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.callback_answer import CallbackAnswerMiddleware

from handlers import admin, admin_broadcast, admin_health, buy, fallback, inline, payment_admin, payment_diagnostics, profile, referral, start, support_chat
from middlewares.ban import ban_middleware
from middlewares.callback_dedup import callback_dedup_middleware
from middlewares.error_guard import error_guard_middleware
from middlewares.rate_limit import rate_limit_middleware
from middlewares.request_context import request_context_middleware
from middlewares.start_dedup import start_dedup_middleware


def build_dispatcher(*, bot: Bot, db, panel, payment_gateway) -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(error_guard_middleware)
    dp.callback_query.middleware(error_guard_middleware)
    dp.message.middleware(request_context_middleware)
    dp.callback_query.middleware(request_context_middleware)
    dp.message.middleware(rate_limit_middleware)
    dp.callback_query.middleware(rate_limit_middleware)
    dp.callback_query.middleware(callback_dedup_middleware)
    dp.message.middleware(start_dedup_middleware)
    dp.message.middleware(ban_middleware)
    dp.callback_query.middleware(ban_middleware)
    dp.callback_query.middleware(CallbackAnswerMiddleware())

    dp.include_router(start.router)
    dp.include_router(profile.router)
    dp.include_router(buy.router)
    dp.include_router(payment_admin.router)
    dp.include_router(payment_diagnostics.router)
    dp.include_router(referral.router)
    dp.include_router(admin.router)
    dp.include_router(admin_broadcast.router)
    dp.include_router(support_chat.router)
    dp.include_router(inline.router)
    dp.include_router(admin_health.router)
    dp.include_router(fallback.router)

    dp["db"] = db
    dp["panel"] = panel
    dp["payment_gateway"] = payment_gateway
    dp["bot"] = bot
    return dp
