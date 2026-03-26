import pytest
from aiogram.exceptions import TelegramForbiddenError

from services.admin_bulk import extend_active_subscriptions_bulk, send_broadcast


class DummyBot:
    def __init__(self, blocked=None):
        self.blocked = set(blocked or [])
        self.sent = []

    async def send_message(self, user_id, text, parse_mode=None):
        if user_id in self.blocked:
            raise TelegramForbiddenError(method="sendMessage", message="forbidden")
        self.sent.append((user_id, text, parse_mode))
        return True


class DummyPanel:
    def __init__(self, failed=None):
        self.failed = set(failed or [])
        self.calls = []

    async def extend_client_expiry(self, base_email, add_days):
        self.calls.append((base_email, add_days))
        return base_email not in self.failed


class DummyDb:
    async def get_all_subscribers(self):
        return [
            {"user_id": 101},
            {"user_id": 102},
            {"user_id": 103},
        ]

    def __init__(self):
        self.reset_calls = []

    async def reset_expiry_notifications(self, user_id):
        self.reset_calls.append(user_id)
        return True


@pytest.mark.asyncio
async def test_send_broadcast_counts_blocked_users():
    bot = DummyBot(blocked={2})
    result = await send_broadcast(bot=bot, user_ids=[1, 2, 3], text="<b>Hello</b>", delay_sec=0)

    assert result.total == 3
    assert result.sent == 2
    assert result.failed == 1
    assert result.blocked == 1
    assert [item[0] for item in bot.sent] == [1, 3]


@pytest.mark.asyncio
async def test_extend_active_subscriptions_bulk_counts_results(monkeypatch):
    from config import Config

    monkeypatch.setattr(Config, "PANEL_EMAIL_DOMAIN", "example", raising=False)
    db = DummyDb()
    panel = DummyPanel(failed={"user_102@example"})

    result = await extend_active_subscriptions_bulk(db=db, panel=panel, add_days=7)

    assert result.total == 3
    assert result.extended == 2
    assert result.failed == 1
    assert db.reset_calls == [101, 103]
    assert panel.calls[0] == ("user_101@example", 7)


@pytest.mark.asyncio
async def test_extend_active_subscriptions_bulk_rejects_non_positive_days():
    db = DummyDb()
    panel = DummyPanel()

    result = await extend_active_subscriptions_bulk(db=db, panel=panel, add_days=0)

    assert result.total == 3
    assert result.extended == 0
    assert result.failed == 3
    assert panel.calls == []
