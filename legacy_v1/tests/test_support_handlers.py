import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class SupportHandlersTests(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_message_replies_with_fallback_text(self):
        from handlers.fallback import unknown_message

        message = SimpleNamespace(answer=AsyncMock())
        await unknown_message(message)

        message.answer.assert_awaited_once()
        args, kwargs = message.answer.await_args
        self.assertIn("Неизвестная команда", args[0])
        self.assertIn("reply_markup", kwargs)

    async def test_support_history_empty_shows_empty_state(self):
        from handlers.support_chat import support_history

        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            message=SimpleNamespace(),
            answer=AsyncMock(),
        )
        db = SimpleNamespace(list_user_support_tickets=AsyncMock(return_value=[]))

        with patch("handlers.support_chat.smart_edit_message", new=AsyncMock()) as smart_edit:
            await support_history(callback, db=db)

        smart_edit.assert_awaited_once()
        args, kwargs = smart_edit.await_args
        self.assertIn("История обращений", args[1])
        self.assertIn("нет обращений", args[1])
        self.assertEqual(kwargs.get("parse_mode"), "HTML")
        callback.answer.assert_awaited_once()

    async def test_support_user_close_deletes_message_in_chat(self):
        from handlers.support_chat import support_user_close

        callback_message = SimpleNamespace(
            delete=AsyncMock(),
            edit_reply_markup=AsyncMock(),
        )
        callback = SimpleNamespace(
            data="support:user_close:77",
            message=callback_message,
            from_user=SimpleNamespace(id=555),
            answer=AsyncMock(),
        )
        db = SimpleNamespace(close_support_ticket=AsyncMock(return_value=True))
        bot = SimpleNamespace(send_message=AsyncMock())

        with patch("handlers.support_chat.Config.ADMIN_USER_IDS", [9001]):
            await support_user_close(callback, db=db, bot=bot)

        db.close_support_ticket.assert_awaited_once_with(77)
        callback_message.delete.assert_awaited_once()
        callback_message.edit_reply_markup.assert_not_awaited()
        bot.send_message.assert_awaited_once()
        callback.answer.assert_awaited_once_with("Вопрос закрыт")


if __name__ == "__main__":
    unittest.main()
