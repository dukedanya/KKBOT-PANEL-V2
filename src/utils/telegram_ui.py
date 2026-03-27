from aiogram.exceptions import TelegramBadRequest


async def smart_edit_message(message, text: str, reply_markup=None, **kwargs):
    try:
        if getattr(message, 'text', None):
            return await message.edit_text(text, reply_markup=reply_markup, **kwargs)
        if any(getattr(message, attr, None) for attr in ('photo', 'video', 'animation', 'document')):
            return await message.edit_caption(caption=text, reply_markup=reply_markup, **kwargs)
        return await message.answer(text, reply_markup=reply_markup, **kwargs)
    except TelegramBadRequest as e:
        err = str(e).lower()
        if 'media_caption_too_long' in err or 'caption is too long' in err:
            return await message.answer(text, reply_markup=reply_markup, **kwargs)
        if 'there is no text in the message to edit' in err:
            try:
                return await message.edit_caption(caption=text, reply_markup=reply_markup, **kwargs)
            except Exception:
                return await message.answer(text, reply_markup=reply_markup, **kwargs)
        if 'message is not modified' in err:
            return None
        if "message can't be edited" in err or 'message to edit not found' in err:
            return await message.answer(text, reply_markup=reply_markup, **kwargs)
        raise


async def smart_edit_by_bot(bot, chat_id: int, message_id: int, text: str, reply_markup=None, **kwargs):
    try:
        return await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=reply_markup, **kwargs)
    except TelegramBadRequest as e:
        err = str(e).lower()
        if 'media_caption_too_long' in err or 'caption is too long' in err:
            return await bot.send_message(chat_id, text, reply_markup=reply_markup, **kwargs)
        if 'there is no text in the message to edit' in err:
            try:
                return await bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=text, reply_markup=reply_markup, **kwargs)
            except Exception:
                return await bot.send_message(chat_id, text, reply_markup=reply_markup, **kwargs)
        if 'message is not modified' in err:
            return None
        if "message can't be edited" in err or 'message to edit not found' in err:
            return await bot.send_message(chat_id, text, reply_markup=reply_markup, **kwargs)
        raise


async def smart_edit_by_ids(bot, chat_id: int, message_id: int, text: str, reply_markup=None, **kwargs):
    return await smart_edit_by_bot(
        bot,
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        reply_markup=reply_markup,
        **kwargs,
    )
