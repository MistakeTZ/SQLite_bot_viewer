import asyncio
from datetime import datetime
from sqlalchemy import select, update

from database.model import User, Repetition
from database.storage import clear_databases
import handlers # noqa F401
from tasks import kb
from tasks.config import tz
from tasks.loader import bot, session


# Отправка запланированных сообщений
async def send_messages():
    await asyncio.sleep(5)

    while True:
        now = datetime.now(tz=tz)

        stmt = (
            select(Repetition)
            .where(Repetition.confirmed.is_(True))
            .where(Repetition.is_send.is_(False))
            .where(Repetition.time_to_send < now)
        )
        result = session.execute(stmt)
        messages_to_send = result.scalars().all()

        if messages_to_send:
            to_send_tasks = [
                send_msg(session, msg) for msg in messages_to_send
            ]
            await asyncio.gather(*to_send_tasks)

        clear_databases(now)

        await asyncio.sleep(60)


async def send_msg(session, message: Repetition):
    # mark as sent
    session.execute(
        update(Repetition)
        .where(Repetition.chat_id == message.chat_id)
        .where(Repetition.message_id == message.message_id)
        .values(is_send=True),
    )
    session.commit()

    # fetch all users
    result = session.execute(select(User))
    all_users = result.scalars().all()

    # build reply
    if message.button_text and message.button_link:
        reply = kb.link(message.button_text, message.button_link)
    else:
        reply = None

    for user in all_users:
        try:
            await bot.copy_message(
                user.telegram_id,
                message.chat_id,
                message.message_id,
                reply_markup=reply,
            )
        except Exception:
            continue
