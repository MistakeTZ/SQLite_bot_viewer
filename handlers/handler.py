import os
from sqlite3 import connect

from aiogram import F
from aiogram.filters import Filter
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from database.storage import databases, Database
from tasks import kb
from tasks.loader import dp, bot, sender
from tasks.states import UserState


class DatabaseFilter(Filter):
    async def __call__(self, msg: Message, state: FSMContext):
        if not msg.document:
            return False
        return msg.document.file_name.split(".")[-1] in ["sqlite3", "db"]


@dp.message(DatabaseFilter())
async def send_database(msg: Message, state: FSMContext):
    document = msg.document.file_id
    user_id = msg.from_user.id

    try:
        file_path = os.path.join("temp", f"tmp{document}.db")
        await bot.download(document, file_path)

        # создаём БД в памяти
        mem_db = connect(":memory:")

        src_db = connect(file_path)
        src_db.backup(mem_db)
        src_db.close()

        # удаляем временный файл
        os.remove(file_path)

        previous = databases.get(user_id)
        if previous:
            previous.unload()

        databases[user_id] = Database(mem_db, msg.document.file_name)
    except Exception as e:
        sender.message(user_id, "open_error", None, str(e))
        return

    await bot.send_message(
        user_id,
        str(databases[user_id]),
        reply_markup=kb.table(2, *databases[user_id].get_buttons()),
    )

    await state.set_state(UserState.db)


# Обращение к базе
@dp.message(UserState.db, F.text)
@dp.edited_message(UserState.db)
async def db_handler(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    database = databases.get(user_id)
    if not database:
        await sender.message(user_id, "no_database")
        return

    text = msg.text
    try:
        if text.lower().startswith("select"):
            await sender.message(
                user_id,
                "query_info",
                kb.table(2, *database.get_buttons()),
                database.tabulate_result(*database.get_query(text))[:4000],
            )
        else:
            database.execute_query(text)
            await bot.send_message(
                user_id,
                sender.text("query_success"),
                reply_markup=kb.table(2, *databases[user_id].get_buttons()),
            )
    except Exception as e:
        await sender.message(user_id, "query_error", None, str(e))
        return
