import os

from aiogram.filters import Filter
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from tasks.loader import dp, bot, sender
from tasks.states import UserState
from tasks import kb
from sqlite3 import connect
from database.storage import databases, Database


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


# Проверка на отсутствие состояний
class NoStates(Filter):
    async def __call__(self, msg: Message, state: FSMContext):
        stat = await state.get_state()
        return stat is None


# Сообщение без состояний
@dp.message(NoStates())
async def no_states_handler(msg: Message, state: FSMContext):
    pass
