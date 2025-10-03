import os

from aiogram import F
from aiogram.types.callback_query import CallbackQuery
from aiogram.types import BufferedInputFile
from aiogram.fsm.context import FSMContext

from tasks import kb
from tasks.loader import dp, sender, bot
from database.storage import databases


# Возвращение в меню
@dp.callback_query(F.data == "back")
async def menu_handler(clbck: CallbackQuery, state: FSMContext) -> None:
    user_id = clbck.from_user.id

    database = databases.get(user_id)
    if not database:
        await clbck.answer(sender.text("no_database"))
        return

    database.last_query = None

    await clbck.message.edit_text(
        str(database),
        reply_markup=kb.table(2, *database.get_buttons()),
    )


# Выбор таблицы
@dp.callback_query(F.data.startswith("table_"))
async def table_handler(clbck: CallbackQuery, state: FSMContext) -> None:
    user_id = clbck.from_user.id
    table = int(clbck.data.split("_")[-1])

    database = databases.get(user_id)
    if not database:
        await clbck.answer(sender.text("no_database"))
        return

    table_name = database.tables[table]

    await sender.edit_message(
        clbck.message,
        "table",
        kb.table(2, *database.get_buttons()),
        table_name,
        database.get_table(table_name)[:4000],
    )


# Скачивание базы данных
@dp.callback_query(F.data.startswith("get_"))
async def get_handler(clbck: CallbackQuery, state: FSMContext) -> None:
    user_id = clbck.from_user.id
    db_format = clbck.data.split("_")[-1]

    database = databases.get(user_id)
    if not database:
        await clbck.answer(sender.text("no_database"))
        return

    if db_format == "sqlite":
        buffer = database.get_sqlite()
        await bot.send_document(user_id, BufferedInputFile(
            file=buffer, filename=database.name + ".sqlite3",
        ))

    elif db_format == "excel":
        file_path = database.get_excel()
        await sender.send_media(
            user_id,
            "document",
            file_path,
            path="temp",
            name=database.name,
        )
        os.remove(os.path.join("temp", file_path))
