from aiogram import F
from aiogram.types.callback_query import CallbackQuery
from aiogram.fsm.context import FSMContext
from tasks.loader import dp, sender

from tasks.states import UserState
from tasks import kb
from database.storage import databases


# Возвращение в меню
@dp.callback_query(F.data == "back")
async def menu_handler(clbck: CallbackQuery, state: FSMContext) -> None:
    await sender.edit_message(clbck.message, "menu")
    await state.set_state(UserState.default)


# Начинается с
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
        database.get_table(table_name)[:4000]
    )
