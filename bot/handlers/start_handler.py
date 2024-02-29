from aiogram import Router, types
from aiogram.filters import CommandStart


start_router = Router()


@start_router.message(CommandStart())
async def on_startup(message: types.Message):
    user_id = message.from_user.id
    name = message.from_user.first_name
    
    text = f'Hi <b><a href="tg://user?id={user_id}">{name}!</a></b>'
    await message.answer(text, parse_mode='html')
