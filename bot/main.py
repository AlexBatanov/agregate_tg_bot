import sys
import asyncio
import logging
from os import getenv

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, enums
from motor.motor_asyncio import AsyncIOMotorClient

from handlers.start_handler import start_router

load_dotenv()
TOKEN = getenv('TOKEN')


async def main():
    client = AsyncIOMotorClient('localhost', 27017)
    db = client['test']['sample_collection']

    dp = Dispatcher()
    bot = Bot(TOKEN, parse_mode=enums.ParseMode.HTML)
    dp.include_router(start_router)
    await dp.start_polling(bot, db=db)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Bot exit')
