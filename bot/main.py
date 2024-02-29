import sys
import asyncio
import logging
from os import getenv

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, enums
from motor.motor_asyncio import AsyncIOMotorClient

from handlers.start_handler import start_router
from handlers.agregate_handler import agreagte_router


load_dotenv()


async def main():
    client = AsyncIOMotorClient(getenv('HOST_DB'), getenv('PORT_DB'))
    db = client['test']
    collection = db['sample_collection']

    dp = Dispatcher()
    bot = Bot(getenv('TOKEN'), parse_mode=enums.ParseMode.HTML)

    dp.include_router(start_router)
    dp.include_router(agreagte_router)

    await dp.start_polling(bot, collection=collection)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Bot exit')
