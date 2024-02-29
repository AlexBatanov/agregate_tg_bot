from aiogram import Router, types

from motor.core import AgnosticDatabase as MDB

from core.validators import validate_data


agreagte_router = Router()


@agreagte_router.message()
async def agregate_salary(message: types.Message, db: MDB):
    validation_result = validate_data(message.text)
    if validation_result:
        await message.answer(validation_result)