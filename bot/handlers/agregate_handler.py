import json
from aiogram import Router, types

from motor.core import AgnosticDatabase as MDB

from core.validators import validate_data
from core.utils import agregate_salaries


agreagte_router = Router()


@agreagte_router.message()
async def agregate_salary(message: types.Message, collection: MDB):
    """Обрабатывает запрос на агрегацию данных о зарплатах."""
    validation_result = validate_data(message.text)

    if validation_result:
        return await message.answer(validation_result)

    data = await agregate_salaries(collection, message.text)
    data_json = json.dumps(data)
    await message.answer(data_json)
