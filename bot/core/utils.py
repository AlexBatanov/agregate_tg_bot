from typing import List, Dict, Tuple
from datetime import datetime, timedelta
import json

from motor.core import AgnosticDatabase as MDB


def get_agregation_parameters(text: str)  -> Tuple[str, datetime, datetime]:
    """
    Извлекает параметры агрегации из JSON-строки
    и возвращает их в виде кортежа.

    Args:
        text (str): JSON-строка с параметрами агрегации.

    Returns:
        tuple: Кортеж, содержащий тип группировки,
        начальную и конечную даты и время.
    """
    data = json.loads(text)
    dt_from = datetime.fromisoformat(data['dt_from'])
    dt_upto = datetime.fromisoformat(data['dt_upto'])
    group_type = data['group_type']

    return group_type, dt_from, dt_upto


async def agregate_salaries(collection: MDB, text: str) -> Dict:
    """
    Асинхронно агрегирует данные о зарплатах
    в соответствии с параметрами из JSON-строки.

    Args:
        collection (MDB): Коллекция MongoDB, содержащая данные о зарплатах.
        text (str): JSON-строка с параметрами агрегации.

    Returns:
        dict: Словарь с данными: "dataset" - список значений зарплат,
              "labels" - список дат и времени
              соответствующих значениям зарплат.
    """
    group_type, dt_from, dt_upto = get_agregation_parameters(text)

    pipeline = create_pipeline(dt_from, dt_upto, group_type)
    result = collection.aggregate(pipeline)

    data = {}

    async for doc in result:
        date = datetime(
            year=doc['_id'].get('year'),
            month=doc['_id'].get('month', 1),
            day=doc['_id'].get('day', 1),
            hour=doc['_id'].get('hour', 0)
        )
        data[date.isoformat()] = doc['total_value']
    
    labels, dataset = add_missing_ones(data, dt_from, dt_upto, group_type)

    return {"dataset": dataset, "labels": labels}


def create_pipeline(
    dt_from: datetime, dt_upto: datetime, group_type: str
) -> List[dict]:
    """
    Создает и возвращает конвейер MongoDB для агрегации данных
    по заданным временным интервалам.

    Args:
        dt_from (datetime): Начальная дата и время для фильтрации.
        dt_upto (datetime): Конечная дата и время для фильтрации.
        group_type (str): Тип группировки данных (
            'day' для дневной группировки, 'month' для месячной
    ).

    Returns:
        list: Список этапов конвейера MongoDB для агрегации данных.
    """
    pipeline = [
        {
            '$match': {
                'dt': {'$gte': dt_from, '$lte': dt_upto}
            }
        },
        {
            '$group': {
                '_id': {
                    'year': {'$year': '$dt'},
                    'month': {'$month': '$dt'},
                    'day': {'$dayOfMonth': '$dt'},
                    'hour': {'$hour': '$dt'}
                },
                'total_value': {'$sum': '$value'},
                'dt': { '$first': '$dt' }
            }
        },
        {
            '$sort': { 'dt': 1 }
        }
    ]

    if group_type == 'day':
        del pipeline[1]['$group']['_id']['hour']
    elif group_type == 'month':
        del pipeline[1]['$group']['_id']['day']
        del pipeline[1]['$group']['_id']['hour']

    return pipeline


def add_missing_ones(
    data: Dict, 
    dt_from: datetime, dt_upto: datetime, group_type: str
) -> Tuple[List[str], List[float]]:
    """
    Добавляет недостающие данные (нулевые значения) в набор данных
    и метки времени для соответствия заданному диапазону и группировке.

    Args:
        data (dict): Словарь дата: зарплата.
        dt_from (datetime): Начальная дата и время диапазона данных.
        dt_upto (datetime): Конечная дата и время диапазона данных.
        group_type (str): Тип группировки данных ('day', 'month', 'hour').

    Returns:
        tuple: Кортеж с обновленными метками времени
            и данными после добавления недостающих значений.
    """

    time_periods = generate_time_periods(dt_from, dt_upto, group_type)
    
    for key, value in data.items():
        time_periods[key] = value

    return list(time_periods.keys()), list(time_periods.values())


def generate_time_periods(start_date, end_date, interval):
    time_periods = {}
    current_date = start_date

    while current_date <= end_date:
        if interval == 'day':
            time_period_key = current_date.strftime('%Y-%m-%dT%H:%M:%S')
        elif interval == 'month':
            time_period_key = current_date.strftime('%Y-%m-01T00:00:00')
        elif interval == 'hour':
            time_period_key = current_date.strftime('%Y-%m-%dT%H:00:00')

        time_periods[time_period_key] = 0
        if interval == 'day':
            current_date += timedelta(days=1)
        elif interval == 'month':
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        elif interval == 'hour':
            current_date += timedelta(hours=1)

    return time_periods
