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

    dataset = []
    labels = []

    async for doc in result:
        date = datetime(
            year=doc['_id'].get('year'),
            month=doc['_id'].get('month', 1),
            day=doc['_id'].get('day', 1),
            hour=doc['_id'].get('hour', 0)
        )
        labels.append(date.isoformat())
        dataset.append(doc['total_value'])
    
    labels, dataset = add_missing_ones(dataset, labels, dt_from, dt_upto, group_type)

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
    dataset: List, labels: List, 
    dt_from: datetime, dt_upto: datetime, group_type: str
) -> Tuple[List[str], List[float]]:
    """
    Добавляет недостающие данные (нулевые значения) в набор данных
    и метки времени для соответствия заданному диапазону и группировке.

    Args:
        dataset (list): Список значений данных.
        labels (list): Список меток времени соответствующих значениям данных.
        dt_from (datetime): Начальная дата и время диапазона данных.
        dt_upto (datetime): Конечная дата и время диапазона данных.
        group_type (str): Тип группировки данных ('day', 'month', 'hour').

    Returns:
        tuple: Кортеж с обновленными метками времени
            и данными после добавления недостающих значений.
    """

    added_dataset = []
    added_labels = []
    
    last_date = datetime.fromisoformat(labels[-1])
    
    if group_type == 'day':
        last_date = last_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        dt_upto = dt_upto.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    if group_type == 'month':
        return labels, dataset

    while dt_from < datetime.fromisoformat(labels[0]):
        added_labels.append(dt_from.isoformat())
        added_dataset.append(0)
        dt_from = increment_date(dt_from, 1, group_type)

    labels = added_labels + labels
    dataset = added_dataset + dataset


    while dt_upto > last_date:
        last_date = increment_date(last_date, 1, group_type)
        dataset.append(0)
        labels.append(last_date.isoformat())

    return labels, dataset


def increment_date(
    date: datetime, increment: int, group_type: str
) -> datetime:
    """
    Увеличивает дату на указанное количество единиц
    в зависимости от типа группировки.

    Args:
        date (datetime): Исходная дата.
        increment (int): Количество единиц для увеличения даты.
        group_type (str): Тип группировки данных ('day', 'hour' и т.д.).

    Returns:
        datetime: Новая дата после увеличения на указанное количество единиц.
    """
    if group_type == 'day':
        return date + timedelta(days=increment)
    elif group_type == 'hour':
        return date + timedelta(hours=increment)
