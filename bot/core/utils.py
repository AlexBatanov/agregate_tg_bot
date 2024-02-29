from datetime import datetime, timedelta
import json


def get_agregation_parameters(text):
    data = json.loads(text)
    dt_from = datetime.fromisoformat(data['dt_from'])
    dt_upto = datetime.fromisoformat(data['dt_upto'])
    group_type = data['group_type']

    return group_type, dt_from, dt_upto


async def agregate_salaries(collection, text):
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


def create_pipeline(dt_from, dt_upto, group_type):
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


def add_missing_ones(dataset, labels, dt_from, dt_upto, group_type):

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


def increment_date(date, increment, group_type):
        if group_type == 'day':
            return date + timedelta(days=increment)
        elif group_type == 'hour':
            return date + timedelta(hours=increment)
