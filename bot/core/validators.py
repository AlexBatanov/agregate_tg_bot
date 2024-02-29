from datetime import datetime
import json

from .constants import NOT_VALID, VALID_DATA_DICT, VALID_DATA, VALID_GROUP_TYPE


def validate_data(message: str) -> str:
    """Проверяет валидность данных в сообщении"""
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return NOT_VALID

    if data.keys() != VALID_DATA_DICT.keys():
        return VALID_DATA
    
    try:
        datetime.fromisoformat(data['dt_from'])
        datetime.fromisoformat(data['dt_upto'])
    except ValueError:
        return VALID_DATA
    
    if data['group_type'] not in VALID_GROUP_TYPE:
        return VALID_DATA
