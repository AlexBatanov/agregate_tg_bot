import json

from .constants import NOT_VALID, VALID_DATA_LIST, VALID_DATA


def validate_data(message):
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return NOT_VALID
    
    if data not in VALID_DATA_LIST:
        return VALID_DATA