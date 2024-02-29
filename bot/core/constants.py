VALID_DATA_DICT = {"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}
NOT_VALID = (
    'Невалидный запос. Пример запроса: '
    '{"dt_from": "2022-09-01T00:00:00", '
    '"dt_upto": "2022-12-31T23:59:00", "group_type": "month"}')

VALID_DATA = (
    'Допустимо отправлять только следующие запросы:'
    '{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'
    '{"dt_from": "2022-10-01T00:00:00", "dt_upto": "2022-11-30T23:59:00", "group_type": "day"}'
    '{"dt_from": "2022-02-01T00:00:00", "dt_upto": "2022-02-02T00:00:00", "group_type": "hour"}'
)
VALID_GROUP_TYPE = ["month", "day", "hour"]
