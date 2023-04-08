from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
import datetime

client = MongoClient()

db = client["sample_collection"]
msg_collection = db["sample_collection"]


def correct_time(dt_from, dt_upto):
    time_format = '%Y-%m-%dT%H:%M:%S'

    dt_from = datetime.datetime.strptime(dt_from, time_format)
    dt_upto = datetime.datetime.strptime(dt_upto, time_format)

    return dt_from, dt_upto


def get_result_month(dt_from, dt_upto):
    result_month = msg_collection.aggregate([
    {
    '$match': {
        'dt': {'$gte': dt_from, '$lte': dt_upto}
    }
    },
    {
        '$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%S',
                    'date': {
                        '$dateFromParts': {
                            'year': {'$year': '$dt'},
                            'month': {'$month': '$dt'}
                        }
                    }
                }
            },
            'total_value': {'$sum': '$value'}
        }
    },
    {
        '$sort': {'_id': 1}
    }
])
    return result_month

def get_result_day(dt_from, dt_upto):
    result_day = msg_collection.aggregate([
    {
        '$match': {
            'dt': {'$gte': dt_from, '$lte': dt_upto}
        }
    },
    {
        '$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%S',
                    'date': {
                        '$dateFromParts': {
                            'year': {'$year': '$dt'},
                            'month': {'$month': '$dt'},
                            'day': {'$dayOfMonth': '$dt'}
                        }
                    }
                }
            },
            'total_value': {'$sum': '$value'}
        }
    },
    {
        '$sort': {'_id': 1}
    }
])
    return result_day

def get_result_hour(dt_from, dt_upto):
    result_hour = msg_collection.aggregate([
    {
        '$match': {
            'dt': {'$gte': dt_from, '$lte': dt_upto}
        }
    },
    {
        '$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%S',
                    'date': {
                        '$dateFromParts': {
                            'year': {'$year': '$dt'},
                            'month': {'$month': '$dt'},
                            'day': {'$dayOfMonth': '$dt'},
                            'hour': {'$hour': '$dt'}
                        }
                    }
                }
            },
            'total_value': {'$sum': '$value'}
        }
    },
    {
        '$sort': {'_id': 1}
    }
])
    return result_hour


def difference_date(dt_from, dt_upto, group_type):
    if group_type == 'month':
        result = get_result_month(dt_from, dt_upto)
        delta_days = relativedelta(months=1)
    elif group_type == 'day':
        result = get_result_day(dt_from, dt_upto)
        delta_days = datetime.timedelta(days=1)
    elif group_type == 'hour':
        result = get_result_hour(dt_from, dt_upto)
        delta_days = datetime.timedelta(hours=1)

    correct_date = []
    my_data = []

    while dt_from <= dt_upto:
        correct_date.append(dt_from.isoformat())
        dt_from += delta_days

    for item in result:
        my_data.append(item['_id'])

    difference_1 = set(correct_date).difference(set(my_data))
    difference_2 = set(my_data).difference(set(correct_date))
    data_difference = list(difference_1.union(difference_2))

    return data_difference


def dataset(dt_from, dt_upto, group_type):

    if group_type == 'month':
        result = get_result_month(dt_from, dt_upto)
        missing_element = difference_date(dt_from, dt_upto, group_type)
    elif group_type == 'day':
        result = get_result_day(dt_from, dt_upto)
        missing_element = difference_date(dt_from, dt_upto, group_type)
    elif group_type == 'hour':
        result = get_result_hour(dt_from, dt_upto)
        missing_element = difference_date(dt_from, dt_upto, group_type)


    values = []
    dates = []

    for item in result:
        values.append(item['total_value'])
        dates.append(item['_id'])

    if missing_element:
        for data in missing_element:
            values.append(0)
            dates.append(data)

    finaly_data = list(zip(values, dates))
    sorted_data = sorted(finaly_data, key=lambda x: x[1])

    sorted_values = [x[0] for x in sorted_data]
    sorted_date = [x[1] for x in sorted_data]

    dataset_dict = {
        "dataset": sorted_values,
        "labels": sorted_date
    }
    return dataset_dict
