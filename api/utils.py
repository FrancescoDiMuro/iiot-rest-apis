import datetime
import os

from typing import List
from re import search

import sys

WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    sys.path.append(WORKING_DIR)

import api.dto


API_METADATA: dict = {
    'title': 'IIoT REST APIs',
    'summary': 'Industrial Internet of Things REST API for gathering, storing and analysing data from IIoT devices.',
    'description': '''This APIs lets you configure tags to store data of at different time intervals, get their values with the possibility
        to apply filters, and generate trends based on the given criterias.'''
}

ROOT_ENDPOINT_METADATA: dict = {
    'summary': 'Root endpoint', 
    'description': 'This is the endpoint root of the REST API server.', 
    'response_model': None,
    'tags': ['root']
}

GET_TAGS_ENDPOINT_METADATA: dict = {
    'summary': 'GET Tags', 
    'description': 'This endpoint lets you interact with the configured tags in the REST API server.', 
    'response_model': List[api.dto.Tags],
    'tags': ['tags']
}

POST_TAGS_ENDPOINT_METADATA: dict = {
    'summary': 'POST Tags', 
    'description': 'This endpoint lets you insert tags in the database.', 
    'response_model': List[api.dto.Tags],
    'tags': ['tags']
}

GET_DATA_ENDPOINT_METADATA: dict = {
    'summary': 'GET Data', 
    'description': 'This endpoint lets you interact with the stored tags\' values in the local SQLite DB.', 
    'response_model': List[api.dto.Data],
    'tags': ['data']
}


def validate_period(period: str) -> bool:

    '''Validates given period of time for queries on Data table.
    
    Arguments:
     - period (str): period of time in forma 'last_amount_unit', where amount is an integer number
     greater than zero and unit is a valid unit in VALID_PERIODS

    Returns:
     - 'True' in case of success
     - 'False' in case of failure
    '''

    PERIOD_PATTERN: str = r'^last_\d+_(?:minute|hour|day|week|month)s?$'
    
    return search(PERIOD_PATTERN, period) is not None     
        

def calculate_period(period: str) -> tuple:

    '''Calculates start_time and end_time from a given textual period.
    
    Arguments:
     - period (str): period of time in forma 'last_amount_unit'

    Returns:
     - 'tuple(start_time, end_time)' in case of success
    '''

    TIMESTAMP_FORMAT: str = '%Y-%m-%dT%H:%M:%S'

    _, amount, unit = period.split('_')

    unit = f'{unit}s' if not unit.endswith('s') else unit

    # Taking the now datetime (end_time) and calculating the time previous now based on amount and unit (start_time)
    end_time = datetime.datetime.now()
    start_time = end_time - datetime.timedelta(**{unit: int(amount)})

    # Formatting timestamps
    start_time = start_time.strftime(TIMESTAMP_FORMAT)
    end_time = end_time.strftime(TIMESTAMP_FORMAT)

    return start_time, end_time
