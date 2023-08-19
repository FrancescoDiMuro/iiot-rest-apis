import datetime
import matplotlib.pyplot as plt
import matplotlib.dates
import os
import sqlalchemy

from fastapi.responses import FileResponse
from re import search
from sqlalchemy.orm import Session
from typing import List

import sys

WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    sys.path.append(WORKING_DIR)

import api.dto
import database.models

API_METADATA_DESCRIPTION: str = '''#### Industrial Internet of Things REST API for gathering, storing and analysing data from IIoT devices.

### Introduction
This APIs lets you configure tags to store data of at different time intervals, get their values with the possibility to apply filters, and generate trends based on the given criterias.

### Configured endpoints
The configured endpoints can be found below.

### Credits
- Developer: Francesco Di Muro - [Work Portfolio](https://www.en.francescodimuro.com/)
- My girlfriend (who tested the various features of the application as a user)'''

API_METADATA: dict = {
    'title': 'IIoT REST APIs',
    'description': API_METADATA_DESCRIPTION
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

GET_CHART_ENDPOINT_METADATA: dict = {
    'summary': 'GET Chart', 
    'description': 'This endpoint lets you generate a chart with the specified tag name and period.', 
    'response_class': FileResponse,
    'tags': ['chart']
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


def generate_chart(tag_name: str, start_time: str, end_time: str, session: Session) -> str | None:
    
    '''Generates a chart based on the specified criteria.

    Arguments:
     - tag_name (str): name of the tag to draw on the chart
     - start_time (str): start time of the data to be retrieved
     - end_time (str): end time of the data to be retrieved
     - session (sqlalchemy.orm.Session): session in which execute the SQL queries

    Returns:
     - 'chart_file_name' in case of success
     - 'None' in case of failure
    '''

    dates: list = []
    values: list = []
    setpoints: list = []

    chart_file_name: str = './api/export.png'

    # Obtaining SYSTEM[n]-PROBE[m]
    tag_name_prefix = '-'.join(tag_name.split('-')[0:2])
        
    sql_statement = sqlalchemy.select(
                    database.models.Tags.name, 
                    database.models.Data.timestamp, 
                    database.models.Data.value) \
                    .join(database.models.Tags, database.models.Data.tag_id == database.models.Tags.id) \
                    .where(
                        sqlalchemy.and_(
                            database.models.Tags.name.like(tag_name),
                            sqlalchemy.between(
                                database.models.Data.timestamp, 
                                start_time, 
                                end_time),
                        )
                    ) \
                    .order_by(database.models.Data.timestamp)
    
    data = session.execute(sql_statement)
    
    # FrozenResult is used in order to freeze the data in the cache to not re-query the database
    frozen_result = data.freeze()

    # Checking if any result has been returned
    if len(frozen_result.data) == 0:
        return None
    else:        

        # Saving dates and values in two different lists
        for row in frozen_result.data:
            dates.append(matplotlib.dates.datestr2num(row.timestamp))
            values.append(row.value)

        # Querying setpoints' values
        setpoints_filter = f'{tag_name_prefix}-SET%'

        sql_statement = sqlalchemy.select(
                        database.models.Tags.name, 
                        database.models.Data.timestamp, 
                        database.models.Data.value) \
                        .join(database.models.Tags, database.models.Data.tag_id == database.models.Tags.id) \
                        .where(
                            sqlalchemy.and_(
                                database.models.Tags.name.like(setpoints_filter),
                                sqlalchemy.between(
                                    database.models.Data.timestamp, 
                                    start_time,
                                    end_time),
                            )
                        ) \
                        .group_by(database.models.Tags.name) \
                        .order_by(database.models.Tags.id)
        
        data = session.execute(sql_statement)

        for row in data:
            setpoints.append(row.value)
        
        # Querying tag's info
        sql_statement = sqlalchemy.select(
                        database.models.Tags.description, 
                        database.models.Tags.low_limit, 
                        database.models.Tags.high_limit, 
                        database.models.Tags.egu) \
                        .where(database.models.Tags.name.like(tag_name))
        
        data = session.execute(sql_statement).one()

        tag_description, tag_low_limit, tag_high_limit, tag_egu = data        

        # Calculating the time delta between end_time and start_time
        time_delta: datetime.timedelta = datetime.datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S') - \
                    datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')

        timestamp_format = '%H:%M' if time_delta.days > 1 else '%d/%m %H:%M'

        date_formatter = matplotlib.dates.DateFormatter(timestamp_format)
        major_locator = matplotlib.dates.AutoDateLocator()

        # Creating subplots
        fig, ax = plt.subplots(figsize=(12.8, 7.2))

        # Plotting the process value data
        ax.plot(dates, values, label='PV') # marker='.'

        ax.set_title(tag_description).set_fontweight('bold')
        ax.grid(color='b', linewidth=0.2)
        ax.set_xlabel('Time')
        ax.set_ylabel(tag_egu)

        ax.xaxis.set_major_formatter(date_formatter)
        ax.xaxis.set_major_locator(major_locator)

        # Obtaining setpoints values from tuples (tag_name, value)
        set_hh, set_h, set_l, set_ll = setpoints

        # Setting the Y ticks
        ax.set_ylim([tag_low_limit, tag_high_limit])

        # Getting the start timestamp and end timestamp
        plot_start_timestamp = dates[0]
        plot_end_timestamp = dates[-1]

        # Plotting horizontal lines (setpoints)
        ax.hlines(y=set_hh, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='r', label='Set HH')
        ax.hlines(y=set_h, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='g', label='Set H')
        ax.hlines(y=set_l, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='c', label='Set L')
        ax.hlines(y=set_ll, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='m', label='Set LL')
        ax.legend(ncols=3, loc='lower left')

        # Auto-formatting dates to be displayed correctly
        fig.autofmt_xdate()       

        plt.savefig(chart_file_name)

        return chart_file_name
    