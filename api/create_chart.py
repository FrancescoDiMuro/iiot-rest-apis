import datetime
import matplotlib.pyplot as plt
import matplotlib.dates
import os
import sqlalchemy
import sqlalchemy.sql.functions as sqlfuncs

from sqlalchemy.orm import sessionmaker

import sys

WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    sys.path.append(WORKING_DIR)

from database.models import Data, Tags
from database.utils import db_connect
from misc.utils import initialize_logger


SCRIPT_NAME: str = os.path.split(__file__)[1]

setpoints: list = []

dates: list = []
values: list = []

# Start application
logger = initialize_logger(SCRIPT_NAME)

# Connection with DB
db_engine = db_connect()
if db_engine is not None:
    logger.info('Connection with DB -> OK')

    Session = sessionmaker(bind=db_engine)

    with Session() as session:

        # Query time range
        query_start_timestamp = '2023-08-18T00:00:00'
        query_end_timestamp = '2023-08-19T00:00:00'

        sql_statement = sqlalchemy.select(
                        Tags.name, 
                        Data.timestamp, 
                        Data.value) \
                        .join(Tags, Data.tag_id == Tags.id) \
                        .where(
                            sqlalchemy.and_(
                                Tags.name.like('SYSTEM2%-PV'),
                                sqlalchemy.between(
                                    Data.timestamp, 
                                    query_start_timestamp, 
                                    query_end_timestamp),
                            )
                        ) \
                        .order_by(Data.timestamp)
        
        data = session.execute(sql_statement)

        for row in data:
            dates.append(matplotlib.dates.datestr2num(row.timestamp))
            values.append(row.value)

        sql_statement = sqlalchemy.select(
                        Tags.name, 
                        Data.timestamp, 
                        Data.value) \
                        .join(Tags, Data.tag_id == Tags.id) \
                        .where(
                            sqlalchemy.and_(
                                Tags.name.like('SYSTEM2%-SET-%'),
                                sqlalchemy.between(
                                    Data.timestamp, 
                                    query_start_timestamp,
                                    query_end_timestamp),
                            )
                        ) \
                        .group_by(Tags.name) \
                        .order_by(Tags.id)
        
        data = session.execute(sql_statement)

        for row in data:
            setpoints.append(row.value)

        sql_statement = sqlalchemy.select(
                        Tags.description, 
                        Tags.low_limit, 
                        Tags.high_limit, 
                        Tags.egu) \
                        .where(Tags.name.like('SYSTEM2%-PV'))
        
        data = session.execute(sql_statement).one()

        tag_description, tag_low_limit, tag_high_limit, tag_egu = data

    logger.info('Generating plot...')
    
    time_delta: datetime.timedelta = datetime.datetime.strptime(query_end_timestamp, '%Y-%m-%dT%H:%M:%S') - \
                datetime.datetime.strptime(query_start_timestamp, '%Y-%m-%dT%H:%M:%S')
    
    
    
    timestamp_format = '%H:%M' if time_delta.days > 1 else '%d/%m %H:%M'

    date_formatter = matplotlib.dates.DateFormatter(timestamp_format)
    major_locator = matplotlib.dates.AutoDateLocator()

    fig, ax = plt.subplots()

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

fig.autofmt_xdate()

logger.info('Exporting plot...')

plt.savefig('./api/export.png')

logger.info('Done :)')
