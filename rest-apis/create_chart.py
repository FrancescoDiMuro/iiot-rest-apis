import logging
import matplotlib.pyplot as plt
import matplotlib.dates
import numpy as np
import os
import sqlalchemy
import sqlalchemy.sql.functions as sqlfuncs
import sys

sys.path.append(os.getcwd())

from database.models import Data, Tags
from sqlalchemy.orm import sessionmaker
from typing import Dict


def initialize_logger() -> logging.Logger:
    
    script_name: str = os.path.split(__file__)[1]

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.DEBUG)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)

    logging_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    stream_handler.setFormatter(logging_formatter)

    # add ch to logger
    logger.addHandler(stream_handler)

    return logger


def db_connect() -> sqlalchemy.Engine | None:
    DB_TYPE: str = 'sqlite'
    DB_API: str = 'pysqlite'
    DB_RELATIVE_FILE_PATH: str = 'database/data.db'
    DB_CONNECTION_STRING: str = f'{DB_TYPE}+{DB_API}:///{DB_RELATIVE_FILE_PATH}'

    engine: sqlalchemy.Engine = sqlalchemy.create_engine(DB_CONNECTION_STRING, echo=True)

    return engine if isinstance(engine, sqlalchemy.Engine) else None


process_values: list = []
setpoints: list = []

dates: list = []
values: list = []

tag_info: Dict[str, str]


# Logger initialization
logger = initialize_logger()

# Connection with DB
db_engine = db_connect()
if db_engine is not None:
    logger.info('Connection with DB -> OK')

    Session = sessionmaker(bind=db_engine)

    with Session() as session:

        query_start_timestamp = '2023-08-08T14:00:00'
        query_end_timestamp = '2023-08-08T15:00:00'

        sql_statement = sqlalchemy.select(
                        Tags.name, 
                        Data.timestamp, 
                        Data.value) \
                        .join(Tags, Data.tag_id == Tags.id) \
                        .where(
                            sqlalchemy.and_(
                                Tags.name.like('%-PV'),
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
                                Tags.name.like('%-SET-%'),
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
                        .where(Tags.name.like('%-PV'))
        
        data = session.execute(sql_statement).one()

        tag_description, tag_low_limit, tag_high_limit, tag_egu = data

    logger.info('Generating plot...')

    date_formatter = matplotlib.dates.DateFormatter('%H:%M:%S')
    minute_locator = matplotlib.dates.MinuteLocator(interval=5)

    fig, ax = plt.subplots()

# Plotting the process value data
ax.plot(dates, values, label='PV') # marker='.'

ax.set_title(tag_description).set_fontweight('bold')
ax.grid(color='b', linewidth=0.2)
ax.set_xlabel('Time')
ax.set_ylabel(tag_egu)

ax.xaxis.set_major_formatter(date_formatter)
ax.xaxis.set_major_locator(minute_locator)

# Obtaining setpoints values from tuples (tag_name, value)
set_hh, set_h, set_l, set_ll = setpoints

# Setting the Y ticks
ax.set_ylim([tag_low_limit, tag_high_limit])

# Getting the start timestamp and end timestamp
plot_start_timestamp = dates[0]
plot_end_timestamp = dates[-1]

# Plotting horizontal lines (setpoints)
# axhline
ax.hlines(y=set_hh, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='r', label='Set HH')
ax.hlines(y=set_h, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='g', label='Set H')
ax.hlines(y=set_l, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='c', label='Set L')
ax.hlines(y=set_ll, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='m', label='Set LL')
ax.legend(ncols=3, loc='lower left')

fig.autofmt_xdate()

logger.info('Exporting plot...')

# plt.show()
plt.savefig('./rest-apis/export.png')

logger.info('Done :)')