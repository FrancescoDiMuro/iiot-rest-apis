import logging
import matplotlib.pyplot as plt
import numpy as np
import os
import sqlalchemy
import sys

sys.path.append(os.getcwd())

from database.models import Data, Tags
from sqlalchemy.orm import sessionmaker


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
    DB_RELATIVE_FILE_PATH: str = '/data.db'
    DB_CONNECTION_STRING: str = f'{DB_TYPE}+{DB_API}://{DB_RELATIVE_FILE_PATH}'

    engine: sqlalchemy.Engine = sqlalchemy.create_engine(DB_CONNECTION_STRING, echo=True)

    return engine if isinstance(engine, sqlalchemy.Engine) else None

logger = initialize_logger()

# Connection with DB
db_engine = db_connect()
if db_engine is not None:
    logger.info('Connection with DB -> OK')

    Session = sessionmaker(bind=db_engine)

    with Session() as session:

        query_start_timestamp = '20230802T160024'
        query_end_timestamp = '20230802T161024'

        # TODO: Implement JOIN
        sql_statement = sqlalchemy.select(Data.tag_id, Data.timestamp, Data.value). \
                        where(sqlalchemy.between(Data.timestamp, query_start_timestamp, query_end_timestamp))
        
        data = session.execute(sql_statement)

        for row in data:
            print(row)

#     logger.info('Generating plot...')


# params = (query_start_timestamp, query_end_timestamp, 'pv')            
# process_values = db_cursor.execute('''SELECT timestamp, value
#                                         FROM data WHERE timestamp BETWEEN ? AND ? AND
#                                         tag_name = ?''', params).fetchall()

# for row in process_values:             
#     dates.append(matplotlib.dates.datestr2num(row[0]))
#     values.append(row[1])

# params = (query_start_timestamp, query_end_timestamp, r'%set%')
# setpoints = db_cursor.execute('''SELECT tag_name, MAX(value)
#                                     FROM data
#                                     WHERE timestamp BETWEEN ? AND ? AND
#                                     tag_name LIKE ?
#                                     GROUP BY tag_name
#                                     ORDER BY tag_name''', params).fetchall()

# date_formatter = matplotlib.dates.DateFormatter('%H:%M:%S')
# minute_locator = matplotlib.dates.MinuteLocator(interval=5)

# fig, ax = plt.subplots()

# # Plotting the process value data
# ax.plot(dates, values, marker='.', label='Temp.')

# ax.set_title('P1I 115.03:Temp.')
# ax.grid(color='b', linewidth=0.2)
# ax.set_xlabel('Time')
# ax.set_ylabel('°C')

# ax.xaxis.set_major_formatter(date_formatter)
# ax.xaxis.set_major_locator(minute_locator)

# # Obtaining setpoints values from tuples (tag_name, value)
# set_hh, set_h, set_l, set_ll = [t[1] for t in setpoints]

# # Setting offsets in order to see the HH and LL setpoints
# if set_hh > 0:
#     h_limit = set_hh + (set_hh/4)
# else:
#     h_limit = set_hh - (set_hh/4)

# if set_ll > 0:
#     l_limit = set_ll - (set_hh/4)
# else:
#     l_limit = set_ll + (set_hh/4)

# # Setting the Y ticks
# ax.set_yticks(np.arange(l_limit, h_limit, Y_TICK_STEP)) 

# # Getting the start timestamp and end timestamp
# plot_start_timestamp = dates[0]
# plot_end_timestamp = dates[-1]

# # Plotting horizontal lines (setpoints)
# # axhline
# ax.hlines(y=set_hh, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='r', label='Set HH')
# ax.hlines(y=set_h, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='g', label='Set H')
# ax.hlines(y=set_l, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='c', label='Set L')
# ax.hlines(y=set_ll, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='m', label='Set LL')
# ax.legend(title='Legenda').get_title().set_fontstyle = 'italic'

# logger.info('Exporting plot...')

# # plt.show()
# plt.savefig('.\export.png')

# logger.info('Done :)')