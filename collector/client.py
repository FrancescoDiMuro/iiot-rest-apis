
import logging
import matplotlib.dates
import matplotlib.pyplot as plt
import numpy as np
import os.path
import snap7
import sqlite3
import schedule
import time
from datetime import datetime
from snap7.util import get_real
from typing import Dict, List


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

    
def db_connect(db_filename: str) -> sqlite3.Connection | None:
    db_connection = sqlite3.connect(db_filename)
    return db_connection if isinstance(db_connection, sqlite3.Connection) else None

    
def plc_connect(plc_ip_address: int, plc_rack: int = 0, plc_slot: int = 0, plc_port: int = 102) -> snap7.client.Client | None:
    client = snap7.client.Client()
    client.connect(plc_ip_address, plc_rack, plc_slot, plc_port)

    return client if client.get_connected() else None


def read_data_from_plc(client: snap7.client.Client, tags: Dict[str, int]) -> List[tuple]:
    data: list = []

    for tag_name, tag_fields in tags.items():
        timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
        value = round(get_real(client.db_read(**tag_fields), 0), 2)
        data.append((timestamp, tag_name, value))

    return data


def store_data_every_minute(client: snap7.client.Client, tags: Dict[str, int], db_connection: sqlite3.Connection) -> bool:
    
    db_cursor = db_connection.cursor()

    data = read_data_from_plc(client, tags)
    if isinstance(db_cursor.executemany('INSERT INTO data VALUES (?, ?, ?)', data), sqlite3.Cursor):
        db_connection.commit()
        logger.info('store_data_every_minute -> OK')
        return True
    
    return False


def store_data_every_five_minutes(client: snap7.client.Client, tags: Dict[str, int], db_connection: sqlite3.Connection) -> bool:
    
    db_cursor = db_connection.cursor()

    data = read_data_from_plc(client, tags)
    if isinstance( db_cursor.executemany('INSERT INTO data VALUES (?, ?, ?)', data), sqlite3.Cursor):
        db_connection.commit()
        logger.info('store_data_every_five_minutes -> OK')
        return True
    
    return False

# Start application
PLC_IP_ADDRESS: str = '10.149.23.65'
PLC_RACK: int = 0
PLC_SLOT: int = 1
PLC_PORT: int = 102

DATA_DB_FILE_NAME: str = '..\database\data.db'

EVERY_FIVE_MINUTES: int = 300
Y_TICK_STEP: int = 10

# Tags' lists
tags_one_minute: Dict[str, int] = {'pv': {'db_number': 842, 'start': 48, 'size': 4}}

tags_five_minutes: Dict[str, int] = {'set-ll': {'db_number': 842, 'start': 32, 'size': 4},
                                     'set-l' : {'db_number': 842, 'start': 36, 'size': 4},
                                     'set-h' : {'db_number': 842, 'start': 40, 'size': 4},
                                     'set-hh': {'db_number': 842, 'start': 44, 'size': 4}
                                    }

process_values: list = []
setpoints: list = []

dates: list = []
values: list = []

logger = initialize_logger()
    
# Connection with DB
db_connection = db_connect(DATA_DB_FILE_NAME)
if db_connection is not None:
    logger.info('Connection with DB -> OK')
    db_cursor = db_connection.cursor()

    # db_cursor.execute('DROP TABLE data')
    db_cursor.execute('CREATE TABLE IF NOT EXISTS data (timestamp TEXT, tag_name TEXT, value REAL)')

    # Connection with PLC
    client = plc_connect(PLC_IP_ADDRESS, PLC_RACK, PLC_SLOT, PLC_PORT)
    if client is not None:

        logger.info('Connection with PLC -> OK')

        schedule.every().minute.do(store_data_every_minute, client, tags_one_minute, db_connection)
        schedule.every(5).minutes.do(store_data_every_five_minutes, client, tags_five_minutes, db_connection)

        try:
            query_start_timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')

            while 1:
                schedule.run_pending()
                time.sleep(1)

        except KeyboardInterrupt:

            query_end_timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
            
            logger.info('Generating plot...')

            params = (query_start_timestamp, query_end_timestamp, 'pv')            
            process_values = db_cursor.execute('''SELECT timestamp, value
                                                  FROM data WHERE timestamp BETWEEN ? AND ? AND
                                                  tag_name = ?''', params).fetchall()
            
            for row in process_values:             
                dates.append(matplotlib.dates.datestr2num(row[0]))
                values.append(row[1])

            params = (query_start_timestamp, query_end_timestamp, r'%set%')
            setpoints = db_cursor.execute('''SELECT tag_name, MAX(value)
                                             FROM data
                                             WHERE timestamp BETWEEN ? AND ? AND
                                             tag_name LIKE ?
                                             GROUP BY tag_name
                                             ORDER BY tag_name''', params).fetchall()

            date_formatter = matplotlib.dates.DateFormatter('%H:%M:%S')
            minute_locator = matplotlib.dates.MinuteLocator(interval=5)

            fig, ax = plt.subplots()

            # Plotting the process value data
            ax.plot(dates, values, marker='.', label='Temp.')

            ax.set_title('P1I 115.03:Temp.')
            ax.grid(color='b', linewidth=0.2)
            ax.set_xlabel('Time')
            ax.set_ylabel('°C')

            ax.xaxis.set_major_formatter(date_formatter)
            ax.xaxis.set_major_locator(minute_locator)

            # Obtaining setpoints values from tuples (tag_name, value)
            set_hh, set_h, set_l, set_ll = [t[1] for t in setpoints]

            # Setting offsets in order to see the HH and LL setpoints
            if set_hh > 0:
                h_limit = set_hh + (set_hh/4)
            else:
                h_limit = set_hh - (set_hh/4)

            if set_ll > 0:
                l_limit = set_ll - (set_hh/4)
            else:
                l_limit = set_ll + (set_hh/4)

            # Setting the Y ticks
            ax.set_yticks(np.arange(l_limit, h_limit, Y_TICK_STEP)) 

            # Getting the start timestamp and end timestamp
            plot_start_timestamp = dates[0]
            plot_end_timestamp = dates[-1]

            # Plotting horizontal lines (setpoints)
            # axhline
            ax.hlines(y=set_hh, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='r', label='Set HH')
            ax.hlines(y=set_h, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='g', label='Set H')
            ax.hlines(y=set_l, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='c', label='Set L')
            ax.hlines(y=set_ll, xmin=plot_start_timestamp, xmax=plot_end_timestamp, colors='m', label='Set LL')
            ax.legend(title='Legenda').get_title().set_fontstyle = 'italic'

            logger.info('Exporting plot...')

            # plt.show()
            plt.savefig('.\export.png')

            logger.info('Done :)')

        finally:    
            db_cursor.close()
            client.disconnect()
