import logging
import os
import snap7
import schedule
import sqlalchemy

from datetime import datetime
from re import findall
from snap7.util import get_real
from sqlalchemy.orm import Session, sessionmaker
from time import sleep
from typing import Dict, List, Union

import sys

WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    sys.path.append(WORKING_DIR)

from database.models import Base, Tags, Data


def initialize_logger() -> logging.Logger:
    
    script_name: str = os.path.split(__file__)[1]

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.DEBUG)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)

    logging_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    stream_handler.setFormatter(logging_formatter)
 
    logger.addHandler(stream_handler)

    return logger


def db_connect() -> sqlalchemy.Engine | None:
    DB_TYPE: str = 'sqlite'
    DB_API: str = 'pysqlite'
    DB_RELATIVE_FILE_PATH: str = 'database/data.db'
    DB_CONNECTION_STRING: str = f'{DB_TYPE}+{DB_API}:///{DB_RELATIVE_FILE_PATH}'

    # Create the SQLAlchemy engine and metadata (if it doesn't exist)
    engine: sqlalchemy.Engine = sqlalchemy.create_engine(DB_CONNECTION_STRING, echo=True)
    Base.metadata.create_all(bind=engine)

    return engine if isinstance(engine, sqlalchemy.Engine) else None

    
def plc_connect(plc_ip_address: int, plc_rack: int = 0, 
                plc_slot: int = 0, plc_port: int = 102) -> snap7.client.Client | None:
    
    client = snap7.client.Client()
    client.connect(plc_ip_address, plc_rack, plc_slot, plc_port)

    return client if client.get_connected() else None


def load_tags(db_session: Session, sql_statement: sqlalchemy.Select) -> List[Dict[str, Dict[str, int]]]:    
    
    TAG_ADDRESS_PATTERN: str = r'^DB(?P<db_number>[^@]+)@(?P<start>[^-]+)\-\>(?P<size>\d+)$'
    tags: list = []
    rows = db_session.execute(statement=sql_statement).all()
    for row in rows:
        db_number, start, size = findall(TAG_ADDRESS_PATTERN, row.address)[0]               
        tag = {
                row.id: {
                    'db_number': int(db_number),
                    'start': int(start),
                    'size': int(size)
                }
        }
        
        tags.append(tag)

    return tags


def read_data_from_plc(client: snap7.client.Client, tags: Dict[str, Dict[str, str]]) -> List[tuple]:
    
    data: List[Dict[str, Union[str, float]]] = []
    
    for tag in tags:
        for tag_id, tag_fields in tag.items():
            timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            value = round(get_real(client.db_read(**tag_fields), 0), 2)
            data.append((timestamp, value, tag_id))

    return data


def store_data_every_minute(client: snap7.client.Client, tags: Dict[str, Dict[str, int]], 
                            db_session: Session) -> bool:
    
    data = read_data_from_plc(client, tags)
    records: list = []
    
    for record in data:
        timestamp, value, tag_id = record
        records.append(Data(timestamp=timestamp, value=value, tag_id=tag_id))
    
    db_session.add_all(records)
    db_session.commit()

    logger.info('store_data_every_minute -> OK')
    return True


def store_data_every_five_minutes(client: snap7.client.Client, tags: Dict[str, Dict[str, int]], 
                                  db_session: Session) -> bool:
    
    data = read_data_from_plc(client, tags)
    records: list = []
    
    for record in data:
        timestamp, value, tag_id = record
        records.append(Data(timestamp=timestamp, value=value, tag_id=tag_id))
    
    db_session.add_all(records)
    db_session.commit()

    logger.info('store_data_every_five_minutes -> OK')
    return True


# Start application
PLC_IP_ADDRESS: str = '10.149.23.65'
PLC_RACK: int = 0
PLC_SLOT: int = 1
PLC_PORT: int = 102

# Tags' lists
tags_one_minute: List[Dict[str, Dict[str, int]]] = []
tags_five_minutes: List[Dict[str, Dict[str, int]]] = []

# Logger initialization
logger = initialize_logger()
    
# Connection with DB
db_engine = db_connect()
if db_engine is not None:
    logger.info('Connection with DB -> OK')

    # Create Session
    Session = sessionmaker(bind=db_engine)     
    
    # Connection with PLC
    client = plc_connect(PLC_IP_ADDRESS, PLC_RACK, PLC_SLOT, PLC_PORT)
    if client is not None:

        logger.info('Connection with PLC -> OK')

        # with Session.begin() as session:
        with Session() as session:

            # Tags one minute
            sql_statement = sqlalchemy.select(
                            Tags.id, Tags.address) \
                            .where(Tags.collection_interval == '1 min') \
                            .order_by(Tags.id)
            
            tags_one_minute = load_tags(session, sql_statement)

            # Tags five minutes
            sql_statement = sqlalchemy.select(
                            Tags.id, Tags.address) \
                            .where(Tags.collection_interval == '5 min') \
                            .order_by(Tags.id)
            
            tags_five_minutes = load_tags(session, sql_statement)
            
            # Trigger one-time storing of data before the scheduling
            store_data_every_minute(client, tags_one_minute, session)
            store_data_every_five_minutes(client, tags_five_minutes, session)
            
            # Schedulers
            schedule.every().minute.do(store_data_every_minute, client, tags_one_minute, session)
            schedule.every(5).minutes.do(store_data_every_five_minutes, client, tags_five_minutes, session)

            while 1:
                try:
                    schedule.run_pending()
                    sleep(0.25)
                except KeyboardInterrupt:
                    logger.info('Collection stopped by user.')
                    quit()
