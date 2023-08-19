import os
import schedule
import sqlalchemy

from sqlalchemy.orm import Session, sessionmaker
from time import sleep
from typing import Dict, List

import sys

WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    sys.path.append(WORKING_DIR)

import database.models
from collector.utils import plc_connect, store_data
from database.utils import db_connect, load_tags
from misc.utils import initialize_logger

   
# Start application
PLC_IP_ADDRESS: str = '10.149.23.65'
PLC_RACK: int = 0
PLC_SLOT: int = 1
PLC_PORT: int = 102

SCRIPT_NAME: str = os.path.split(__file__)[1]

# Tags' lists
tags_one_minute: List[Dict[str, Dict[str, int]]] = []
tags_five_minutes: List[Dict[str, Dict[str, int]]] = []

# Logger initialization
logger = initialize_logger(SCRIPT_NAME)
    
# Connection with DB
engine = db_connect(create_metadata=True, echo=False)
if engine is not None:
    logger.info('Connection with DB -> OK')

    # Create Session
    Session = sessionmaker(bind=engine)

    # Connection with PLC
    client = plc_connect(PLC_IP_ADDRESS, PLC_RACK, PLC_SLOT, PLC_PORT)
    if client is not None:

        logger.info('Connection with PLC -> OK')

        # Session initailization (without auto-commit)
        with Session() as session:

            # Selecting tags with one minute collection interval
            sql_statement = sqlalchemy.select(
                                database.models.Tags.id, 
                                database.models.Tags.address) \
                            .where(sqlalchemy.and_(
                                database.models.Tags.collection_interval == '1 min',
                                database.models.Tags.deleted_at.is_(None))) \
                            .order_by(database.models.Tags.id)
            
            tags_one_minute = load_tags(session, sql_statement)

            # Selecting tags with five minutes collection interval
            sql_statement = sqlalchemy.select(
                                database.models.Tags.id, 
                                database.models.Tags.address) \
                            .where(sqlalchemy.and_(
                                database.models.Tags.collection_interval == '5 min',
                                database.models.Tags.deleted_at .is_(None))) \
                            .order_by(database.models.Tags.id)
            
            tags_five_minutes = load_tags(session, sql_statement)
            
            # Trigger one-time storing data before the scheduling
            store_data(client, tags_one_minute, session, logger, '1 min')
            store_data(client, tags_five_minutes, session, logger, '5 min')
            
            # Schedulers
            schedule.every().minute.do(store_data, client, tags_one_minute, session, logger, '1 min')
            schedule.every(5).minutes.do(store_data, client, tags_five_minutes, session, logger, '5 min')

            while 1:
                try:
                    schedule.run_pending()
                    sleep(0.25)
                except KeyboardInterrupt:
                    logger.info('Collection stopped by user.')
                    session.close()
                    quit()
