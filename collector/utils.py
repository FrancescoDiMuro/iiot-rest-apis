import os
import snap7

from datetime import datetime
from logging import Logger
from snap7.util import get_real
from sqlalchemy.orm import Session
from typing import List, Dict, Union

import sys

WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    sys.path.append(WORKING_DIR)

from database.models import Data

def plc_connect(plc_ip_address: str, plc_rack: int = 0, 
                plc_slot: int = 0, plc_port: int = 102) -> snap7.client.Client | None:
    
    client = snap7.client.Client()
    client.connect(plc_ip_address, plc_rack, plc_slot, plc_port)

    return client if client.get_connected() else None


def read_data_from_plc(client: snap7.client.Client, tags: Dict[str, Dict[str, str]]) -> List[tuple]:
    
    data: List[Dict[str, Union[str, float]]] = []
    
    for tag in tags:
        for tag_id, tag_fields in tag.items():
            timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            value = round(get_real(client.db_read(**tag_fields), 0), 2)
            data.append((timestamp, value, tag_id))

    return data


def store_data_every_minute(client: snap7.client.Client, tags: Dict[str, Dict[str, int]], 
                            db_session: Session, logger: Logger) -> bool:
    
    data = read_data_from_plc(client, tags)
    records: list = []
    
    for record in data:
        timestamp, value, tag_id = record
        records.append(Data(timestamp=timestamp, value=value, tag_id=tag_id))
    
    db_session.add_all(records)
    db_session.commit()

    # if data_inserted:

    logger.info('store_data_every_minute -> OK')
    return True


def store_data_every_five_minutes(client: snap7.client.Client, tags: Dict[str, Dict[str, int]], 
                                  db_session: Session, logger: Logger) -> bool:
    
    data = read_data_from_plc(client, tags)
    records: list = []
    
    for record in data:
        timestamp, value, tag_id = record
        records.append(Data(timestamp=timestamp, value=value, tag_id=tag_id))
    
    db_session.add_all(records)
    db_session.commit()

    logger.info('store_data_every_five_minutes -> OK')
    return True