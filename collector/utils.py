import os
import snap7

from datetime import datetime
from logging import Logger
from snap7.util import get_real
from sqlalchemy import insert
from sqlalchemy.orm import Session
from typing import List, Dict, Union

import sys

WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    sys.path.append(WORKING_DIR)

from database.models import Data

def plc_connect(plc_ip_address: str, plc_rack: int = 0, 
                plc_slot: int = 0, plc_port: int = 102) -> snap7.client.Client | None:
    
    '''Connects with the specified PLC.
    
    Arguments:
     - plc_ip_address (str): IP address of the PLC
     - plc_rack (int): rack number where the PLC is located
     - plc_slot (int): slot number where the PLC is located
     - plc_port (int): port number used to connect to the PLC

    Returns:
     - 'snap7.client.Client' in case of success
     - 'None' in case of failure
    '''
    
    client = snap7.client.Client()
    client.connect(plc_ip_address, plc_rack, plc_slot, plc_port)

    return client if client.get_connected() else None


def read_data_from_plc(client: snap7.client.Client, tags: Dict[str, Dict[str, str]]) -> List[tuple]:
    
    '''Reads data from specified PLC.
    
    Arguments:
     - client (snap7.client.Client): client instance of the connected PLC
     - tags (Dict[str, Dict[str, str]]): dictionary of tags (dictionaries)

    Returns:
     - 'List[tuple]' in case of success
    '''
    
    data: List[Dict[str, Union[str, float]]] = []
    
    for tag in tags:
        for tag_id, tag_fields in tag.items():
            timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            value = round(get_real(client.db_read(**tag_fields), 0), 2)
            data.append((timestamp, value, tag_id))

    return data


def store_data(client: snap7.client.Client, tags: Dict[str, Dict[str, int]], 
               session: Session, logger: Logger, tags_collection_interval: str = '') -> bool:
    
    '''Store data into the database.
    
    Arguments:
     - client (snap7.client.Client): client instance of the connected PLC
     - tags (Dict[str, Dict[str, str]]): dictionary of tags (dictionaries)
     - session (sqlalchemy.orm.Session): session used to commit transactions to db
     - tags_collection_interval (str): collection interval of the tags passed to the function

    Returns:
     - 'bool' in case of success
    '''
    
    
    data = read_data_from_plc(client, tags)
    records: list = []
    
    for record in data:
        timestamp, value, tag_id = record
        records.append({'timestamp': timestamp, 
                        'value': value, 
                        'tag_id': tag_id})

    sql_statement = insert(Data).values(records).returning(Data.id)
    
    inserted_rows = session.execute(sql_statement).all()
    if inserted_rows[-1].id > 0:
        session.commit()
        logger.info(f'store_data ({tags_collection_interval}) -> OK')
        return True
    
    return False
