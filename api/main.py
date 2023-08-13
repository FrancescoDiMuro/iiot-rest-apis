import datetime
import logging
import os
import sqlalchemy
import sqlalchemy.sql.functions as sqlfuncs

import sys

WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    sys.path.append(WORKING_DIR)

import database.models
import api.dto

from database.utils import db_connect
from misc.utils import initialize_logger
from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import Session, sessionmaker
from typing import List


def validate_period(period: str) -> bool:

    VALID_PERIODS: list = [
        'minutes',
        'hours',
        'days',
        'weeks',
        'months'
    ]

    unit = period.split('_')[2]
    unit = f'{unit}s' if not unit.endswith('s') else unit  

    return True if unit in VALID_PERIODS else False       
        

def calculate_period(period: str) -> tuple:

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


SCRIPT_NAME: str = os.path.split(__file__)[1]

# Logger initialization
logger = initialize_logger(SCRIPT_NAME)
    
# Connection with DB
db_engine = db_connect(create_metadata=False, echo=True)
if db_engine is not None:

    # Create Session
    Session = sessionmaker(bind=db_engine)

    with Session() as session:     
        app = FastAPI()        


@app.get('/')
async def root():
    return {'message': 'This is the root'}


@app.get('/tags')
async def get_tags(name_like: str = '%', description_like: str = '%') -> List[api.dto.Tags]:
    
    sql_statement = sqlalchemy.select(database.models.Tags) \
                    .where(sqlalchemy.and_( \
                          database.models.Tags.deleted_at == None),
                          database.models.Tags.name.like(name_like),
                          database.models.Tags.description.like(description_like)) \
                    .order_by(database.models.Tags.id)
    
    # https://docs.sqlalchemy.org/en/14/tutorial/data_select.html#selecting-orm-entities-and-columns
    tags = session.scalars(sql_statement)
    
    l = []

    for tag in tags:
        l.append(api.dto.Tags(**{k:v for k,v in tag.__dict__.items() if not k.startswith('_')}))

    return l


@app.get('/data')
async def get_data(period: str = 'last_1_hour', start_time: str = '', end_time: str = '', name_like: str = '%') -> List[api.dto.Data] | object:
    
    # If the user is not providing any specific time range, then the period is considered   
    if start_time == '' and end_time == '':
        if validate_period(period):
            start_time, end_time = calculate_period(period)
        else:
            raise HTTPException(status_code=422, detail='Invalid period')
                
    # Selecting data
    sql_statement = sqlalchemy.select(
                        database.models.Tags.name,                         
                        database.models.Data.timestamp, 
                        database.models.Data.value) \
                        .join(database.models.Tags, database.models.Data.tag_id == database.models.Tags.id) \
                        .where(
                            sqlalchemy.and_(
                                sqlalchemy.between(
                                    database.models.Data.timestamp, 
                                    start_time, 
                                    end_time),
                                database.models.Tags.name.like(name_like)                            
                            )
                        ) \
                        .order_by(database.models.Data.timestamp)
        
    data = session.execute(sql_statement)
    
    l = []
    
    for row in data:
        l.append(api.dto.Data(name=row.name, timestamp=row.timestamp, value=row.value))

    return l
