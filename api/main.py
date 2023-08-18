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
from api.utils import (validate_period, calculate_period, 
                       API_METADATA, 
                       ROOT_ENDPOINT_METADATA, 
                       GET_TAGS_ENDPOINT_METADATA, POST_TAGS_ENDPOINT_METADATA, 
                       GET_DATA_ENDPOINT_METADATA)
from misc.utils import initialize_logger
from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import Session, sessionmaker
from typing import List


SCRIPT_NAME: str = os.path.split(__file__)[1]

# Logger initialization
logger = initialize_logger(SCRIPT_NAME)
    
# Connection with DB
db_engine = db_connect(create_metadata=False, echo=False)
if db_engine is not None:

    # Create Session
    Session = sessionmaker(bind=db_engine)

    with Session() as session:     
        app = FastAPI(**API_METADATA)  


@app.get('/', **ROOT_ENDPOINT_METADATA)
async def root():
    return {'message': 'This is the root'}


@app.get('/tags', **GET_TAGS_ENDPOINT_METADATA)
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


@app.post('/tags', **POST_TAGS_ENDPOINT_METADATA)
async def get_tags(tags: List[api.dto.Tags]) -> List[api.dto.Tags]:
    
    data: list = []

    OPTIONAL_PARAMETERS: list = ['id', 'created_at', 'updated_at']

    # Before passing the values to the INSERT statement, we must remove those that are optional
    # in order to let the SQLAlchemy ORM process the default values for them (created_at, updated_at)
    for tag in tags:        
        data.append({k:v for k,v in tag.__dict__.items() if k not in OPTIONAL_PARAMETERS})

    sql_statement = sqlalchemy.insert(database.models.Tags).values(data).returning(database.models.Tags.id)
    
    inserted_rows = session.execute(sql_statement).all()    
    if inserted_rows[-1].id > 0:
        session.commit()
        logger.info('Tags imported!')
            
    return tags

@app.get('/data', **GET_DATA_ENDPOINT_METADATA)
async def get_data(period: str = 'last_1_hour', start_time: str = '', end_time: str = '', name_like: str = '%') -> List[api.dto.Data] | object:
    
    # If the user is not providing any specific time range, then the parameter 'period' is considered   
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
