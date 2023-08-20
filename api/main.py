import os
import sqlalchemy

from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import Session, sessionmaker
from typing import List
from fastapi.responses import FileResponse, RedirectResponse

import sys

WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    sys.path.append(WORKING_DIR)

import database.models
import api.dto

from database.utils import db_connect
from api.utils import (validate_period, calculate_period, generate_chart, 
                       API_METADATA, 
                       ROOT_ENDPOINT_METADATA, 
                       GET_TAGS_ENDPOINT_METADATA, POST_TAGS_ENDPOINT_METADATA, 
                       GET_DATA_ENDPOINT_METADATA,
                       GET_CHART_ENDPOINT_METADATA)
from misc.utils import initialize_logger


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

# root endpoint
@app.get('/', **ROOT_ENDPOINT_METADATA)
async def root():
    return RedirectResponse('http://127.0.0.1:8000/docs')


# GET tags endpoint
@app.get('/tags', **GET_TAGS_ENDPOINT_METADATA)
async def get_tags(name_like: str = '%', description_like: str = '%') -> List[api.dto.Tags]:
    
    # List of values to return
    tags: list = []
    
    sql_statement = sqlalchemy.select(database.models.Tags) \
                    .where(sqlalchemy.and_( \
                          database.models.Tags.deleted_at.is_(None),
                          database.models.Tags.name.like(name_like),
                          database.models.Tags.description.like(description_like))) \
                    .order_by(database.models.Tags.id)
    
    # https://docs.sqlalchemy.org/en/14/tutorial/data_select.html#selecting-orm-entities-and-columns
    tags_scalars = session.scalars(sql_statement)

    for tag in tags_scalars:
        tags.append(api.dto.Tags(**{k:v for k,v in tag.__dict__.items() if not k.startswith('_')}))

    return tags

# POST tags endpoint
@app.post('/tags', **POST_TAGS_ENDPOINT_METADATA)
async def post_tags(tags: List[api.dto.Tags]) -> List[api.dto.Tags]:
    
    # Data to be inserted in the DB
    data: list = []

    OPTIONAL_PARAMETERS: list = ['id', 'created_at', 'updated_at']

    # Before passing the values to the INSERT statement, we must remove those that are optional
    # in order to let the SQLAlchemy ORM process the default values for them (created_at, updated_at)
    for tag in tags:        
        data.append({k:v for k,v in tag.__dict__.items() if k not in OPTIONAL_PARAMETERS})

    sql_statement = sqlalchemy.insert(database.models.Tags).values(data).returning(database.models.Tags.id)
    
    # Checking if the insert statement has been successfull
    inserted_rows = session.execute(sql_statement).all()    
    if inserted_rows[-1].id > 0:
        session.commit()
        logger.info('Tags imported!')
            
    return tags


# GET data endpoint
@app.get('/data', **GET_DATA_ENDPOINT_METADATA)
async def get_data(period: str = 'last_1_hour', start_time: str = None, end_time: str = None, name_like: str = '%') -> List[api.dto.Data] | object:
    
    # List of values to return
    data = []

    # If the user is not providing any specific time range, then the parameter 'period' is considered   
    if start_time is None or end_time is None:
        if validate_period(period):
            start_time, end_time = calculate_period(period)
        else:
            raise HTTPException(status_code=422, detail='Invalid period')
                
    # Selecting the data
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
        
    data_rows = session.execute(sql_statement)
    
    for row in data_rows:
        data.append(api.dto.Data(name=row.name, timestamp=row.timestamp, value=row.value))

    return data


# GET chart endpoint
@app.get('/chart', **GET_CHART_ENDPOINT_METADATA)
async def get_chart(tag_name: str = None, period: str = 'last_1_hour', start_time: str = None, end_time: str = None):
    
    # Checking if the user set the tag_name parameter
    if tag_name is None:
        raise HTTPException(status_code=422, detail='No tag specified')
    else:
        
        # If the user is not providing any specific time range, then the parameter 'period' is considered   
        if start_time is None or end_time is None:
            if validate_period(period):
                start_time, end_time = calculate_period(period)
            else:
                raise HTTPException(status_code=422, detail='Invalid period')
            
    file_path = generate_chart(tag_name, start_time, end_time, session)
    
    # Checking if the user asked for wrong tag name
    if file_path is None:
        raise HTTPException(status_code=422, detail='No tag name found')
            
    return FileResponse(file_path)
