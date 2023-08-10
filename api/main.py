import logging
import os
import sqlalchemy
import sqlalchemy.sql.functions as sqlfuncs

import sys

# os.chdir('.\\')
WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    print(f'{WORKING_DIR=}')
    sys.path.append(WORKING_DIR)

import database.models
import api.dto

from fastapi import FastAPI
from sqlalchemy.orm import Session, sessionmaker
from typing import List


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

    return engine if isinstance(engine, sqlalchemy.Engine) else None


# Logger initialization
logger = initialize_logger()
    
# Connection with DB
db_engine = db_connect()
if db_engine is not None:

    # Create Session
    Session = sessionmaker(bind=db_engine)     

    # with Session.begin() as session:
    with Session() as session:     

        app = FastAPI()        

@app.get('/')
async def root():
    return {'message': 'This is the root'}

@app.get('/tags')
async def get_tags(name_like: str = '%', description_like: str = '%') -> List[api.dto.Tags] | None:
    
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
async def get_data(start_time: str = '', end_time: str = '', name_like: str = '%', description_like: str = '%') -> None:
    
    if start_time == '' and end_time == '':
        sql_statement = sqlalchemy.select(sqlfuncs.min(database.models.Data.timestamp),
                                          sqlfuncs.max(database.models.Data.timestamp)) \
                                  .order_by(database.models.Data.timestamp.desc())
        
        start_time, end_time = session.execute(sql_statement).all()[0]
        

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
                            )
                        ) \
                        .order_by(database.models.Data.timestamp)
        
    data = session.execute(sql_statement)
    
    l = []
    
    for row in data:
        l.append([row.name, row.timestamp, row.value])

    return l
        
    

    # for tag in tags:
    #     l.append(api.dto.Tags(**{k:v for k,v in tag.__dict__.items() if not k.startswith('_')}))

    # return l
