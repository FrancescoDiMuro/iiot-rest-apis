import inspect
import logging
import os
import sqlalchemy

import sys

WORKING_DIR: str = os.getcwd()

if WORKING_DIR not in sys.path:
    sys.path.append(WORKING_DIR)

import database.models

from fastapi import FastAPI
from sqlalchemy.orm import Session, sessionmaker


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

def object_as_dict(obj):
    result = {instance.key: getattr(obj, instance.key)  for instance in inspect(obj).mapper.column_attrs}
    print(result)
    return result

@app.get('/tags')
async def get_tags():

    l = []
    
    sql_statement = sqlalchemy.select(database.models.Tags) \
                    .where(database.models.Tags.deleted_at == None) \
                    .order_by(database.models.Tags.id)
    
    # https://docs.sqlalchemy.org/en/14/tutorial/data_select.html#selecting-orm-entities-and-columns
    tags = session.scalars(sql_statement)
    for tag in tags:
        l.append({k: v for k, v in tag.__dict__.items() if not k.startswith('_')})

    return l
