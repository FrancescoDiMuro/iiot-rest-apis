import sqlalchemy

from database.models import Base
from re import findall
from sqlalchemy.orm import Session
from typing import List, Dict


DB_TYPE: str = 'sqlite'
DB_API: str = 'pysqlite'
DB_RELATIVE_FILE_PATH: str = 'database/data.db'
DB_CONNECTION_STRING: str = f'{DB_TYPE}+{DB_API}:///{DB_RELATIVE_FILE_PATH}'


def db_connect(create_metadata: bool = False, echo: bool = False) -> sqlalchemy.Engine | None:    

    '''Connects to the specified db (SQLite).
    
    Arguments:
     - create_metadata (bool): flag to enable the creation of database metadata
     - echo (bool): flag to enable the printing of debug messages on the console

    Returns:
     - 'sqlalchemy.Engine' in case of success
     - 'None' in case of failure
    '''
    
    # Create the SQLAlchemy engine and metadata (if specified)
    engine: sqlalchemy.Engine = sqlalchemy.create_engine(DB_CONNECTION_STRING, echo=echo)
    if create_metadata:
        Base.metadata.create_all(bind=engine)

    return engine if isinstance(engine, sqlalchemy.Engine) else None


def load_tags(session: Session, sql_statement: sqlalchemy.Select) -> List[Dict[str, Dict[str, int]]]:    
    
    '''Loads tags based on specified SQL statement and specified session.
    
    Arguments:
     - session (sqlalchemy.orm.Session): session used to commit transactions to db
     - sql_statement (sqlalchemy.Select): select statement to be used on the session

    Returns:
     - 'List[Dict[str, Dict[str, int]]]' in case of success
    '''
    
    TAG_ADDRESS_PATTERN: str = r'^DB(?P<db_number>[^@]+)@(?P<start>[^-]+)\-\>(?P<size>\d+)$'
    tags: list = []
    rows = session.execute(statement=sql_statement).all()
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
