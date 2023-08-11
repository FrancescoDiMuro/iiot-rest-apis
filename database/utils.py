import sqlalchemy

from database.models import Base
from re import findall
from sqlalchemy.orm import Session
from typing import List, Dict


DB_TYPE: str = 'sqlite'
DB_API: str = 'pysqlite'
# DB_ABSOLUTE_FILE_PATH: str = 'D:\\Python\\snap7_client\\database\\data.db'
DB_RELATIVE_FILE_PATH: str = 'database/data.db'
DB_CONNECTION_STRING: str = f'{DB_TYPE}+{DB_API}:///{DB_RELATIVE_FILE_PATH}'


def db_connect(create_metadata: bool = False, echo: bool = False) -> sqlalchemy.Engine | None:    

    # Create the SQLAlchemy engine and metadata (if it doesn't exist)
    engine: sqlalchemy.Engine = sqlalchemy.create_engine(DB_CONNECTION_STRING, echo=echo)
    if create_metadata:
        Base.metadata.create_all(bind=engine)

    return engine if isinstance(engine, sqlalchemy.Engine) else None


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