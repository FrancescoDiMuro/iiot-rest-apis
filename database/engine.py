from models import Base
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


engine: Engine = create_engine('sqlite+pysqlite:///data.db', echo=True)

Base.metadata.create_all(bind=engine)


