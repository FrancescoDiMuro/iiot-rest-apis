from models import Base
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from sqlalchemy import text

engine: Engine = create_engine('sqlite+pysqlite:///data.db', echo=True)

with engine.connect() as connection:
    result = connection.execute(text('SELECT * FROM data'))
    print(result.first())

    # data_table = Table('data', MetaData(), autoload_with=engine)
    # print(data_table.metadata.tables)

# Base.metadata.tables['tags'].schema = 'main'

Base.metadata.create_all(bind=engine)

# Session = sessionmaker(bind=engine)

# with Session.begin() as session:   
#     session.add()
