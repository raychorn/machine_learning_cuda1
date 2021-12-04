import os
import sys

from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

DB_URI = 'postgresql+psycopg2://securexdev:secureme2020@tp01-2066.web-service.org:49153/securex_assets'
engine = create_engine(DB_URI)

tables = engine.table_names()
print(tables)

from contextlib import contextmanager

from sqlalchemy.orm import scoped_session, sessionmaker

@contextmanager
def db_session(db_url):
    """ Creates a context with an open SQLAlchemy session.
    """
    engine = create_engine(db_url) # , convert_unicode=True
    connection = engine.connect()
    db_session = scoped_session(sessionmaker(autocommit=False, autoflush=True, bind=engine))
    yield db_session
    db_session.close()
    connection.close()


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


Base.metadata.create_all(engine)

metadata_obj = MetaData()
metadata_obj.reflect(bind=engine)

with db_session(DB_URI) as session:
    with open('{}/securex_assets_report.txt'.format(os.path.dirname(__file__)), 'w') as fOut:
        _tables = sorted([t for t in tables])
        for t in _tables:
            print(t)
            print(t, file=fOut)
            
            theTable = Table(t, metadata_obj, autoload_with=engine)
            
            print('BEGIN: theTable.columns', file=fOut)
            for c in theTable.columns:
                print('{} :: {}'.format(t, c), file=fOut)
            print('END!!! theTable.columns', file=fOut)
            
            #print(session.query(aTable).all())
            print('-'*80, file=fOut)
            print('\n', file=fOut)
            print()
        