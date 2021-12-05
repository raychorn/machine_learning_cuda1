import os
import sys

import json

import dotenv

import datetime

from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

fp_env = dotenv.find_dotenv()
print('fp_env: {}'.format(fp_env))
dotenv.load_dotenv(fp_env)

Base = declarative_base()

POSTGRES_DB_URI = os.environ.get('POSTGRES_DB_URI')

engine = create_engine(POSTGRES_DB_URI)

tables = engine.table_names()
print(tables)

from contextlib import contextmanager

from sqlalchemy.orm import scoped_session, sessionmaker

def isgoodipv4(s):
    pieces = s.split('.')
    if (len(pieces) != 4):
        return False
    try: 
        return all(0<=int(p)<256 for p in pieces)
    except ValueError:
        return False
    return False

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

interested_in = ['asset']

with db_session(POSTGRES_DB_URI) as session:
    with open('{}/securex_assets_report.txt'.format(os.path.dirname(__file__)), 'w') as fOut:
        _tables = sorted([t for t in tables])
        for t in _tables:
            if (not t in interested_in):
                continue
            print('{} -> {}'.format(t, type(t)))
            print(t, file=fOut)
            
            theTable = Table(t, metadata_obj, autoload_with=engine)
            
            print('BEGIN: theTable.columns', file=fOut)
            for c in theTable.columns:
                msg = '{} :: {}'.format(t, c)
                print(msg)
                print(msg, file=fOut)
            print('END!!! theTable.columns', file=fOut)
            
            items = session.query(theTable).all()
            fpathJson = '{}/securex_postgres_{}.json'.format(os.path.dirname(__file__), t)
            with open(fpathJson, 'w') as fJson1:
                _items = [list(r) for r in items]
                _items = [dict(zip(theTable.columns.keys(), r)) for r in _items]
                for row in _items:
                    for k,v in row.items():
                        if (isinstance(v, datetime.datetime)):
                            row[k] = v.isoformat()
                    ip_addr = row.get('hostname', '')
                    if (isgoodipv4(ip_addr)):
                        result = session.query(theTable).filter(theTable.c.hostname=='{}'.format(ip_addr)).one()
                        if (type(result).__name__ == 'Row'):
                            _result = dict(zip(theTable.columns.keys(), result))
                            assert _result.get('hostname') == ip_addr, '{} != {}'.format(_result.get('hostname'), ip_addr)
                json.dump(_items, fJson1, indent=4)
            print('{} items written to {}'.format(len(items), fpathJson))
            msg = '-'*80
            print(msg)
            print(msg, file=fOut)
            print('\n', file=fOut)
            print()
        