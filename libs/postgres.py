import os
import sys

import re

import json

import dotenv

import datetime

import traceback

from contextlib import contextmanager

from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import scoped_session, sessionmaker

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

from sqlalchemy.orm import class_mapper

@contextmanager
def db_session(db_url_or_engine):
    """ Creates a context with an open SQLAlchemy session.
    """
    engine = create_engine(db_url_or_engine) if isinstance(db_url_or_engine, str) else db_url_or_engine
    connection = engine.connect()
    db_session = scoped_session(sessionmaker(autocommit=False, autoflush=True, bind=engine))
    try:
        yield db_session
    except:
        db_session.rollback()
        raise
    finally:
        db_session.close()
        connection.close()

class Query(object):
    
    def __init__(self, table_name, colName, targetValue, envPath=None, logger=None):
        self.envPath = envPath
        self.logger = logger
        
        self.table_name = table_name
        self.colName = colName
        self.targetValue = targetValue
        
        if ((self.envPath is None) or (not isinstance(self.envPath, str)) or (not os.path.exists(self.envPath)) or (not os.path.isfile(self.envPath))):
            self.envPath = dotenv.find_dotenv()
        assert (self.envPath is not None) and (isinstance(self.envPath, str)) and (os.path.exists(self.envPath)) and (os.path.isfile(self.envPath)), 'envPath ({}) is not valid or cannot be found.'.format(self.envPath)
        dotenv.load_dotenv(self.envPath)

        self.Base = declarative_base()

        self.POSTGRES_DB_URI = os.environ.get('POSTGRES_DB_URI')
        self.engine = create_engine(self.POSTGRES_DB_URI)

        self.Base.metadata.create_all(self.engine)

        self.metadata_obj = MetaData()
        self.metadata_obj.reflect(bind=self.engine)

        self.Table = Table(self.table_name, self.metadata_obj, autoload_with=self.engine)

    def __call__(self, f):
        try:
            def wrapped_f(*args, **kwargs):
                with db_session(self.engine) as session:
                    kwargs['self'] = self
                    kwargs['session'] = session
                    return f(*args, **kwargs)
            return wrapped_f
        except Exception as ex:
            extype, ex, tb = sys.exc_info()
            formatted = traceback.format_exception_only(extype, ex)[-1]
            if (self.logger):
                self.logger.error(formatted)
            else:
                print(formatted)

def query_db(table_name, filter_by={}):
    '''
        filter_by = {
        "column1": "!1", # not equal to
        "column2": "1",   # equal to
        "column3": ">1",  # great to. etc...
        }
    '''
    def computed_operator(column, v):
        if re.match(r"^!", v):
            """__ne__"""
            val = re.sub(r"!", "", v)
            return column.__ne__(val)
        if re.match(r">(?!=)", v):
            """__gt__"""
            val = re.sub(r">(?!=)", "", v)
            return column.__gt__(val)
        if re.match(r"<(?!=)", v):
            """__lt__"""
            val = re.sub(r"<(?!=)", "", v)
            return column.__lt__(val)
        if re.match(r">=", v):
            """__ge__"""
            val = re.sub(r">=", "", v)
            return column.__ge__(val)
        if re.match(r"<=", v):
            """__le__"""
            val = re.sub(r"<=", "", v)
            return column.__le__(val)
        if re.match(r"(\w*),(\w*)", v):
            """between"""
            a, b = re.split(r",", v)
            return column.between(a, b)
        """ default __eq__ """
        return column.__eq__(v)

    
    fp_env = dotenv.find_dotenv()
    print('fp_env: {}'.format(fp_env))
    dotenv.load_dotenv(fp_env)

    Base = declarative_base()

    POSTGRES_DB_URI = os.environ.get('POSTGRES_DB_URI')
    engine = create_engine(POSTGRES_DB_URI)

    Base.metadata.create_all(engine)

    metadata_obj = MetaData()
    metadata_obj.reflect(bind=engine)

    with db_session(engine) as session:
        theTable = Table(table_name, metadata_obj, autoload_with=engine)
        
        query = session.query()
        filters = []
        theCols = theTable.columns
        mapped_cols = {c.name:c for c in theTable.columns}
        for k, v in filter_by.items():
            if (not k in list(mapped_cols.keys())):
                continue
            filters.append(computed_operator(mapped_cols.get(k), "{}".format(v)))
        query = query.filter(*filters)
        result = query.all()
        
        if (type(result).__name__ == 'Row'):
            _result = dict(zip(theTable.columns.keys(), result))
            print('_result: {}'.format(_result))

def isgoodipv4(s):
    pieces = s.split('.')
    if (len(pieces) != 4):
        return False
    try: 
        return all(0<=int(p)<256 for p in pieces)
    except ValueError:
        return False
    return False

if (__name__ == '__main__'):
    @Query('asset', 'hostname', '172.31.60.14')
    def foo(self=None, session=None):
        _result = None
        result = session.query(self.Table).filter(self.Table.c.hostname=='{}'.format(self.targetValue)).one()
        if (type(result).__name__ == 'Row'):
            _result = dict(zip(self.Table.columns.keys(), result))
        print('results: {}'.format(_result))
    foo()
    print()