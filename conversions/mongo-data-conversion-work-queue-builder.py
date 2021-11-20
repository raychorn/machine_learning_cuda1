import os
import sys
import json

import uuid

import traceback

import types
import dotenv

import shutil

import socket
import logging

import datetime as dt
from datetime import datetime

from itertools import chain

from expandvars import expandvars

import numpy as np
import pandas as pd

from binner import processor as bin_processor

############################################################################
from logging.handlers import RotatingFileHandler

production_token = 'production'
development_token = 'development'

is_running_production = lambda : (socket.gethostname().find('raychorn') == -1)

something_greater_than_zero = lambda s:(s > 0)

is_really_something = lambda s,t:(s is not None) and ( (callable(t) and (not isinstance(t, types.FunctionType)) and isinstance(s, t)) or (callable(t) and (isinstance(t, types.FunctionType)) and t(s)) )

is_really_something_with_stuff = lambda s,t:is_really_something(s,t) and (len(s) > 0)

default_timestamp = lambda t:t.isoformat().replace(':', '').replace('-','').split('.')[0]

__verbose_command_line_option__ = '--verbose'

is_verbose = any([str(arg).find(__verbose_command_line_option__) > -1 for arg in sys.argv])

def get_logger(fpath=__file__, product='scheduler', logPath='logs', is_running_production=is_running_production()):
    def get_stream_handler(streamformat="%(asctime)s:%(levelname)s -> %(message)s"):
        stream = logging.StreamHandler()
        stream.setLevel(logging.INFO if (not is_running_production) else logging.DEBUG)
        stream.setFormatter(logging.Formatter(streamformat))
        return stream

        
    def setup_rotating_file_handler(logname, logfile, max_bytes, backup_count):
        assert is_really_something(backup_count, something_greater_than_zero), 'Missing backup_count?'
        assert is_really_something(max_bytes, something_greater_than_zero), 'Missing max_bytes?'
        ch = RotatingFileHandler(logfile, 'a', max_bytes, backup_count)
        l = logging.getLogger(logname)
        l.addHandler(ch)
        return l

    _fpath = os.path.dirname(__file__) if (os.path.isfile(fpath)) else fpath
    assert os.path.exists(_fpath) and os.path.isdir(_fpath), 'Cannot create a logger without a directory for the logs and "{}" is unacceptable. Please fix.'.format(_fpath)
    
    base_filename = os.path.splitext(os.path.basename(fpath))[0]

    log_filename = '{}{}{}{}{}{}{}{}{}_{}.log'.format(logPath, os.sep, base_filename, os.sep, production_token if (is_running_production) else development_token, os.sep, product, os.sep, base_filename, default_timestamp(datetime.utcnow()))
    log_filename = os.sep.join([os.path.dirname(fpath), log_filename])

    if (not is_running_production):
        logs_base_dir = os.sep.join([os.path.dirname(fpath), logPath])
        if (os.path.exists(logs_base_dir) and os.path.isdir(logs_base_dir)):
            shutil.rmtree(logs_base_dir)
    
    if not os.path.exists(os.path.dirname(log_filename)):
        os.makedirs(os.path.dirname(log_filename))

    if (os.path.exists(log_filename)):
        os.remove(log_filename)

    log_format = ('[%(asctime)s] %(levelname)-8s %(name)-12s -> %(message)s')
    logging.basicConfig(
        level=logging.DEBUG if (not is_running_production) else logging.INFO,
        format=log_format,
        filename=(log_filename),
    )

    logger = setup_rotating_file_handler(base_filename, log_filename, (1024*1024), 10)
    if (is_verbose):
        logger.addHandler(get_stream_handler())
    
    return logger

logger = get_logger()

from pymongo.mongo_client import MongoClient

fp_env = dotenv.find_dotenv()
logger.info('fp_env: {}'.format(fp_env))
dotenv.load_dotenv(fp_env)

is_not_none = lambda s:(s is not None)

def get_mongo_client(mongouri=None, db_name=None, username=None, password=None, authMechanism=None):
    if (is_not_none(authMechanism)):
        assert is_not_none(username), 'Cannot continue without a username ({}).'.format(username)
        assert is_not_none(password), 'Cannot continue without a password ({}).'.format(password)
    assert is_not_none(db_name), 'Cannot continue without a db_name ({}).'.format(db_name)
    assert is_not_none(mongouri), 'Cannot continue without a mongouri ({}).'.format(mongouri)
    return  MongoClient(mongouri, username=username, password=password, authSource=db_name, authMechanism=authMechanism)


def isgoodipv4(s):
    pieces = s.split('.')
    if (len(pieces) != 4):
        return False
    try: 
        return all(0<=int(p)<256 for p in pieces)
    except ValueError:
        return False
    return False

############################################################################

docker_libs = os.environ.get('docker_libs')
if (is_really_something_with_stuff(docker_libs, str)):
    f_libs = docker_libs
else:
    f_libs = os.environ.get('libs')

f_libs = f_libs.split(';')
for f in f_libs:
    if (os.path.exists(f) and os.path.isdir(f)):
        if (f not in sys.path):
            sys.path.insert(0, f)

from utils2 import typeName
from whois import ip_address_owner

from database import docs_generator

from vyperlogix.contexts import timer

__env__ = {}
__literals__ = os.environ.get('LITERALS', [])
__literals__ = [__literals__] if (not isinstance(__literals__, list)) else __literals__
for k,v in os.environ.items():
    if (k.find('MONGO_') > -1):
        __env__[k] = expandvars(v) if (k not in __literals__) else v

__env__['MONGO_INITDB_DATABASE'] = os.environ.get('MONGO_INITDB_DATABASE')
__env__['MONGO_URI'] = os.environ.get('MONGO_URI')
__env__['MONGO_INITDB_USERNAME'] = os.environ.get("MONGO_INITDB_ROOT_USERNAME")
__env__['MONGO_INITDB_PASSWORD'] = os.environ.get("MONGO_INITDB_ROOT_PASSWORD")
__env__['MONGO_AUTH_MECHANISM'] ='SCRAM-SHA-1'

source_db_name = os.environ.get('MONGO_SOURCE_DATA_DB')
source_coll_name = os.environ.get('MONGO_SOURCE_DATA_COL')

dest_db_name = os.environ.get('MONGO_WORK_QUEUE_DATA_DB')
dest_coll_name = os.environ.get('MONGO_WORK_QUEUE_COL')

try:
    client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))
except:
    sys.exit()

try:
    assert is_really_something_with_stuff(source_db_name, str), 'Cannot continue without the db_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_SOURCE_DATA_DB.", exc_info=True)
    sys.exit()

try:
    assert is_really_something_with_stuff(source_coll_name, str), 'Cannot continue without the coll_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_SOURCE_DATA_COL.", exc_info=True)
    sys.exit()

try:
    assert is_really_something_with_stuff(dest_db_name, str), 'Cannot continue without the db_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_DEST_DATA_DB.", exc_info=True)
    sys.exit()

try:
    assert is_really_something_with_stuff(dest_coll_name, str), 'Cannot continue without the coll_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_DEST_DATA_COL.", exc_info=True)
    sys.exit()

logger.info(str(client))

db = lambda cl,n:cl.get_database(n)
db_collection = lambda cl,n,c:db(cl,n).get_collection(c)

criteria = {'$and': [{'action': {'$ne': 'REJECT'}}, {'srcaddr': {'$ne': "-"}, 'dstaddr': {'$ne': "-"}}, {'srcport': {'$ne': "0"}, 'dstport': {'$ne': "0"}}]}

#projection = {'srcaddr': 1, 'dstaddr': 1, 'srcport': 1, 'dstport': 1, 'protocol': 1, 'packets': 1, 'bytes': 1, 'start': 1, 'end': 1, 'log-status': 1, '__metadata__.srcaddr.owner': 1, '__metadata__.dstaddr.owner': 1, '__dataset__': 1, '__dataset_index__': 1}

with timer.Timer() as timer1:
    num_events = db_collection(client, source_db_name, source_coll_name).count_documents({})
msg = 'Data read :: num_events: {} in {:.2f} secs'.format(num_events, timer1.duration)
print(msg)
logger.info(msg)

def invert_dict(d, dest_dict=None):
    for k,v in d.items():
        v = v if (isinstance(v, list)) else [v]
        for name in v:
            bucket = dest_dict.get(name, [])
            bucket.append(k)
            dest_dict[name] = list(set(bucket))

def doc_cleaner(doc, ignores=[]):
    ignores = ignores if (isinstance(ignores, list)) else []
    return {k:int(v) if (str(v).isdigit() or str(v).isdecimal()) else v for k,v in doc.items() if (k not in ignores)}

def modify_criteria(criteria=None, additional=None):
    items = criteria.get('$and', [])
    items.append(additional)
    return criteria

dest_coll = db_collection(client, dest_db_name, dest_coll_name)

@bin_processor(bin_size=600, chunk_size=100, stats=None, db=db_collection(client, dest_db_name, dest_coll_name), logger=logger)
def step2_binner(data, bin_count, bin_size, stats={}, db=None, chunk_size=-1, logger=None):
    __bin = {}
    __bin['id'] = str(uuid.uuid4())
    __bin['data'] = [doc_cleaner(doc) for doc in data]
    write_to_mongoDB( [__bin], db, chunk_size=chunk_size, logger=logger)
    msg = 'bin_collector :: scheduled for binning: {}-{}'.format(data[0].get('start'), data[-1].get('start'))
    logger.info(msg)
    print(msg)

def write_to_mongoDB( recs, db_collection, chunk_size=100, logger=None):
    my_list = recs if (isinstance(recs, list)) else [recs]
    l =  len(my_list)
    _range = range(l)
    steps = _range[chunk_size::chunk_size]

    i = 0
    t_chunks = 0
    for j in steps:
        chunk = my_list[i:j]
        msg = '{}:{} -> {}'.format(i, j, len(chunk))
        if (logger):
            logger.info(msg)
        print(msg)
        db_collection.insert_many(chunk)
        i = j
        t_chunks += len(chunk)

    msg = '{}:{} -> {}'.format(i, l, len(my_list[i:]))
    if (logger):
        logger.info(msg)
    print(msg)
    if (i < l):
        chunk = my_list[i:]
        msg = '{}:{} -> {}'.format(i, len(my_list), len(chunk))
        if (logger):
            logger.info(msg)
        print(msg)
        db_collection.insert_many(chunk)
        t_chunks += len(chunk)

    assert t_chunks == l, 't_chunks != l, there must be an error in the process of committing the data to the db.'
    msg = 'Done, {} total, {} expected'.format(t_chunks, l)
    if (logger):
        logger.info(msg)
    print(msg)
    return

limit = nlimit = min(100000, num_events)

__sort = {'start': 1}
projection = {'start': 1, 'end': 1, 'dstport': 1}

with timer.Timer() as timer2:
    try:
        for doc in docs_generator(db_collection(client, source_db_name, source_coll_name), sort=__sort, criteria={}, projection=projection, skip=0, limit=limit, nlimit=nlimit, maxTimeMS=12*60*60*1000, logger=logger):
            step2_binner(doc_cleaner(doc))
    except Exception as e:
        logger.error("Error in step2_binner", exc_info=True)

msg = 'Bin Collector :: num_events: {} in {:.2f} secs'.format(num_events, timer2.duration)
print(msg)
logger.info(msg)

logger.info('Done.')

sys.exit()