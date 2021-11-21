import os
import sys

import time
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

import pymongo
from pymongo import ReturnDocument
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

from vyperlogix.threads import pooled
from vyperlogix.decorators import executor

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

dest_master_coll_name = os.environ.get('MONGO_WORK_QUEUE_MASTER_COL')

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
    assert is_really_something_with_stuff(dest_coll_name, str), 'Cannot continue without the dest_coll_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_DEST_DATA_COL.", exc_info=True)
    sys.exit()

try:
    assert is_really_something_with_stuff(dest_master_coll_name, str), 'Cannot continue without the dest_master_coll_name.'
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

def doc_cleaner(doc, ignores=[], normalize=[]):
    ignores = ignores if (isinstance(ignores, list)) else []
    for k in normalize:
        if (k in doc):
            doc[k] = str(doc[k])
    return {k:int(v) if (str(v).isdigit() or str(v).isdecimal()) else v for k,v in doc.items() if (k not in ignores)}

def modify_criteria(criteria=None, additional=None):
    items = criteria.get('$and', [])
    items.append(additional)
    return criteria

dest_coll = db_collection(client, dest_db_name, dest_coll_name)

master_coll = db_collection(client, dest_db_name, dest_master_coll_name)

__stats__ = []

__bin_count__ = 0

__is_running__ = True

def callback(*args, **kwargs):
    global __is_running__
    msg = 'bin_collector :: confirmed shutdown.'
    logger.info(msg)
    print(msg)
    __is_running__ = False

_executor = pooled.BoundedExecutor(2, 5, callback=callback)

@executor.threaded(_executor)
def worker(db=None, chunk_size=100, logger=None):
    assert db is not None, 'db is required'
    assert chunk_size is not None, 'chunk_size is required'
    assert logger is not None, 'logger is required'
    __is__ = False
    while __is_running__ and (not Q.empty()) and (not __is__):
        try:
            if (not Q.empty()):
                docs = Q.get_nowait()
                if (docs):
                    if (len(docs) > 0):
                        msg = 'bin_collector :: scheduled for binning: {}-{}'.format(docs[0].get('data', [])[0].get('start'), docs[0].get('data', [])[-1].get('start'))
                        logger.info(msg)
                        print(msg)
                        db.insert_many(docs)
                    else:
                        msg = 'bin_collector :: shutting down due to end of data.'
                        logger.info(msg)
                        print(msg)
                        __is__ = True
                        break
                else:
                    print('bin_collector :: no docs to process, so sleeping.')
                    time.sleep(1)
            else:
                print('bin_collector :: Q is empty, so sleeping.')
                time.sleep(1)
        except Exception as e:
            logger.error(str(e), exc_info=True)
            break
    return __is__

#worker(db=dest_coll, chunk_size=100, logger=logger)

print('bin_collector :: started')

__bin_size__ = 600

@bin_processor(bin_size=__bin_size__, chunk_size=10, stats=__stats__, db=dest_coll, logger=logger)
def step2_binner(data, bin_count, bin_size, stats=[], db=None, chunk_size=-1, logger=None):
    global __bin_count__
    __bin = {}
    __bin_count__ += 1
    __bin['bin_num'] = __bin_count__
    __bin['bin_count'] = bin_count
    __bin['bin_size'] = bin_size
    __bin['uuid'] = str(uuid.uuid4())
    __bin['data'] = [doc_cleaner(doc, normalize=['_id']) for doc in data]
    stats.append(__bin)
    l = len(stats)
    if (l > chunk_size):
        db.insert_many(stats)
        msg = 'bin_collector :: scheduled for binning: {} bins, {}-{}'.format(l, data[0].get('start'), data[-1].get('start'))
        logger.info(msg)
        print(msg)
        del stats[:]

limit = nlimit = min(100000, num_events)

__sort = {'start': 1}
projection = {'start': 1, 'end': 1, 'dstport': 1}

__sort2 = {'num': 1}

with timer.Timer() as timer2:
    try:
        _num_events = 1
        events = []
        for doc in docs_generator(db_collection(client, source_db_name, source_coll_name), sort=__sort, criteria={}, projection=projection, skip=0, limit=limit, nlimit=nlimit, maxTimeMS=12*60*60*1000, logger=logger):
            events.append({'num': _num_events, 'doc': doc, 'selected': False})
            if (len(events) >= 100):
                master_coll.insert_many(events)
                msg = 'bin_collector :: created master record: {}-{}'.format(events[0].get('num'), events[-1].get('num'))
                logger.info(msg)
                print(msg)
                del events[:]
            _num_events += 1

        msg = 'bin_collector :: master records created: {}'.format(_num_events)
        logger.info(msg)
        print(msg)

        is_verbose = False
        
        seq_num = 1
        while (1):
            _doc = master_coll.find_one_and_update({ "num": seq_num, "selected": False }, { "$set": { "selected": True } }, return_document=ReturnDocument.AFTER, sort=[('_id', pymongo.ASCENDING)])
            if (is_verbose):
                msg = 'bin_collector :: choose master record: num --> {}'.format(_doc.get('num', -1))
                logger.info(msg)
                print(msg)
            if (_doc):
                st = _doc.get('doc', {}).get('start', -1)
                tt = st + dt.timedelta(seconds=__bin_size__)
                _docs = master_coll.find({ "doc.start": {"$lte": tt }, "selected": False }, { "doc": 1, "selected" : 1, "num": 1 }, sort=[('doc.start', pymongo.ASCENDING)])
                newvalues = { "$set": { "selected": True } }
                for d in _docs:
                    step2_binner(doc_cleaner(d.get('doc'), normalize=['_id']))
                    master_coll.update_one(d, newvalues)
                    seq_num = d.get('num', seq_num)
                seq_num += 1
            else:
                msg = 'bin_collector :: nothing to choose from master records, nothing more to do.'
                logger.info(msg)
                print(msg)
                break
            
        l = len(__stats__)
        if (l > 0):
            dest_coll.insert_many(__stats__)
            msg = 'bin_collector :: scheduled for binning: {} bins, {}-{}'.format(l, __stats__[0].get('start'), __stats__[-1].get('start'))
            logger.info(msg)
            print(msg)
            del __stats__[:]
    except Exception as e:
        logger.error("Error in step2_binner", exc_info=True)

msg = 'Bin Collector :: num_events: {} in {:.2f} secs'.format(_num_events, timer2.duration)
print(msg)
logger.info(msg)

#msg = 'bin collector :: __is_running__={}'.format(__is_running__)
#print(msg)
#logger.info(msg)

#executor.shutdown()

logger.info('Done.')

sys.exit()
