import os
import sys

import math

import time
import json

import uuid

from queue import Queue

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

import multiprocessing

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
from database import get_pipeline_for

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

dest_stats_coll_name = os.environ.get('MONGO_WORK_QUEUE_STATS_COL')

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
    assert is_really_something_with_stuff(dest_stats_coll_name, str), 'Cannot continue without the dest_stats_coll_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_WORK_QUEUE_STATS_COL.", exc_info=True)
    sys.exit()


logger.info(str(client))

db = lambda cl,n:cl.get_database(n)
db_collection = lambda cl,n,c:db(cl,n).get_collection(c)

db_coll = lambda cl,n,c:cl.get_database(n).get_collection(c)

repeat_char = lambda c,n:''.join([c for i in range(n)])

criteria = {'$and': [{'action': {'$ne': 'REJECT'}}, {'srcaddr': {'$ne': "-"}, 'dstaddr': {'$ne': "-"}}, {'srcport': {'$ne': "0"}, 'dstport': {'$ne': "0"}}]}

#projection = {'srcaddr': 1, 'dstaddr': 1, 'srcport': 1, 'dstport': 1, 'protocol': 1, 'packets': 1, 'bytes': 1, 'start': 1, 'end': 1, 'log-status': 1, '__metadata__.srcaddr.owner': 1, '__metadata__.dstaddr.owner': 1, '__dataset__': 1, '__dataset_index__': 1}

source_coll = db_collection(client, source_db_name, source_coll_name)

dest_coll = db_collection(client, dest_db_name, dest_coll_name)

dest_stats_coll = db_collection(client, dest_db_name, dest_stats_coll_name)

n_cores = multiprocessing.cpu_count() / 2

dest_stats_coll.delete_many({})

msg = 'BEGIN: Count docs in {}'.format(source_coll_name)
print(msg)
logger.info(msg)

with timer.Timer() as timer1:
    num_events = source_coll.count_documents({})
msg = 'END!!! Count docs in {} :: num_events: {} in {:.2f} secs'.format(source_coll_name, num_events, timer1.duration)
print(msg)
logger.info(msg)

collection_size = num_events
batch_size = round(collection_size / n_cores)
_skips = [i for i in range(0, collection_size, int(batch_size))]
skips = range(0, collection_size, int(batch_size))

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

__stats__ = []

__process_stats__ = []

__bin_count__ = 0

__is_running__ = True

def aggregate_docs_count():
    pipeline = [
        {
            u"$project": {
                u"proc_id": 1.0,
                u"doc_cnt": 1.0
            }
        }, 
        {
            u"$match": {
                u"proc_id": {
                    u"$exists": True
                }
            }
        }, 
        {
            u"$group": {
                u"_id": None,
                u"total": {
                    u"$sum": u"$doc_cnt"
                }
            }
        }
    ]

    client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))
    cursor = db_coll(client, dest_db_name, dest_stats_coll_name).aggregate(pipeline, allowDiskUse=True, maxTimeMS=12*3600*1000)

    _total = {'total': 0}
    try:
        for doc in cursor:
            _total['total'] = doc.get('total', -1)
    finally:
        client.close()
    return _total

def process_cursor(proc_id, source_db_name, source_coll_name, sort, criteria, projection, skip_n, limit_n, nlimit, process_stats, logger):
    global __process_stats__
    assert proc_id is not None, 'proc_id is required'
    assert source_db_name is not None, 'source_db_name is required'
    assert source_coll_name is not None, 'source_coll_name is required'
    assert sort is not None, 'sort is required'
    assert criteria is not None, 'criteria is required'
    assert projection is not None, 'projection is required'
    assert skip_n is not None, 'skip_n is required'
    assert limit_n is not None, 'limit_n is required'
    assert nlimit is not None, 'nlimit is required'
    assert process_stats is not None, 'process_stats is required'
    assert logger is not None, 'logger is required'

    msg = 'BEGIN: process_cursor ({}) :: skip_n={}, limit_n={}, nlimit={}'.format(proc_id, skip_n, limit_n, nlimit)
    if (logger):
        logger.info(msg)
    print(msg)

    try:
        client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))
        with timer.Timer() as timer3:
            n_blk = int(limit_n/100)
            doc_cnt = 0
            try:
                pipeline = get_pipeline_for(criteria, projection, skip_n, limit_n, sort)
                cursor = db_coll(client, source_db_name, source_coll_name).aggregate(pipeline, allowDiskUse=True, maxTimeMS=12*3600*1000)
                for doc in cursor:
                    doc_cnt += 1
                    if ((doc_cnt % n_blk) == 0):
                        num_cells = (doc_cnt / limit_n)
                        #print('{}{}::({}|{})'.format(repeat_char(' ', 2-len(str(proc_id))), proc_id, repeat_char('.', num_cells), repeat_char('.', 99-num_cells)))
                        print('{}{}::({:2f})'.format(repeat_char(' ', 2-len(str(proc_id))), proc_id, num_cells), end='\n')
                    #<do your magic> 
                    # for example:
                    #result = your_function(doc['your_field'] # do some processing on each document
                    # update that document by adding the result into a new field
                    #collection.update_one({'_id': doc['_id']}, {'$set': {'<new_field_eg>': result} })
            except Exception as e:
                if (logger):
                    logger.error("Error in step2_binner", exc_info=True)
                print('Error in step2_binner!')

        _msg = ' in {:.2f} secs'.format(timer3.duration)

        dest_stats_coll = db_coll(client, dest_db_name, dest_stats_coll_name)
        s = {'proc_id':proc_id, 'doc_cnt':doc_cnt, 'duration':timer3.duration}
        dest_stats_coll.insert_one(s)
    finally:
        client.close()

    msg = 'END: process_cursor {} :: completed: doc_cnt={}, skip_n={}, limit_n={}, nlimit={}{}'.format(proc_id, doc_cnt, skip_n, limit_n, nlimit, _msg)
    if (logger):
        logger.info(msg)
    print(msg)

print('bin_collector :: started')

__bin_size__ = 600

@bin_processor(bin_size=__bin_size__, chunk_size=1, stats=__stats__, db=dest_coll, logger=logger)
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
    if (l >= chunk_size):
        for aBin in stats:
            db.insert_one(aBin)
        msg = 'bin_collector :: scheduled for binning: {} bins, {}-{}'.format(l, stats[0].get('data', [])[0].get('start'), stats[0].get('data', [])[-1].get('start'))
        logger.info(msg)
        print(msg)
        del stats[:]

limit = nlimit = max(100000, num_events) # - num_master_events

__sort = {'start': pymongo.ASCENDING}
projection = {'start': 1, 'end': 1, 'dstport': 1}

with timer.Timer() as timer2:
    try:
        _num_events = 1
        events = []
        if (1):
            msg = 'bin_collector :: creating processes.'
            logger.info(msg)
            print(msg)
            processes = [ multiprocessing.Process(target=process_cursor, args=(_i, source_db_name, source_coll_name, __sort, criteria, projection, skip_n, batch_size, skip_n+batch_size, {}, logger)) for _i,skip_n in enumerate(skips)]

            for process in processes:
                process.start()

            for process in processes:
                process.join()

        msg = 'bin_collector :: master records {}: {}'.format('created' if (len(events) > 0) else 'has', _num_events)
        logger.info(msg)
        print(msg)

        is_verbose = False
        
        if (0):
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
                    _docs = master_coll.find({ "$and": [ { "doc.start": {"$gte": st } }, { "doc.start": {"$lte": tt } }, {"selected": False} ] }, sort=[('doc.start', pymongo.ASCENDING)])
                    newvalues = { "$set": { "selected": True } }
                    _cnt = 0
                    for d in _docs:
                        step2_binner(doc_cleaner(d.get('doc'), normalize=['_id']))
                        seq_num = d.get('num', seq_num)
                        _cnt += 1
                    _d = master_coll.update_many({ "$and": [ { "doc.start": {"$gte": st } }, { "doc.start": {"$lte": tt } }, {"selected": False} ] }, { "$set": { "selected": True } } )
                    seq_num += 1
                else:
                    msg = 'bin_collector :: nothing to choose from master records, nothing more to do.'
                    logger.info(msg)
                    print(msg)
                    break
                
            l = len(__stats__)
            if (l > 0):
                for aBin in __stats__:
                    db.insert_one(aBin)
                msg = 'bin_collector :: scheduled for binning: {} bins, {}-{}'.format(l, __stats__[0].get('data', [])[0].get('start'), __stats__[0].get('data', [])[-1].get('start'))
                logger.info(msg)
                print(msg)
                del __stats__[:]
    except Exception as e:
        logger.error("Error in step2_binner", exc_info=True)

total_docs_count = aggregate_docs_count()
assert 'total' in list(total_docs_count.keys()), 'total not found in total_docs_count'
assert total_docs_count.get('total', -1) == num_events, 'total_docs_count ({}) != num_events ({}), diff={}'.format(total_docs_count.get('total', -1), num_events, num_events - total_docs_count.get('total', -1))

dest_stats_coll.insert_one({'name':'bin_collector', 'doc_cnt':num_events, 'total': total_docs_count.get('total', -1), 'duration':timer2.duration})

msg = 'Bin Collector :: num_events: {} in {:.2f} secs'.format(_num_events, timer2.duration)
print(msg)
logger.info(msg)

logger.info('Done.')

sys.exit()
