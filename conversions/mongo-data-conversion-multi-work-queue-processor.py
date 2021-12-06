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

is_verbose = not any([str(arg).find(__verbose_command_line_option__) > -1 for arg in sys.argv])

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

from binning import binner

from utils2 import typeName
from whois import ip_address_owner

from vyperlogix.mongo.database import docs_generator
from vyperlogix.mongo.database import get_pipeline_for

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

source_db_name = os.environ.get('MONGO_WORK_QUEUE_DATA_DB')
source_coll_name = os.environ.get('MONGO_WORK_QUEUE_COL')

dest_db_name = os.environ.get('MONGO_DEST_DATA_DB')
dest_binned_coll_name = os.environ.get('MONGO_DEST_DATA_BINNED_COL')
dest_data_coll_name = os.environ.get('MONGO_DEST_DATA_COL')

dest_stats_coll_name = os.environ.get('MONGO_DEST_DATA_STATS_COL')

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

print('source_db_name: {}'.format(source_db_name))
print('source_coll_name: {}'.format(source_coll_name))
source_coll = db_collection(client, source_db_name, source_coll_name)

dest_data_coll = db_collection(client, dest_db_name, dest_data_coll_name)
dest_binned_coll = db_collection(client, dest_db_name, dest_binned_coll_name)
dest_stats_coll = db_collection(client, dest_db_name, dest_stats_coll_name)

n_cores = multiprocessing.cpu_count() - 1

yn = input("Please approve {},{},{} delete all. (y/n)".format(dest_data_coll.full_name, dest_binned_coll.full_name, dest_stats_coll.full_name))
if (str(yn.upper()) == 'Y'):
    dest_data_coll.delete_many({})
    dest_binned_coll.delete_many({})
    dest_stats_coll.delete_many({})

msg = 'BEGIN: Count scheduled bins in {}'.format(source_coll_name)
print(msg)
logger.info(msg)

with timer.Timer() as timer1:
    num_bins = source_coll.count_documents({})
msg = 'END!!! Count scheduled bins in {} :: num_bins: {} in {:.2f} secs'.format(source_coll_name, num_bins, timer1.duration)
print(msg)
logger.info(msg)

collection_size = num_bins
batch_size = round(collection_size / n_cores)
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

__bin_count__ = 0

__is_running__ = True

def _aggregate(pipeline, db_name, coll_name):
    client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))
    cursor = db_coll(client, db_name, coll_name).aggregate(pipeline, allowDiskUse=True, maxTimeMS=12*3600*1000)

    _total = {'total': 0}
    try:
        for doc in cursor:
            _total['total'] = doc.get('total', -1)
    finally:
        client.close()
    return _total
    

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
    return _aggregate(pipeline, dest_db_name, dest_stats_coll_name)

def aggregate_bins_docs_total():
    pipeline = [
        {
            u"$project": {
                u"datasize": {
                    u"$size": u"$data"
                }
            }
        }, 
        {
            u"$group": {
                u"_id": None,
                u"total": {
                    u"$sum": u"$datasize"
                }
            }
        }
    ]
    return _aggregate(pipeline, dest_db_name, dest_coll_name)

def process_cursor(proc_id, source_db_name, source_coll_name, sort, criteria, projection, skip_n, limit_n, logger):
    assert proc_id is not None, 'proc_id is required'
    assert source_db_name is not None, 'source_db_name is required'
    assert source_coll_name is not None, 'source_coll_name is required'
    assert sort is not None, 'sort is required'
    assert criteria is not None, 'criteria is required'
    assert projection is not None, 'projection is required'
    assert skip_n is not None, 'skip_n is required'
    assert limit_n is not None, 'limit_n is required'
    assert logger is not None, 'logger is required'

    __stats__ = []

    client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))

    dest_binned = db_coll(client, dest_db_name, dest_binned_coll_name)
    dest_data = db_coll(client, dest_db_name, dest_data_coll_name)
    
    @bin_processor(bin_size=600, stats=__stats__, db=dest_binned, logger=logger)
    def step2_binner(data, db=None, bin_count=-1, bin_size=-1, stats={}, proc_id=-1, logger=None):
        if (logger):
            logger.info('step2_binner :: bin size: {}'.format(len(data)))
        if (isinstance(data, list)):
            pass
        start_date = data[0]['start']
        end_date = data[-1]['end']
        df_d = pd.DataFrame(data)

        df_d["date"] = start_date.date()
        df_d["hour"] = start_date.hour
        df_d = df_d.groupby(["dstport", "date", "hour"],  as_index=False).sum()
        
        #BPP
        df_d["bpp"] = df_d["bytes"]/df_d["packets"] 
        df_d["bpp_norm"] =  np.log(df_d['bpp'])
        df_d["bpp_zscore+log"] = (df_d["bpp_norm"] - df_d["bpp_norm"].mean()) / df_d["bpp_norm"].std()   
        
        df_d[["date", "hour"]] = df_d[["date", "hour"]].astype(str)
        dd = df_d["date"]
        hh = df_d["hour"]
        bin_id = dd +" h-"+ hh + " b-" + str(bin_count)
        df_d["bin"] = bin_id
        df_d["start"] = start_date
        df_d["end"] = end_date
        
        if (isinstance(stats, list)):
            bucket = {'bin': bin_id, 'start': start_date, 'end': end_date, 'dstport-bin': df_d.to_dict(orient='records')}
            stats.append(bucket)


    msg = 'BEGIN: process_cursor ({}) :: skip_n={}, limit_n={}'.format(proc_id, skip_n, limit_n)
    if (logger):
        logger.info(msg)
    print(msg)

    try:
        with timer.Timer() as timer3:
            n_blk = int(limit_n/100)
            doc_cnt = 0
            try:
                pipeline = get_pipeline_for(criteria, projection, skip_n, limit_n, sort)
                db = db_coll(client, source_db_name, source_coll_name)
                cursor = db.aggregate(pipeline, allowDiskUse=True, maxTimeMS=12*3600*1000)
                for doc in cursor:
                    doc_cnt += 1
                    if ((doc_cnt % n_blk) == 0):
                        num_cells = (doc_cnt / limit_n)
                        print('{}{}::({:2f})'.format(repeat_char(' ', 2-len(str(proc_id))), proc_id, num_cells), end='\n')
                    #<do your magic>
                    _data = doc.get('data', [])
                    for dd in _data:
                        step2_binner(dd, proc_id=proc_id)
                        dd['uuid'] = doc.get('uuid')
                        dd['start_month'] = dd.get('start').month
                        dd['start_day'] = dd.get('start').day
                        dd['start_year'] = dd.get('start').year
                        dd['start_hour'] = dd.get('start').hour
                        dd['start_minute'] = dd.get('start').minute
                        dd['end_month'] = dd.get('end').month
                        dd['end_day'] = dd.get('end').day
                        dd['end_year'] = dd.get('end').year
                        dd['end_hour'] = dd.get('end').hour
                        dd['end_minute'] = dd.get('end').minute
                    dest_data.insert_many(_data)

                    l = len(__stats__)
                    if (l > 0):
                        for stat in __stats__:
                            dstport_bin = stat.get('dstport-bin', [])
                            for _bin in dstport_bin:
                                _bin['uuid'] = doc.get('uuid')
                                _bin['start_month'] = _bin.get('start').month
                                _bin['start_day'] = _bin.get('start').day
                                _bin['start_year'] = _bin.get('start').year
                                _bin['start_hour'] = _bin.get('start').hour
                                _bin['start_minute'] = _bin.get('start').minute
                                _bin['end_month'] = _bin.get('end').month
                                _bin['end_day'] = _bin.get('end').day
                                _bin['end_year'] = _bin.get('end').year
                                _bin['end_hour'] = _bin.get('end').hour
                                _bin['end_minute'] = _bin.get('end').minute
                            dest_binned.insert_many(dstport_bin)
                        del __stats__[:]
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

    msg = 'END: process_cursor {} :: completed: doc_cnt={}, skip_n={}, limit_n={}{}'.format(proc_id, doc_cnt, skip_n, limit_n, _msg)
    if (logger):
        logger.info(msg)
    print(msg)

print('bin_collector :: started')

__bin_size__ = 600

__sort = {}
projection = {}

with timer.Timer() as timer2:
    try:
        _num_events = 1
        events = []
        if (1):
            msg = 'bin_collector :: creating processes.'
            logger.info(msg)
            print(msg)
            processes = [ multiprocessing.Process(target=process_cursor, args=(_i, source_db_name, source_coll_name, __sort, criteria, projection, skip_n, batch_size, logger)) for _i,skip_n in enumerate(skips)]

            for process in processes:
                process.start()

            for process in processes:
                process.join()

        msg = 'bin_collector :: master records {}: {}'.format('created' if (len(events) > 0) else 'has', _num_events)
        logger.info(msg)
        print(msg)

    except Exception as e:
        logger.error("Error in step2_binner", exc_info=True)

msg = 'Bin Collector :: {:.2f} secs'.format(timer2.duration)
print(msg)
logger.info(msg)

logger.info('Done.')

sys.exit()
