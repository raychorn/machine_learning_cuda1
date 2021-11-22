import os
import sys
import json

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

is_running_production = lambda : (socket.gethostname() not in ['raychorn-alienware'])

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

from vyperlogix.decorators import threading

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

raw_data_source = os.environ.get('VPCFLOWLOGS_DATA_DIR')

dest_db_name = os.environ.get('MONGO_VPCFLOWLOGS_DATA_DB')
dest_coll_name = os.environ.get('MONGO_VPCFLOWLOGS_COL')

try:
    client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))
except:
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
    
try:
    assert is_really_something_with_stuff(raw_data_source, str), 'Cannot continue without the raw_data_source.'
    assert os.path.exists(raw_data_source) and os.path.isdir(raw_data_source), 'Cannot continue without the raw_data_source and is must be a directory.'
except Exception:
    logger.error("Fatal error with .env, check VPCFLOWLOGS_DATA_DIR.", exc_info=True)
    sys.exit()

logger.info(str(client))

db = lambda cl,n:cl.get_database(n)
db_collection = lambda cl,n,c:db(cl,n).get_collection(c)

dest_coll = db_collection(client, dest_db_name, dest_coll_name)
dest_coll.delete_many({})

fname_cols = ['account', 'bucket', 'region', 'name', 'timestamp', 'fname']

def iterate_directory(root):
    for subdir, dirs, files in os.walk(root):
        for file in files:
            fp = os.path.join(subdir, file)
            logger.info('{} :: {}'.format(misc.funcName(), fp))
            yield fp

def process_source_file(fpath, fcols=fname_cols, collection=None):
    doc = dict(zip(*[fcols, os.path.basename(fpath).split('_')]))
    if (str(doc.get('account')).isdigit()):
        doc['account'] = int(doc.get('account'))
    if (isinstance(doc.get('timestamp'), str)):
        doc['timestamp'] = datetime.datetime.strptime(doc.get('timestamp'), "%Y%m%dT%H%MZ")
    __source__ = doc.get('__source__', fpath)
    if (__source__.find('/mnt/') > -1):
        doc['__source__'] = os.sep.join(__source__.split(os.sep)[3:])
    return None

with timer.Timer() as timer1:
    for fp in iterate_directory(raw_data_source):
        process_source_file(fp, collection=coll)
msg = 'Data read :: num_events: {} in {:.2f} secs'.format(num_events, timer1.duration)
print(msg)
logger.info(msg)

def doc_cleaner(doc, ignores=[]):
    ignores = ignores if (isinstance(ignores, list)) else []
    return {k:int(v) if (str(v).isdigit() or str(v).isdecimal()) else v for k,v in doc.items() if (k not in ignores)}

def modify_criteria(criteria=None, additional=None):
    items = criteria.get('$and', [])
    items.append(additional)
    return criteria

__stats__ = {}

__num_events = 0
@bin_processor(bin_size=600, chunk_size=100, stats=__stats__, db=db_collection(client, dest_db_name, dest_coll_name), logger=logger)
def step2_binner(data, bin_count, bin_size, stats={}, db=None, chunk_size=-1, logger=None):
    if (logger):
        logger.info('step2_binner :: bin size: {}'.format(len(data)))
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
    
    if (isinstance(stats, dict)):
        kk = '{}:{}'.format(dd[0], hh[0])
        b_stats = stats.get(kk, {})
        b_stats['bin_count'] = b_stats.get('bin_count', 0) + 1
        b_stats['bin_size'] = b_stats.get('bin_size', 0) + int(df_d.size)
        stats[kk] = b_stats

    #write_df_to_mongoDB( df_d, db, chunk_size=chunk_size, logger=logger)
    msg = 'step2_binner :: binned: {}:{}'.format(dd[0], hh[0])
    logger.info(msg)
    print(msg)

logger.info('Done.')

sys.exit()
