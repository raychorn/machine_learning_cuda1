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

first_item = lambda x: next(iter(x))

index_of_item = lambda item,items: first_item([i for i,x in enumerate(items) if (x == item)])

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

f_libs = f_libs.split(':')
for f in f_libs:
    if (os.path.exists(f) and os.path.isdir(f)):
        if (f not in sys.path):
            sys.path.insert(0, f)

from vyperlogix import misc

from vyperlogix.contexts import timer

from vyperlogix.decorators import threading
from vyperlogix.mongo.database import get_pipeline_for

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

db_name = 'DataScience7'
master_col_name = 'sx-vpclogss3-filtered-master'
binned_coll_name = 'sx-vpclogss3-filtered-binned'
binned_metadata_coll_name = 'sx-vpclogss3-filtered-binned-metadata'

try:
    client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))
except:
    sys.exit()

try:
    assert is_really_something_with_stuff(master_col_name, str), 'Cannot continue without the db_name.'
except Exception:
    logger.error("Fatal error with .env, check master_col_name.", exc_info=True)
    sys.exit()

try:
    assert is_really_something_with_stuff(binned_coll_name, str), 'Cannot continue without the coll_name.'
except Exception:
    logger.error("Fatal error with .env, check binned_coll_name.", exc_info=True)
    sys.exit()
    
try:
    assert is_really_something_with_stuff(binned_metadata_coll_name, str), 'Cannot continue without the coll_name.'
except Exception:
    logger.error("Fatal error with .env, check binned_metadata_coll_name.", exc_info=True)
    sys.exit()
    
logger.info(str(client))

db = lambda cl,n:cl.get_database(n)
db_collection = lambda cl,n,c:db(cl,n).get_collection(c)

data_cache = {}

master_coll = db_collection(client, db_name, master_col_name)
with timer.Timer() as timer1:
    num_bins = master_coll.count_documents({})
msg = 'Database {} scanned :: number of bins counted: {} in {:.2f} secs'.format(master_coll.full_name, num_bins, timer1.duration)
print(msg)
logger.info(msg)

def bin_num_from_bin_id(bin_id):
    DD,HH,NN = bin_id.split('.')
    DD = int(DD)
    HH = int(HH)
    NN = int(NN)
    return (DD*24*6) + (HH*6) + NN

def compare_bin_ids(bin_id1, bin_id2):
    b1_num = bin_num_from_bin_id(bin_id1)
    b2_num = bin_num_from_bin_id(bin_id2)
    
    is_eq = (b1_num == b2_num)
    is_lt = (b1_num < b2_num)
    is_gt = (b1_num > b2_num)
    delta = max(b1_num, b2_num) - min(b1_num, b2_num)
    return is_eq, is_lt, is_gt, delta

def next_expected_bin_id(bin_id):
    DD,HH,NN = bin_id.split('.')
    DD = int(DD)
    HH = int(HH)
    NN = int(NN) + 1
    if (NN > 5):
        NN = 0
        HH = int(HH) + 1
        if (HH > 23):
            HH = 0
            DD = int(DD) + 1
    return '{}.{}.{}'.format(DD, HH, NN)

_sort = {'BinID':1}
_criteria = {}
_projection = {'BinID':1}

missing_bins = []

fname = '{}{}missing_bins_report.txt'.format(os.path.dirname(__file__), os.sep)
fOut = open(fname, 'w')

files_count = 0
with timer.Timer() as timer1:
    _pipeline = get_pipeline_for(_criteria, _projection, 0, num_bins, _sort)
    _cursor = master_coll.aggregate(_pipeline, allowDiskUse=True, maxTimeMS=12*3600*1000)

    expected_bin_id = None
    current_bin_id = None
    first_bin_id = None
    bin_count = 0
    for doc in _cursor:
        bin_id = doc.get('BinID')
        data_cache[bin_id] = data_cache.get(bin_id, 0) + 1
        if (first_bin_id is None):
            first_bin_id = bin_id
        bin_count += 1
    last_bin_id = bin_id
    is_eq, is_lt, is_gt, delta = compare_bin_ids(first_bin_id, last_bin_id)
    
    first_bin_num = bin_num_from_bin_id(first_bin_id)
    last_bin_num = bin_num_from_bin_id(last_bin_id)
    expected_bin_id = first_bin_id
    while (first_bin_num <= last_bin_num):
        expected_bin_id = next_expected_bin_id(expected_bin_id)
        if (data_cache.get(expected_bin_id, -1) == -1):
            missing_bins.append(expected_bin_id)
        elif (bin_num_from_bin_id(expected_bin_id) <= last_bin_num):
            print('F {} :: E {} :: L {} :: Hit {}'.format(first_bin_id, expected_bin_id, last_bin_id, data_cache.get(expected_bin_id, -1)), file=fOut)
        first_bin_num = bin_num_from_bin_id(expected_bin_id)
msg = 'Database {} scanned :: number of bins scanned: {} in {:.2f} secs'.format(master_coll.full_name, bin_count, timer1.duration)
print(msg, file=fOut)
logger.info(msg)

msg = 'Missing bins num: {}'.format(len(missing_bins))
print(msg, file=fOut)
logger.info(msg)

fOut.flush()
fOut.close()

logger.info('Done.')

sys.exit()
