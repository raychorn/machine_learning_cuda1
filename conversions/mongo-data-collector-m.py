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

f_libs = f_libs.split(';')
for f in f_libs:
    if (os.path.exists(f) and os.path.isdir(f)):
        if (f not in sys.path):
            sys.path.insert(0, f)

from utils2 import typeName
from whois import ip_address_owner

from database import docs_generator

from vyperlogix import misc

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

__bulk_size__ = int(os.environ.get('MONGO_BULK_SIZE', 100))

fname_cols = ['account', 'bucket', 'region', 'name', 'timestamp', 'fname']

acceptable_numeric_special_chars = ['+','-','.',',']
acceptable_numeric_digits = ['0','1','2','3','4','5','6','7','8','9']
acceptable_numeric_chars = acceptable_numeric_digits + acceptable_numeric_special_chars
is_numeric_char = lambda ch:(ch in acceptable_numeric_chars) if (len(ch) > 0) else False
is_really_numeric = lambda s:(len(s) > 0) and (len(str(s).split('.')) < 2) and all([is_numeric_char(ch) for ch in s])
only_numeric_chars = lambda s:''.join([ch for ch in s if (is_numeric_char(ch))])
only_numeric_special_chars = lambda s:''.join([ch for ch in s if (ch in acceptable_numeric_special_chars)])
def normalize_numeric(value):
    value = str(value)
    if (len(value) == len(only_numeric_chars(value)+only_numeric_special_chars(value))):
        value = value.replace(',', '')
        is_positive = value.find('+') > -1
        if (is_positive):
            value = value.replace('+', '')
        is_negative = value.find('-') > -1
        if (is_negative):
            value = value.replace('-', '')
        is_floating = len(value.split('.')) == 2
        try:
            value = int(value) if (not is_floating) else float(value)
        except Exception as ex:
            logger.exception(str(ex), exc_info=sys.exc_info())
        if (is_negative):
            value = -value
    return value

def iterate_directory(root):
    for subdir, dirs, files in os.walk(root):
        for file in files:
            fp = os.path.join(subdir, file)
            logger.info('{} :: {}'.format(misc.funcName(), fp))
            yield fp

def decompress_gzip(fp=None, _id=None, environ=None, logger=None):
    import gzip

    with timer.Timer() as timer3:
        diff = -1
        num_rows = -1
        __status__ = []
        if (logger):
            logger.info('BEGIN: decompress_gzip :: fp is "{}".'.format(fp))
        assert os.path.exists(fp) and os.path.isfile(fp), 'Cannot do much with the provided filename ("{}"). Please fix.'.format(fp)
        try:
            with gzip.open(fp, 'r') as infile:
                outfile_content = infile.read().decode('UTF-8')
            __status__.append({'gzip': True})
            if (logger):
                logger.info('INFO: decompress_gzip :: __status__ is {}.'.format(__status__))
        except Exception as ex:
            logger.exception(str(ex), exc_info=sys.exc_info())
            __status__.append({'gzip': False})
            if (logger):
                logger.info('INFO: decompress_gzip :: __status__ is {}.'.format(__status__))
        try:
            lines = [l.split() for l in outfile_content.split('\n')]
            rows = [{k:normalize_numeric(v) for k,v in dict(zip(lines[0], l)).items()} for l in lines[1:]]
            rows = [row for row in rows if (len(row) > 0)]
            diff = rows[-1].get('start', 0) - rows[0].get('start', 0)
            num_rows = len(rows)
        except Exception as ex:
            if (logger):
                logger.exception(str(ex), exc_info=sys.exc_info())
        if (logger):
            logger.info('END!!! decompress_gzip :: fp is "{}".'.format(fp))
            logger.info('INFO: decompress_gzip :: __status__ is {}.'.format(__status__))
    msg = 'decompress_gzip :: {:.2f} secs'.format(timer3.duration)
    print(msg)
    logger.info(msg)
    return {'status': __status__[0], 'diff': diff, 'num_rows':num_rows, 'rows':rows}


def ingest_source_file(doc, collection=None, logger=None):
    with timer.Timer() as timer2:
        try:
            fpath = doc.get('fpath')
            assert os.path.exists(fpath) and os.path.isfile(fpath), 'Cannot continue without a valid file path ({}).'.format(fpath)
            vector = decompress_gzip(fp=fpath, logger=logger)
            if (isinstance(collection, list)):
                vector['tag'] = doc.get('tag')
                collection.append(vector)
                if (len(collection) >= __bulk_size__):
                    dest_coll.insert_many(collection)
                    collection.clear()
        except Exception as ex:
            logger.exception(str(ex), exc_info=sys.exc_info())
    msg = 'ingest_source_file :: {:.2f} secs'.format(timer2.duration)
    print(msg)
    logger.info(msg)
    

def process_source_file(fpath, fcols=fname_cols, collection=None, logger=None):
    doc = dict(zip(*[fcols, os.path.basename(fpath).split('_')]))
    if (str(doc.get('account')).isdigit()):
        doc['account'] = int(doc.get('account'))
    if (isinstance(doc.get('timestamp'), str)):
        doc['timestamp'] = dt.datetime.strptime(doc.get('timestamp'), "%Y%m%dT%H%MZ")
    doc['fpath'] = fpath
    __source__ = doc.get('__source__', fpath)
    if (__source__.find('/mnt/') > -1):
        toks = __source__.split(os.sep)
        doc['tag'] = os.sep.join(toks[index_of_item('vpcflowlogs',toks)-1:])
    return ingest_source_file(doc, collection=collection, logger=logger)

data_cache = []

files_count = 0
with timer.Timer() as timer1:
    for fp in iterate_directory(raw_data_source):
        process_source_file(fp, collection=data_cache, logger=logger)
        files_count += 1
        if (files_count % 100 == 0):
            print(files_count)
    if (len(data_cache) > 0):
        dest_coll.insert_many(data_cache)
        data_cache.clear()
msg = 'Data read :: number of files: {} in {:.2f} secs'.format(files_count, timer1.duration)
print(msg)
logger.info(msg)

logger.info('Done.')

sys.exit()
