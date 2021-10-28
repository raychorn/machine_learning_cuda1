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

dest_db_name = os.environ.get('MONGO_DEST_DATA_DB')
dest_coll_name = os.environ.get('MONGO_DEST_DATA_COL')

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

db_collection = lambda cl,n,c:cl.get_database(n).get_collection(c)

criteria = {'$and': [{'action': {'$ne': 'REJECT'}}, {'srcaddr': {'$ne': "-"}, 'dstaddr': {'$ne': "-"}}, {'srcport': {'$ne': "0"}, 'dstport': {'$ne': "0"}}]}

projection = {'srcaddr': 1, 'dstaddr': 1, 'srcport': 1, 'dstport': 1, 'protocol': 1, 'packets': 1, 'bytes': 1, 'start': 1, 'end': 1, 'log-status': 1, '__metadata__.srcaddr.owner': 1, '__metadata__.dstaddr.owner': 1, '__dataset__': 1, '__dataset_index__': 1}

with timer.Timer() as timer1:
    num_events = db_collection(client, source_db_name, source_coll_name).count_documents({})
msg = 'Data read :: num_events: {} in {:.2f} secs'.format(num_events, timer1.duration)
print(msg)
logger.info(msg)

projection_end = {'start': 1, 'end': -1}

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


def step1(df):
    df['start'] = pd.to_datetime(df['start'],unit='s')
    df['end'] = pd.to_datetime(df['end'],unit='s')
    df['hour']   = df.start.dt.hour.astype('uint8')
    df['minute'] = df.start.dt.minute.astype('uint8')
    
    df['second'] = df.start.dt.second.astype('uint8')
    df['duration'] = df['end'] - df['start']
    
    df["date"] = df.start.dt.date

def step2(df):
    firstDate = df.start.min()
    lastDate  = df.end.max()

    assert firstDate != lastDate, 'firstDate == lastDate, there must be an error in the data.'
    
    date = firstDate
    date1 = firstDate
    bin = 1
    #empty dataframe
    df_d0 = pd.DataFrame()
    while date < lastDate:
        date1 += dt.timedelta(minutes=10)
        if (date1.minute == 0):
            df_d = df[((df["start"].dt.date == date.date()) & (df["hour"] == date.hour) & ((df["minute"] >= date.minute) & (df["minute"] < 60)))]
        else:
            df_d = df[((df["start"].dt.date == date.date()) & (df["hour"] == date.hour) & ((df["minute"] >= date.minute) & (df["minute"] < date1.minute)))]
        
        df_d = df_d.groupby(["dstport", "date", "hour"],  as_index=False).sum()
        
        #BPP
        df_d["bpp"] = df_d["bytes"]/df_d["packets"] 
        df_d["bpp_norm"] =  np.log(df_d['bpp'])
        df_d["bpp_zscore+log"] = (df_d["bpp_norm"] - df_d["bpp_norm"].mean()) / df_d["bpp_norm"].std()   
        
        #MAKING BINS 
        df_d[["date", "hour"]] = df_d[["date", "hour"]].astype(str)
        df_d["bin"] = df_d["date"] +" h-"+ df_d["hour"] + " b-" + str(bin)
        
        
        df_d0 = df_d0.append(df_d, sort=False)
        
        #print('{} - {}'.format(date, date1))

        bin+=1
        if bin > 6:
            bin = 1
        date += dt.timedelta(minutes=10)
    date -= dt.timedelta(days=1)
    return df_d0

def write_df_to_mongoDB( df, db_collection, chunk_size = 100):
    db_collection.delete_many({})
    my_list = df.to_dict('records')
    l =  len(my_list)
    ran = range(l)
    steps = ran[chunk_size::chunk_size]

    i = 0
    t_chunks = 0
    for j in steps:
        chunk = my_list[i:j]
        print('{}:{} -> {}'.format(i, j, len(chunk)))
        db_collection.insert_many(chunk)
        i = j
        t_chunks += len(chunk)

    print('{}:{} -> {}'.format(i, l, len(my_list[i:])))
    if (i < l):
        chunk = my_list[i:]
        print('{}:{} -> {}'.format(i, len(my_list), len(chunk)))
        db_collection.insert_many(chunk)
        t_chunks += len(chunk)

    assert t_chunks == l, 't_chunks != l, there must be an error in the process of committing the data to the db.'
    print('Done, {} total, {} expected'.format(t_chunks, l))
    return

limit = nlimit = max(1000, num_events)

if (0):
    for doc in docs_generator(db_collection(client, source_db_name, source_coll_name), criteria={}, projection=projection_end, skip=0, limit=limit, nlimit=nlimit):
        print(doc)
else:
    projection = {'start': 1, 'end': -1, 'dstport': 1, 'bytes': 1, 'packets': 1}

    with timer.Timer() as timer2:
        df =  pd.DataFrame(list(docs_generator(db_collection(client, source_db_name, source_coll_name), criteria={}, projection=projection, skip=0, limit=limit, nlimit=nlimit)))
        print(df.shape)

        step1(df)
        df_d0 = step2(df)
        print(df_d0.shape)
        print(df_d0)

    msg = 'Step 1+2 :: num_events: {} in {:.2f} secs'.format(df.size, timer2.duration)
    print(msg)
    logger.info(msg)

    with timer.Timer() as timer3:
        write_df_to_mongoDB( df_d0, db_collection(client, dest_db_name, dest_coll_name), chunk_size = 100)
    msg = 'Write Data :: num_events: {} in {:.2f} secs'.format(df_d0.size, timer3.duration)
    print(msg)
    logger.info(msg)

logger.info('Done.')

sys.exit()
