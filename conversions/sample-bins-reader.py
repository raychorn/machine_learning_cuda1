import os
import sys

import types

import dotenv

import socket
import logging

import shutil

import datetime as dt
from datetime import datetime

import pandas as pd

from pymongo.mongo_client import MongoClient

from logging.handlers import RotatingFileHandler

'''
db.getCollection("sx-vpclogss3-filtered-work-queue").find({
        "dstport-bin": {
            $exists: true
        }
    }, {
        "data.dstport": 1,
        "dstport-bin.bin": 1,
        "dstport-bin.bpp": 1,
        "dstport-bin.bpp_norm": 1,
        "dstport-bin.bpp_zscore+log": 1,
        "dstport-bin.bytes": 1,
        "dstport-bin.date": 1,
        "dstport-bin.dstport": 1,
        "dstport-bin.end": 1,
        "dstport-bin.hour": 1,
        "dstport-bin.packets": 1,
        "dstport-bin.protocol": 1,
        "dstport-bin.srcport": 1,
        "dstport-bin.start": 1
    })
    .sort({
        "dstport-bin.date": 1,
        "dstport-bin.hour": 1
    })
    .limit(100)
'''
production_token = 'production'
development_token = 'development'

is_running_production = lambda : (socket.gethostname().find('raychorn') == -1)

is_verbose = not is_running_production()

something_greater_than_zero = lambda s:(s > 0)

is_really_something = lambda s,t:(s is not None) and ( (callable(t) and (not isinstance(t, types.FunctionType)) and isinstance(s, t)) or (callable(t) and (isinstance(t, types.FunctionType)) and t(s)) )

is_really_something_with_stuff = lambda s,t:is_really_something(s,t) and (len(s) > 0)

default_timestamp = lambda t:t.isoformat().replace(':', '').replace('-','').split('.')[0]


fp_env = dotenv.find_dotenv()
dotenv.load_dotenv(fp_env)

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

from vyperlogix.mongo.database import docs_generator
from vyperlogix.mongo.database import get_pipeline_for

from vyperlogix.contexts import timer

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

MONGO_INITDB_DATABASE = 'admin'
MONGO_URI = 'mongodb://mongodb1-10.web-service.org:27017,mongodb2-10.web-service.org:27017,mongodb3-10.web-service.org:27017/?replicaSet=rs0&authSource=admin&compressors=snappy&retryWrites=true'
MONGO_INITDB_USERNAME = 'root'
MONGO_INITDB_PASSWORD = 'sisko@7660$boo'
MONGO_AUTH_MECHANISM = 'SCRAM-SHA-256'

is_not_none = lambda s:(s is not None)

db = lambda cl,n:cl.get_database(n)
db_collection = lambda cl,n,c:db(cl,n).get_collection(c)

def get_mongo_client(mongouri=None, db_name=None, username=None, password=None, authMechanism=None):
    if (is_not_none(authMechanism)):
        assert is_not_none(username), 'Cannot continue without a username ({}).'.format(username)
        assert is_not_none(password), 'Cannot continue without a password ({}).'.format(password)
    assert is_not_none(db_name), 'Cannot continue without a db_name ({}).'.format(db_name)
    assert is_not_none(mongouri), 'Cannot continue without a mongouri ({}).'.format(mongouri)
    return  MongoClient(mongouri, username=username, password=password, authSource=db_name, authMechanism=authMechanism)


def maximum_absolute_scaling(df):
    df_scaled = df.copy()
    for column in df_scaled.columns:
        df_scaled[column] = df_scaled[column]  / df_scaled[column].abs().max()
    return df_scaled
    
try:
    client = get_mongo_client(mongouri=MONGO_URI, db_name=MONGO_INITDB_DATABASE, username=MONGO_INITDB_USERNAME, password=MONGO_INITDB_PASSWORD, authMechanism=MONGO_AUTH_MECHANISM)
except:
    sys.exit()

source_db_name = 'DataScience1-queue2_processed'
source_coll_name = 'sx-vpclogss3-filtered-work-queue'

source_coll = db_collection(client, source_db_name, source_coll_name)

msg = 'BEGIN: Count bins in {}+{}'.format(source_db_name, source_coll_name)
print(msg)
with timer.Timer() as timer1:
    num_items = source_coll.count_documents({})
msg = 'END!!! Count bins in {}+{} :: num_items: {} in {:.2f} secs'.format(source_db_name, source_coll_name, num_items, timer1.duration)
print(msg)

collection_size = num_items
batch_size = round(collection_size / 10)
skips = range(0, collection_size, int(batch_size))

projection = {
        "data":1,
        "dstport-bin.bin": 1,
        "dstport-bin.bpp": 1,
        "dstport-bin.bpp_norm": 1,
        "dstport-bin.bpp_zscore+log": 1,
        "dstport-bin.bytes": 1,
        "dstport-bin.date": 1,
        "dstport-bin.dstport": 1,
        "dstport-bin.end": 1,
        "dstport-bin.hour": 1,
        "dstport-bin.packets": 1,
        "dstport-bin.protocol": 1,
        "dstport-bin.srcport": 1,
        "dstport-bin.start": 1
    }

_sort = {
        "dstport-bin.date": 1,
        "dstport-bin.hour": 1
    }

items = []
try:
    msg = 'BEGIN: Load bins from {}+{}'.format(source_db_name, source_coll_name)
    print(msg)
    with timer.Timer() as timer2:
        for cursor in [docs_generator(source_coll, projection=projection, sort=_sort, skip=skip_n, limit=batch_size, maxTimeMS=5*60*60*1000, verbose=is_verbose, logger=logger) for skip_n in enumerate(skips)]:
            for doc in cursor:
                data_list = doc.get('data', [])
                for dd in data_list:
                    items.append(dd)
    msg = 'END!!! Load bins from {}+{} :: num_items: {} in {:.2f} secs'.format(source_db_name, source_coll_name, len(items), timer2.duration)
    print(msg)
            
    df = pd.DataFrame(items)
    print(df.size)
    print(df.shape)
    print(df.head())

    df_selected = df[['dstport', 'start']]
    df_selected['start'] = df_selected['start'].dt.total_seconds().astype(int)
    df_scaled = maximum_absolute_scaling(df_selected)

finally:
    client.close()
