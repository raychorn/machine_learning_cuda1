import os
import sys
import json

import traceback

import types

import shutil

import socket
import logging

import datetime as dt
from datetime import datetime

import multiprocessing

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

def get_logger(fpath=__file__, product='list-databases', logPath='logs', is_running_production=is_running_production()):
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

LOGPATH = os.environ.get('LOGPATH')
if (LOGPATH is None) or (not os.path.exists(LOGPATH)):
    LOGPATH = __file__
print('*** LOGPATH: {}'.format(LOGPATH))
logger = get_logger(fpath=LOGPATH, product=os.path.splitext(os.path.basename(__file__))[0])

from pymongo.mongo_client import MongoClient

is_not_none = lambda s:(s is not None)

def get_mongo_client(mongouri=None, db_name=None, username=None, password=None, authMechanism=None):
    if (is_not_none(authMechanism)):
        assert is_not_none(username), 'Cannot continue without a username ({}).'.format(username)
        assert is_not_none(password), 'Cannot continue without a password ({}).'.format(password)
    assert is_not_none(db_name), 'Cannot continue without a db_name ({}).'.format(db_name)
    assert is_not_none(mongouri), 'Cannot continue without a mongouri ({}).'.format(mongouri)
    return  MongoClient(mongouri, username=username, password=password, authSource=db_name, authMechanism=authMechanism)


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

from vyperlogix.mongo.database import docs_generator
from vyperlogix.mongo.database import get_pipeline_for

from vyperlogix.contexts import timer

__env__ = {}

__env__['MONGO_INITDB_DATABASE'] = os.environ.get('MONGO_INITDB_DATABASE')
__env__['MONGO_URI'] = os.environ.get('MONGO_URI')
__env__['MONGO_INITDB_USERNAME'] = os.environ.get("MONGO_INITDB_ROOT_USERNAME")
__env__['MONGO_INITDB_PASSWORD'] = os.environ.get("MONGO_INITDB_ROOT_PASSWORD")
__env__['MONGO_AUTH_MECHANISM'] = os.environ.get('MONGO_AUTH_MECHANISM')

try:
    client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))
except:
    sys.exit()

db = lambda cl,n:cl.get_database(n)
db_collection = lambda cl,n,c:db(cl,n).get_collection(c)

logger.info(str(client))

n_cores = int(multiprocessing.cpu_count() / 2)

ignores = ['local', 'admin', 'config', 'test']

from bson.json_util import dumps

DESTDIR = sys.argv[1] if (len(sys.argv) > 1) else os.sep.join([os.path.dirname(__file__), 'mongodumps'])

if (not os.path.exists(DESTDIR) or not os.path.isdir(DESTDIR)):
    os.makedirs(DESTDIR)

if (0):
    import binner

    def unix_time_secs():
        import time
        return int(time.time())

    ts = unix_time_secs() * 1
    cnt = 0
    for n in range(0, 86400, 300):
        _ts = ts + n
        print('#{}: {}'.format(cnt, binner.BinID(_ts)))
        cnt += 1
    print()

def process_batch(env, vector, logger):

    def process_cursor(proc_id, _destfpath, _db_name, _coll_name, _sort, _criteria, _projection, skip_n, limit_n, logger):
        _client = get_mongo_client(mongouri=env.get('MONGO_URI'), db_name=env.get('MONGO_INITDB_DATABASE'), username=env.get('MONGO_INITDB_USERNAME'), password=env.get('MONGO_INITDB_PASSWORD'), authMechanism=env.get('MONGO_AUTH_MECHANISM'))

        _pipeline = get_pipeline_for(_criteria, _projection, skip_n, limit_n, _sort)
        _db = db_collection(_client, _db_name, _coll_name)
        _cursor = _db.aggregate(_pipeline, allowDiskUse=True, maxTimeMS=12*3600*1000)

        docs = [doc for doc in _cursor]
        msg = 'DEBUG: {} {}.{} -> len(docs) -> {} of {}'.format(proc_id, _db_name, _coll_name, len(docs), limit_n)
        print(msg)
        logger.info(msg)
        with open('{}/{}{}.json'.format(_destfpath, _coll_name, skip_n), 'w') as file:
            json.dump(json.loads(dumps(docs)), file)

    
    __client = get_mongo_client(mongouri=env.get('MONGO_URI'), db_name=env.get('MONGO_INITDB_DATABASE'), username=env.get('MONGO_INITDB_USERNAME'), password=env.get('MONGO_INITDB_PASSWORD'), authMechanism=env.get('MONGO_AUTH_MECHANISM'))

    dbName = vector.get('dbName')
    collName = vector.get('collName')
    num_docs = vector.get('num_docs')
    
    __destfpath = os.sep.join([DESTDIR, dbName])
    os.makedirs(__destfpath, exist_ok=True)

    collection_size = num_docs
    batch_size = round(collection_size / n_cores)
    skips = range(0, collection_size, int(batch_size))

    __sort = {}
    __criteria = {}
    __projection = {}

    processes = [ multiprocessing.Process(target=process_cursor, args=(_i, __destfpath, dbName, collName, __sort, __criteria, __projection, skip_n, batch_size, logger)) for _i,skip_n in enumerate(skips)]

    for process in processes:
        process.start()

    for process in processes:
        process.join()


try:
    db_names = [db_name for db_name in client.list_database_names() if (db_name not in ignores)]
    for dbName in db_names:
        _db = db(client, dbName)
        for collName in _db.list_collection_names():
            collection = db_collection(client, dbName, collName)
            print('BEGIN: Counting docs in {}.{}'.format(dbName, collName))
            num_docs = collection.count_documents({})
            print('END!!! Counting docs in {}.{}'.format(dbName, collName))
            vector = {'dbName': dbName, 'collName': collName, 'num_docs': num_docs}
            process_batch(__env__, vector, logger)
except Exception as ex:
    extype, ex, tb = sys.exc_info()
    formatted = traceback.format_exception_only(extype, ex)[-1]
    if (logger):
        logger.error(formatted)
    else:
        print(formatted)

logger.info('Done.')

sys.exit()
