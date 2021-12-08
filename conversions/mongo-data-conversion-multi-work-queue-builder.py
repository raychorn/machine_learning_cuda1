import os
import sys

import re

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

import ipaddress

import datetime as dt
from datetime import datetime

from itertools import chain

from expandvars import expandvars

import numpy as np
import pandas as pd

import multiprocessing

from binner import collector as bin_collector

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
__validation_command_line_option__ = '--validation'

if (not is_running_production()):
    sys.argv.append(__verbose_command_line_option__)
    #sys.argv.append(__validation_command_line_option__)
    
is_verbose = any([str(arg).find(__verbose_command_line_option__) > -1 for arg in sys.argv])
is_validating = any([str(arg).find(__validation_command_line_option__) > -1 for arg in sys.argv])

print('is_running_production: {}'.format(is_running_production()))
print('is_verbose: {}'.format(is_verbose))
print('is_validating: {}'.format(is_validating))
print()

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

f_libs = re.split(';|:|,', f_libs)
for f in f_libs:
    if (os.path.exists(f) and os.path.isdir(f)):
        if (f not in sys.path):
            sys.path.insert(0, f)

from utils2 import typeName
from whois import ip_address_owner

from postgres import Query

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
__env__['MONGO_AUTH_MECHANISM'] ='SCRAM-SHA-256'

__securex_metadata__ = {}

use_postgres_db = eval(os.environ.get('USE_POSTGRES_DB', False))

if (use_postgres_db):
    @Query('asset', 'hostname', None)
    def get_all_postgres_metadata(self=None, session=None):
        _results = {}
        try:
            results = session.query(self.Table).all()
            items = [list(r) for r in results]
            items = [dict(zip(self.Table.columns.keys(), r)) for r in items]
            for item in items:
                hostname = item.get('hostname')
                if (isgoodipv4(hostname)):
                    _results[hostname] = item
        except Exception as e:
            pass
        return _results
    __securex_metadata__ = get_all_postgres_metadata()

data_source = os.environ.get('VPCFLOWLOGS_DATA_SOURCE')

is_data_source_filesystem = (os.path.exists(data_source) and os.path.isdir(data_source))
is_data_source_s3 = data_source.startswith('s3://')
is_data_source_mongodb = (not is_data_source_filesystem and not is_data_source_s3)

msg = 'data_source: {}'.format(data_source)
print(msg)
logger.info(msg)

msg = 'is_data_source_filesystem: {}'.format(is_data_source_filesystem)
print(msg)
logger.info(msg)

msg = 'is_data_source_s3: {}'.format(is_data_source_s3)
print(msg)
logger.info(msg)

msg = 'is_data_source_mongodb: {}'.format(is_data_source_mongodb)
print(msg)
logger.info(msg)

source_db_name = os.environ.get('MONGO_SOURCE_DATA_DB')
source_coll_name = os.environ.get('MONGO_SOURCE_DATA_COL')

dest_db_name = os.environ.get('MONGO_WORK_QUEUE_DATA_DB')
dest_coll_work_queue_name = os.environ.get('MONGO_WORK_QUEUE_COL')

dest_stats_coll_name = os.environ.get('MONGO_WORK_QUEUE_STATS_COL')

dest_bins_coll_name = os.environ.get('MONGO_WORK_QUEUE_BINS_COL')
dest_bins_processed_coll_name = os.environ.get('MONGO_WORK_QUEUE_BINS_PROCD_COL')
dest_bins_rejected_coll_name = os.environ.get('MONGO_WORK_QUEUE_REJECTED_BINS_COL')

dest_networks_coll_name = os.environ.get('MONGO_WORK_QUEUE_NETWORKS_COL')

try:
    client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))
except:
    sys.exit()
    
print('client: {}'.format(client))

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
    assert is_really_something_with_stuff(dest_coll_work_queue_name, str), 'Cannot continue without the dest_coll_work_queue_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_WORK_QUEUE_COL.", exc_info=True)
    sys.exit()

try:
    assert is_really_something_with_stuff(dest_stats_coll_name, str), 'Cannot continue without the dest_stats_coll_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_WORK_QUEUE_STATS_COL.", exc_info=True)
    sys.exit()

try:
    assert is_really_something_with_stuff(dest_bins_coll_name, str), 'Cannot continue without the dest_bins_coll_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_WORK_QUEUE_BINS_COL.", exc_info=True)
    sys.exit()

try:
    assert is_really_something_with_stuff(dest_bins_processed_coll_name, str), 'Cannot continue without the dest_bins_processed_coll_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_WORK_QUEUE_BINS_PROCD_COL.", exc_info=True)
    sys.exit()
    
try:
    assert is_really_something_with_stuff(dest_bins_rejected_coll_name, str), 'Cannot continue without the dest_bins_rejected_coll_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_WORK_QUEUE_REJECTED_BINS_COL.", exc_info=True)
    sys.exit()

try:
    assert is_really_something_with_stuff(dest_networks_coll_name, str), 'Cannot continue without the dest_networks_coll_name.'
except Exception:
    logger.error("Fatal error with .env, check MONGO_WORK_QUEUE_NETWORKS_COL.", exc_info=True)
    sys.exit()

logger.info(str(client))

db = lambda cl,n:cl.get_database(n)
db_collection = lambda cl,n,c:db(cl,n).get_collection(c)

db_coll = lambda cl,n,c:cl.get_database(n).get_collection(c)

repeat_char = lambda c,n:''.join([c for i in range(n)])

criteria = {'$and': [{'action': {'$ne': 'REJECT'}}, {'srcaddr': {'$ne': "-"}, 'dstaddr': {'$ne': "-"}}, {'srcport': {'$ne': "0"}, 'dstport': {'$ne': "0"}}]}

def __criteria__(doc):
    '''
    criteria = {'$and': [{'action': {'$ne': 'REJECT'}}, {'srcaddr': {'$ne': "-"}, 'dstaddr': {'$ne': "-"}}, {'srcport': {'$ne': "0"}, 'dstport': {'$ne': "0"}}]}
    '''
    return (doc.get('action') != 'REJECT') and (doc.get('srcaddr') != '-') and (doc.get('dstaddr') != '-') and (doc.get('srcport') != '0') and (doc.get('dstport') != '0')

source_coll = db_collection(client, source_db_name, source_coll_name)

dest_work_queue_coll = db_collection(client, dest_db_name, dest_coll_work_queue_name)

dest_stats_coll = db_collection(client, dest_db_name, dest_stats_coll_name)

dest_bins_coll = db_collection(client, dest_db_name, dest_bins_coll_name)

dest_bins_processed_coll = db_collection(client, dest_db_name, dest_bins_processed_coll_name)

dest_bins_rejected_coll = db_collection(client, dest_db_name, dest_bins_rejected_coll_name)

dest_networks_coll = db_collection(client, dest_db_name, dest_networks_coll_name)

n_cores = multiprocessing.cpu_count() - 1

deletable_cols = [
                    dest_stats_coll.full_name, 
                    dest_work_queue_coll.full_name, 
                    dest_bins_coll.full_name, 
                    dest_bins_processed_coll.full_name, 
                    dest_bins_rejected_coll.name,
                    dest_networks_coll.name
                ]

if (not is_validating):
    yn = input("Please approve {} delete all. (y/n)".format(', '.join(deletable_cols)))
    if (str(yn.upper()) == 'Y'):
        dest_stats_coll.delete_many({})
        dest_work_queue_coll.delete_many({})
        dest_bins_coll.delete_many({})
        dest_bins_processed_coll.delete_many({})
        dest_bins_rejected_coll.delete_many({})

if (is_data_source_mongodb):
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
    skips = range(0, collection_size, int(batch_size))
elif (is_data_source_filesystem):
    msg = 'BEGIN: Count files in {}'.format(data_source)
    print(msg)
    logger.info(msg)

    def iterate_directory(root):
        for subdir, dirs, files in os.walk(root):
            for file in files:
                fp = os.path.join(subdir, file)
                yield fp

    master_file_count = 0
    with timer.Timer() as timer1:
        files = []
        for fp in iterate_directory(data_source):
            master_file_count += 1
            files.append(fp)
        num_files = len(files)
        batch_size = round(num_files / n_cores)
        skips = [files[n:n+batch_size] for n in range(0, num_files, batch_size)]
    msg = 'END!!! Count files in {} :: num_files: {}, master_file_count {} in {:.2f} secs'.format(data_source, num_files, master_file_count, timer1.duration)
    print(msg)
    logger.info(msg)

elif (is_data_source_s3):
    pass

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
    return _aggregate(pipeline, dest_db_name, dest_coll_work_queue_name)

def process_cursor(proc_id, source_db_name, source_coll_name, sort, criteria, projection, skip_n, limit_n, nlimit, process_stats, logger):
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

    __stats__ = []

    client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))

    dest_work_queue = db_coll(client, dest_db_name, dest_coll_work_queue_name)
    
    def bin_processor(data, bin_count, bin_size, stats=[], db=None, chunk_size=-1, proc_id=None, logger=logger):
        global __bin_count__
        __bin = {}
        __bin_count__ += 1
        __bin['proc_id'] = proc_id
        __bin['bin_num'] = __bin_count__
        __bin['bin_size'] = bin_size
        __bin['uuid'] = str(uuid.uuid4())
        __bin['data'] = [doc_cleaner(doc, normalize=['_id']) for doc in data]
        stats.append(__bin)
        l = len(stats)
        if (l >= chunk_size):
            bins = [aBin for aBin in stats]
            db.insert_many(bins)
            msg = 'bin_collector :: scheduled for binning: {} bins, {}-{}'.format(l, stats[0].get('data', {}).get('start'), stats[0].get('data', {}).get('start'))
            logger.info(msg)
            print(msg)
            del stats[:]

    msg = 'BEGIN: process_cursor ({}) :: skip_n={}, limit_n={}, nlimit={}'.format(proc_id, skip_n, limit_n, nlimit)
    if (logger):
        logger.info(msg)
    print(msg)

    try:
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
                    bin_processor(doc_cleaner(doc, normalize=['_id']), proc_id=proc_id)

                l = len(__stats__)
                if (l > 0):
                    bins = [aBin for aBin in __stats__]
                    db.insert_many(bins)
                    msg = 'bin_collector :: scheduled for binning: {} bins, {}-{}'.format(l, __stats__[0].get('data', [])[0].get('start'), __stats__[0].get('data', [])[-1].get('start'))
                    logger.info(msg)
                    print(msg)
                    del __stats__[:]
            except Exception as e:
                if (logger):
                    logger.error("Error in process_cursor", exc_info=True)
                print('Error in process_cursor!')

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

###########################################################################
def process_files(proc_id, skip_n, logger):
    import binner
    
    assert isinstance(proc_id, int), 'int proc_id is required.'
    assert isinstance(skip_n, list), 'skip_n is required as a list of files.'
    assert logger is not None, 'logger is required.'

    client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))

    dest_work_queue = db_coll(client, dest_db_name, dest_coll_work_queue_name)
    
    dest_bins_coll = db_collection(client, dest_db_name, dest_bins_coll_name)
    dest_bins_rejected_coll = db_collection(client, dest_db_name, dest_bins_rejected_coll_name)
    dest_bins_processed_coll = db_collection(client, dest_db_name, dest_bins_processed_coll_name)
    dest_networks_coll = db_collection(client, dest_db_name, dest_networks_coll_name)

    def file_bin_processor(doc, stats=None, db=dest_work_queue, logger=logger):
        try:
            for _doc in doc.get('rows', []):
                __bin = {}
                __bin['BinID'] = the_binid = binner.BinID(_doc.get('start'))
                toks = the_binid.split('.')
                assert len(toks) == 3, 'BinID must be of the form: XXXXXXX.YY.Z'
                __bin['BinID_X'], __bin['BinID_Y'], __bin['BinID_Z'] = int(toks[0]), int(toks[1]), int(toks[2])
                __bin['data'] = doc_cleaner(_doc, normalize=['_id'])
                __bin['tag'] = doc.get('tag')
                __metadata__ = {}
                def normalize_asn_description(subj={}, owner={}):
                    '''
                        self.__the_metadata__[k] = {k:v, 'owner': asn_description.replace(',', '') if (_owner) else 'LAN'}

                    '''
                    try:
                        asn_description = subj.get('asn_description', 'UNKNOWN')
                        if (asn_description is None):
                            _nets = owner.get('nets', [])
                            if (len(_nets) > 0):
                                asn_description = _nets[0].get('name', 'UNKNOWN')
                        toks = re.split('[^a-zA-Z]', asn_description)
                        if (len([t for t in toks if (t == '')]) > 0):
                            toks = toks[0:toks.index('')]
                            asn_description = ' '.join(toks)
                    except Exception as e:
                        pass
                    return asn_description
                
                cidrs = lambda l:['.'.join(l[0:i]) for i in range(2, len(l))]

                __networks__ = {}
                
                _srcaddr = _doc.get('srcaddr')
                __isgoodipv4__ = isgoodipv4(_srcaddr)
                __metadata__['srcaddr'] = ip_address_owner(_srcaddr) if (__isgoodipv4__) else {}
                __metadata__['srcaddr']['asn_description'] = normalize_asn_description(subj=__metadata__['srcaddr'], owner=__metadata__['srcaddr'])
                if (__isgoodipv4__):
                    __metadata__['srcaddr']['is_private'] = ipaddress.ip_address(_srcaddr).is_private
                    if (use_postgres_db):
                        __metadata__['srcaddr']['securex'] = __securex_metadata__.get(_srcaddr, None)
                        __metadata__['srcaddr']['securex_hostname'] = __metadata__.get('srcaddr', {}).get('securex', {}).get('hostname', None)
                    _cidrs = cidrs(str(_srcaddr).split('.'))
                    for c in _cidrs:
                        __metadata__['srcaddr'][c] = _srcaddr
                        __networks__[c] = _srcaddr
                    __networks__[_srcaddr] = ','.join(_cidrs)
                _dstaddr = _doc.get('dstaddr')
                __isgoodipv4__ = isgoodipv4(_dstaddr)
                __metadata__['dstaddr'] = ip_address_owner(_dstaddr) if (__isgoodipv4__) else {}
                __metadata__['dstaddr']['asn_description'] = normalize_asn_description(subj=__metadata__['dstaddr'], owner=__metadata__['dstaddr'])
                if (__isgoodipv4__):
                    __metadata__['dstaddr']['is_private'] = ipaddress.ip_address(_dstaddr).is_private
                    if (use_postgres_db):
                        __metadata__['dstaddr']['securex'] = __securex_metadata__.get(_dstaddr, None)
                        __metadata__['dstaddr']['securex_hostname'] = __metadata__.get('dstaddr', {}).get('securex', {}).get('hostname', None)
                    _cidrs = cidrs(str(_dstaddr).split('.'))
                    for c in _cidrs:
                        __metadata__['dstaddr'][c] = _dstaddr
                        __networks__[c] = _dstaddr
                    __networks__[_dstaddr] = ','.join(_cidrs)
                __bin['__metadata__'] = __metadata__
                
                if (len(__networks__) > 0):
                    dest_networks_coll.insert_one(__networks__)
                
                @bin_collector(db=db, logger=logger)
                def db_insert(_bin=None, db=None, logger=None):
                    assert db is not None, 'db is required.'
                    assert _bin is not None, '_bin is required.'
                    assert isinstance(_bin, list), '_bin must be a list.'
                    assert len(_bin) > 0, '_bin must be a list of length > 0.'
                    binid = _bin[0].get('BinID')
                    binid_doc = {'BinID':binid}
                    db.find_one_and_update(binid_doc, {'$set': binid_doc}, upsert=True)
                    filtered_bin = [b for b in _bin if (__criteria__(b.get('data', {})))]
                    rejected_bin = [b for b in _bin  if (not __criteria__(b.get('data', {})))]
                    if (len(filtered_bin) > 0):
                        dest_bins_coll.insert_many(filtered_bin, ordered=False)
                    if (len(rejected_bin) > 0):
                        dest_bins_rejected_coll.insert_many(rejected_bin, ordered=False)
                    msg = 'bin_collector :: scheduled for binning: {} --> {} events'.format(binid, len(_bin))
                    if (logger):
                        logger.info(msg)
                    print(msg)
                db_insert(__bin)
        except Exception as e:
            if (logger):
                logger.error("Error in process_files", exc_info=True)
            print('Error in process_files!')
            
    fname_cols = ['account', 'bucket', 'region', 'name', 'timestamp', 'fname']

    first_item = lambda x: next(iter(x))

    index_of_item = lambda item,items: first_item([i for i,x in enumerate(items) if (x == item)])

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

    msg = 'BEGIN: process_cursor ({}) :: skip_n={}'.format(proc_id, len(skip_n))
    if (logger):
        logger.info(msg)
    print(msg)

    try:
        doc_cnt = 0
        file_cnt = 0
        events_cnt = 0
        with timer.Timer() as timer3:
            try:
                for fp in skip_n:
                    file_cnt += 1
                    num_cells = (file_cnt / len(skip_n))
                    print('{}{}::({:2f})'.format(repeat_char(' ', 2-len(str(proc_id))), proc_id, num_cells), end='\n')
                    #BEGIN: <do your magic>
                    data_cache = []
                    process_source_file(fp, collection=data_cache, logger=logger)
                    for doc in data_cache:
                        doc_cnt += 1
                        events_cnt += len(doc.get('rows', []))
                        if (not is_validating):
                            file_bin_processor(doc_cleaner(doc, normalize=['_id']), stats=__stats__, logger=logger)
                    #END!!! <do your magic>

            except Exception as e:
                if (logger):
                    logger.error("Error in process_files", exc_info=True)
                print('Error in process_files!')

        dest_stats_coll = db_coll(client, dest_db_name, dest_stats_coll_name)
        s = {
            'proc_id':proc_id,
            'file_cnt':file_cnt,
            'doc_cnt':doc_cnt,
            'events_cnt':events_cnt,
            'master_file_count':master_file_count,
            'duration':timer3.duration
        }
        dest_stats_coll.find_one_and_update({'proc_id':proc_id}, {'$set': s}, upsert=True)

        _msg = 'master_file_count {}, file_cnt {}, events_cnt {} in {:.2f} secs'.format(master_file_count, file_cnt, events_cnt, timer3.duration)
        print(_msg)
        if (logger):
            logger.info(_msg)
    finally:
        client.close()

    msg = 'END: process_files {} :: completed: doc_cnt={}, skip_n={}'.format(proc_id, doc_cnt, len(skip_n))
    if (logger):
        logger.info(msg)
    print(msg)

###########################################################################

def process_buckets(proc_id, skip_n, logger):
    raise NotImplementedError('process_buckets')

###########################################################################

print('bin_collector :: started')

__bin_size__ = 600

__sort = {'start': pymongo.ASCENDING}
projection = {'start': 1, 'end': 1, 'dstport': 1, 'srcport': 1, 'protocol': 1, 'bytes': 1, 'packets': 1}

with timer.Timer() as timer2:
    try:
        _num_events = 1
        events = []
        if (1):
            msg = 'bin_collector :: creating processes.'
            logger.info(msg)
            print(msg)
            if (is_data_source_mongodb):
                processes = [ multiprocessing.Process(target=process_cursor, args=(_i, source_db_name, source_coll_name, __sort, criteria, projection, skip_n, batch_size, skip_n+batch_size, {}, logger)) for _i,skip_n in enumerate(skips)]
            elif (is_data_source_filesystem):
                processes = [ multiprocessing.Process(target=process_files, args=(_i, skip_n, logger)) for _i,skip_n in enumerate(skips)]
            elif (is_data_source_s3):
                processes = [ multiprocessing.Process(target=process_buckets, args=(_i, skip_n, logger)) for _i,skip_n in enumerate(skips)]
            else:
                raise ValueError('Invalid data source')

            for process in processes:
                process.start()

            for process in processes:
                process.join()

        msg = 'bin_collector :: master records {}: {}'.format('created' if (len(events) > 0) else 'has', _num_events)
        logger.info(msg)
        print(msg)

        is_verbose = False
        
    except Exception as e:
        logger.error("Error in main loop.", exc_info=True)

client = get_mongo_client(mongouri=__env__.get('MONGO_URI'), db_name=__env__.get('MONGO_INITDB_DATABASE'), username=__env__.get('MONGO_INITDB_USERNAME'), password=__env__.get('MONGO_INITDB_PASSWORD'), authMechanism=__env__.get('MONGO_AUTH_MECHANISM'))

dest_work_queue = db_coll(client, dest_db_name, dest_coll_work_queue_name)

if (not is_validating):
    @bin_collector(db=dest_work_queue, flush=True, logger=logger)
    def db_insert(_bin=None, db=None, logger=None):
        assert db is not None, 'db is required.'
        assert _bin is not None, '_bin is required.'
        assert isinstance(_bin, list), '_bin must be a list.'
        if (len(_bin) > 0):
            binid = _bin[0].get('BinID')
            binid_doc = {'BinID':binid}
            db.find_one_and_update(binid_doc, {'$set': binid_doc}, upsert=True)
            filtered_bin = [b for b in _bin if (__criteria__(b.get('data', {})))]
            rejected_bin = [b for b in _bin  if (not __criteria__(b.get('data', {})))]
            if (len(filtered_bin) > 0):
                dest_bins_coll.insert_many(filtered_bin, ordered=False)
            if (len(rejected_bin) > 0):
                dest_bins_rejected_coll.insert_many(rejected_bin, ordered=False)
        msg = 'bin_collector :: scheduled for binning: {} --> {} events'.format(binid, len(_bin))
        if (logger):
            logger.info(msg)
        print(msg)
        msg = 'bin_collector :: scheduled for binning: {} --> {} events'.format(binid, len(doc['data']))
        if (logger):
            logger.info(msg)
        print(msg)
    db_insert([])

if (0):
    total_docs_count = aggregate_docs_count()
    assert 'total' in list(total_docs_count.keys()), 'total not found in total_docs_count'
    assert total_docs_count.get('total', -1) == num_events, 'total_docs_count ({}) != num_events ({}), diff={}'.format(total_docs_count.get('total', -1), num_events, num_events - total_docs_count.get('total', -1))

    total_docs_binned = aggregate_bins_docs_total()
    assert 'total' in list(total_docs_binned.keys()), 'total not found in total_docs_binned'
    #assert total_docs_binned.get('total', -1) == num_events, 'total_docs_binned ({}) != num_events ({}), diff={}'.format(total_docs_binned.get('total', -1), num_events, num_events - total_docs_binned.get('total', -1))

    dest_stats_coll.insert_one({'name':'bin_collector', 'doc_cnt':num_events, 'total_docs_count': total_docs_count.get('total', -1), 'total_docs_binned': total_docs_binned.get('total', -1), 'duration':timer2.duration})

msg = 'Bin Collector :: num_events: {} in {:.2f} secs'.format(_num_events, timer2.duration)
print(msg)
logger.info(msg)

logger.info('Done.')

sys.exit()
