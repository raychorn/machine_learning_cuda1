import os
import sys

import pandas as pd

from pymongo.mongo_client import MongoClient

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

MONGO_INITDB_DATABASE = 'admin'
MONGO_URI = 'mongodb://mongodb1-10.web-service.org:27017,mongodb2-10.web-service.org:27017,mongodb3-10.web-service.org:27017/?replicaSet=rs0&authSource=admin&compressors=snappy&retryWrites=true'
MONGO_INITDB_USERNAME = 'root'
MONGO_INITDB_PASSWORD = 'sisko@7660$boo'
MONGO_AUTH_MECHANISM = 'SCRAM-SHA-256'

is_not_none = lambda s:(s is not None)

db = lambda cl,n:cl.get_database(n)
db_collection = lambda cl,n,c:db(cl,n).get_collection(c)

def get_pipeline_for(criteria, projection, skip_num, limit_n, sort):
    pipeline = []
    if (criteria):
        pipeline.append({
            "$match": criteria
        })
    if (projection):
        pipeline.append({
            "$project": projection
        })
    if (isinstance(skip_num, int)):
        pipeline.append({
            "$skip": skip_num  # No. of documents to skip (Should be `0` for Page - 1)
        })
    if (isinstance(limit_n, int)):
        pipeline.append({
            "$limit": limit_n  # No. of documents to be displayed on your webpage
        })
    if (sort):
        pipeline.append({
            "$sort": sort
        })
    return pipeline

def docs_generator(collection, criteria={}, projection=None, sort=None, skip=0, limit=10, maxTimeMS=30000, verbose=False, logger=None):
    '''
    This generator function will perform aggregations to step through very large datasets using "limit" as the stepping factor.
    This means this generator will retrieve "limit" docs for each iteration until all the docs have been retrieved.
    nlimit is the maximum number of docs to iterate.
    sort :: { $sort: { <field1>: <sort order>, <field2>: <sort order> ... } } where the sort={'field1': 1, 'field2': -1}
    '''
    skip_num = skip
    pipeline = get_pipeline_for(criteria, projection, skip_num, limit, sort)
    if (verbose) and (logger):
        logger.debug('DEBUG:  Pipeline: {}'.format(pipeline))
    try:
        for doc in collection.aggregate(pipeline, allowDiskUse=True, maxTimeMS=maxTimeMS):
            yield doc
    except Exception as ex:
        if (logger):
            logger.error('DEBUG: Pipeline stopped due to {}'.format(str(ex)))


def get_mongo_client(mongouri=None, db_name=None, username=None, password=None, authMechanism=None):
    if (is_not_none(authMechanism)):
        assert is_not_none(username), 'Cannot continue without a username ({}).'.format(username)
        assert is_not_none(password), 'Cannot continue without a password ({}).'.format(password)
    assert is_not_none(db_name), 'Cannot continue without a db_name ({}).'.format(db_name)
    assert is_not_none(mongouri), 'Cannot continue without a mongouri ({}).'.format(mongouri)
    return  MongoClient(mongouri, username=username, password=password, authSource=db_name, authMechanism=authMechanism)

try:
    client = get_mongo_client(mongouri=MONGO_URI, db_name=MONGO_INITDB_DATABASE, username=MONGO_INITDB_USERNAME, password=MONGO_INITDB_PASSWORD, authMechanism=MONGO_AUTH_MECHANISM)
except:
    sys.exit()

source_db_name = 'DataScience1-queue2_processed'
source_coll_name = 'sx-vpclogss3-filtered-work-queue'

source_coll = db_collection(client, source_db_name, source_coll_name)

num_items = source_coll.count_documents({})

collection_size = num_items
batch_size = round(collection_size / 10)
skips = range(0, collection_size, int(batch_size))

projection = {
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
    for cursor in [docs_generator(source_coll, projection=projection, sort=_sort, skip=skip_n, limit=batch_size, maxTimeMS=30000, verbose=False, logger=None) for skip_n in enumerate(skips)]:
        for doc in cursor:
            data_list = doc.get('dstport-bin', [])
            #print(len(data_list))
            for dd in data_list:
                items.append(dd)
            
    df = pd.DataFrame(items)
    print(df.size)
    print(df.shape)
    print(df.head())
finally:
    client.close()
