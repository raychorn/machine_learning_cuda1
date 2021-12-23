# To-Do:

## Move the __metadata__ to a separate collection because it is causing false issues.


```
BEGIN:
_id
version
account-id
interface-id
srcaddr
dstaddr
srcport
dstport
protocol
packets
bytes
start
end
action
log-status
__dataset__
__dataset_index__
__metadata__.start_MM
__metadata__.start_DD
__metadata__.start_YYYY
__metadata__.end_MM
__metadata__.end_DD
__metadata__.end_YYYY
__metadata__.srcaddr_subnet1
__metadata__.srcaddr_subnet2
__metadata__.srcaddr_subnet3
__metadata__.dstaddr_subnet1
__metadata__.dstaddr_subnet2
__metadata__.dstaddr_subnet3
__metadata__.srcaddr
__metadata__.owner
__metadata__.dstaddr
END!!!

```

## Method for validation of bin collection (if the bins are valid then the bin processing must be correct):

### Iterate S3 files and count the events from the database and compare with the count of all events from all bins.
### Check the max(start) - min(start) for each bin and report any > 10 minutes.
### Number of bins per hour? (Cannot exceed 6)
### Charts and Graphs for binned data???  This would take a good deal of time to generate.

```
libs=/mnt/FourTB/__projects/vyperlogix/machine_learning_cuda1/libs:/mnt/FourTB/__projects/vyperlogix/securex_attack-inferencer/attack-inferencer/libs:/mnt/FourTB/__projects/vyperlogix/private_vyperlogix_lib3:/mnt/FourTB/__projects/vyperlogix/securex_attack-inferencer/attack-inferencer/libs

LITERALS=MONGO_INITDB_ROOT_PASSWORD

SSH_USERNAME=raychorn

MONGO_BULK_SIZE=2000

#MONGO_INITDB_DATABASE=admin
#MONGO_URI=mongodb://mongodb1-10.web-service.org:27017,mongodb2-10.web-service.org:27017,mongodb3-10.web-service.org:27017/?replicaSet=rs0&authSource=admin&compressors=snappy&retryWrites=true
#MONGO_INITDB_ROOT_USERNAME=???
#MONGO_INITDB_ROOT_PASSWORD=???
#MONGO_AUTH_MECHANISM=SCRAM-SHA-256

USE_POSTGRES_DB=True
POSTGRES_DB_URI=postgresql+psycopg2://???:???@tp01-2066.web-service.org:49153/securex_assets

MONGO_INITDB_DATABASE=admin
MONGO_URI=mongodb://MONGOSHARD4-10.WEB-SERVICE.ORG:27017/?authSource=admin&compressors=snappy&retryWrites=true
MONGO_INITDB_ROOT_USERNAME=???
MONGO_INITDB_ROOT_PASSWORD=???
MONGO_AUTH_MECHANISM=SCRAM-SHA-256

MONGO_SOURCE_DATA_DB=DataScience2
MONGO_SOURCE_DATA_COL=sx-vpclogss3-filtered

MONGO_WORK_QUEUE_DATA_DB=DataScience6
MONGO_WORK_QUEUE_COL=sx-vpclogss3-filtered-master
MONGO_WORK_QUEUE_MASTER_COL=sx-vpclogss3-filtered-work-queue-master
MONGO_WORK_QUEUE_STATS_COL=sx-vpclogss3-filtered-work-queue-stats
MONGO_WORK_QUEUE_BINS_COL=sx-vpclogss3-filtered-work-queue-bins
MONGO_WORK_QUEUE_BINS_PROCD_COL=sx-vpclogss3-filtered-work-queue-bins-processed
MONGO_WORK_QUEUE_REJECTED_BINS_COL=sx-vpclogss3-rejected-work-queue-bins
MONGO_WORK_QUEUE_NETWORKS_COL=sx-vpclogss3-networks
MONGO_WORK_QUEUE_UNIQUE_NETWORKS_COL=sx-vpclogss3-networks-unique

MONGO_DEST_DATA_BINNED_COL=sx-vpclogss3-filtered-binned
MONGO_DEST_DATA_COL=sx-vpclogss3-filtered-raw-data
MONGO_DEST_DATA_STATS_COL=sx-vpclogss3-filtered-binner-stats
MONGO_DEST_DATA_METADATA_COL=sx-vpclogss3-filtered-binned-metadata

VPCFLOWLOGS_DATA_SOURCE=/mnt/FourTB/data/AWSLogs/898174439248/vpcflowlogs/
VPCFLOWLOGS_DATA_DB=DataScience-vpcflowlogs
VPCFLOWLOGS_DATA_COL=sx-vpclogss3-data

MONGO_VPCFLOWLOGS_DATA_DB=DataScience2-bins2
MONGO_VPCFLOWLOGS_COL=sx-vpclogss3-bins

RUN_MODE=DEV # Uses the local mongodb to test data ingestion funcitons. Use PROD to use the 
```
