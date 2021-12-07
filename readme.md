
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
