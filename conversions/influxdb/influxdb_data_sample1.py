# -*- coding: utf-8 -*-
"""Tutorial on using the InfluxDB client."""

from datetime import datetime
import os

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

def main():
    #####################################################################
    # You can generate an API token from the "API Tokens Tab" in the UI
    token = 'vEwNWbBIQd4KsBnO4ge_4QMN_FlR_X5juqoctBbjH3Z5w6abJqZ1AfRMTn-McvdF9vgIp_DVw4xHo64aXGb20w=='
    org = "raychorn@gmail.com"
    bucket = "vpcflowlogs"

    with InfluxDBClient(url="https://us-east-1-1.aws.cloud2.influxdata.com", token=token, org=org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        data = "mem,host=host1 used_percent=23.43234543"
        write_api.write(bucket, org, data)

        data = "mem,host=host2 used_percent=24.43234543"
        write_api.write(bucket, org, data)

    #####################################################################

if (__name__ == '__main__'):
    main()
