import imp
import os
import sys

import traceback

import enum

from io import StringIO

from dateutil import parser

from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

class options(enum.Enum):
    files = 1
    db = 2
    
#__options__ = options.files
__options__ = options.db

if (not any([str(f).find('vyperlogix_lib') > 0 for f in sys.path])):
    sys.path.insert(0, '/mnt/FourTB/__projects/vyperlogix/private_vyperlogix_lib3')

__data_root__ = '/mnt/FourTB/data/AWSLogs/898174439248/vpcflowlogs/'

all_files = []

def get_all_files_from(rootdir):
    the_files = {}
    for dirname,dirs,files in os.walk(rootdir):
        for file in files:
            filepath = dirname + os.sep + file
            toks = file.split('_')
            _ts = [t for t in toks if t.endswith('Z')]
            ts = _ts[0] if (len(_ts) > 0) else None
            try:
                yyyy = ts[0:4]
                mm = ts[4:6]
                dd = ts[6:8]
                t = ts[8]
                h = ts[9:11]
                m = ts[11:13]
                iso_ts = '{}-{}-{}T{}:{}'.format(yyyy,mm,dd,h,m)
                iso_date = parser.isoparse(iso_ts)
                assert iso_date.month == int(mm), '{} != {}'.format(iso_date.month, mm)
                assert iso_date.year == int(yyyy), '{} != {}'.format(iso_date.year, yyyy)
                assert iso_date.day == int(dd), '{} != {}'.format(iso_date.day, dd)
                assert iso_date.hour == int(h), '{} != {}'.format(iso_date.hour, h)
                assert iso_date.minute == int(m), '{} != {}'.format(iso_date.minute, m)
            except Exception as e:
                print(e)
                continue
            if (the_files.get(iso_date) is None):
                the_files[iso_ts] = [filepath]
            else:
                the_files[iso_ts].append(filepath)
    return the_files


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
            print(str(ex))
        if (is_negative):
            value = -value
    return value

def decompress_gzip(fp=None):
    import gzip
    from vyperlogix.contexts import timer

    with timer.Timer() as timer3:
        diff = -1
        num_rows = -1
        __status__ = []
        print('BEGIN: decompress_gzip :: fp is "{}".'.format(fp))
        assert os.path.exists(fp) and os.path.isfile(fp), 'Cannot do much with the provided filename ("{}"). Please fix.'.format(fp)
        try:
            with gzip.open(fp, 'r') as infile:
                outfile_content = infile.read().decode('UTF-8')
            __status__.append({'gzip': True})
            print('INFO: decompress_gzip :: __status__ is {}.'.format(__status__))
        except Exception as ex:
            __status__.append({'gzip': False})
            print("Error in decompress_gzip.1 {}".format(ex))
        try:
            lines = [l.split() for l in outfile_content.split('\n')]
            rows = [{k:normalize_numeric(v) for k,v in dict(zip(lines[0], l)).items()} for l in lines[1:]]
            rows = [row for row in rows if (len(row) > 0)]
            diff = rows[-1].get('start', 0) - rows[0].get('start', 0)
            num_rows = len(rows)
        except Exception as ex:
            print("Error in decompress_gzip.2 {}".format(ex))
    msg = 'decompress_gzip :: {:.2f} secs'.format(timer3.duration)
    print(msg)
    return {'status': __status__[0], 'diff': diff, 'num_rows':num_rows, 'rows':rows}


def dicts_to_csv(dicts=None, fp=None):
    from vyperlogix.contexts import timer

    with timer.Timer() as timer3:
        print('BEGIN: dicts_to_csv :: fp is "{}".'.format(fp))
        assert isinstance(fp, StringIO) or (os.path.exists(fp) and os.path.isfile(fp)), 'Cannot do much with the provided filename ("{}"). Please fix.'.format(fp)
        fOut = open(fp, 'w') if (not isinstance(fp, StringIO)) else fp
        try:
            fOut.write(','.join(dicts[0].keys())+'\n')
            for d in dicts:
                fOut.write(','.join([str(d.get(k, '')) for k in d.keys()])+'\n')
            _status = True
        except Exception as ex:
            _status = False
        print('INFO: dicts_to_csv :: {}'.format(dicts))
    msg = 'dicts_to_csv :: {:.2f} secs'.format(timer3.duration)
    print(msg)
    return {'status': _status, 'num_rows':len(dicts), 'rows':dicts, 'is_StringIO': isinstance(fp, StringIO), 'csv':fp if (not isinstance(fp, StringIO)) else fp.getvalue()}


def write_to_db(data='mem,host=host1 used_percent=23.43234543'):
    token = 'vEwNWbBIQd4KsBnO4ge_4QMN_FlR_X5juqoctBbjH3Z5w6abJqZ1AfRMTn-McvdF9vgIp_DVw4xHo64aXGb20w=='
    org = "raychorn@gmail.com"
    bucket = "vpcflowlogs"

    try:
        with InfluxDBClient(url="https://us-east-1-1.aws.cloud2.influxdata.com", token=token, org=org) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            write_api.write(bucket, org, data)
            client.close()
    except Exception as ex:
        traceback.print_exc()
        

def write_point_to_db(point):
    # vpcflowlogs3
    bucket = "vpcflowlogs3"
    token = 'YeSvMwjCdYyorCIunw9itYRGAm9HELR4ED26SslUI0qyUZwjYfzyjsYisd3V_Nd-_UNnUHXiYCc104hVC_pOXQ=='
    # vpcflowlogs2
    #token = 'n77ag5hrxxrsQqWQEb1fpuqBoUKFVRxZXNtyobfkWS6zbvpZorBNCuv1kB0B-XSldo7UKS54xy8GwL7rlRSC3A=='
    org = "raychorn@gmail.com"

    try:
        with InfluxDBClient(url="https://us-east-1-1.aws.cloud2.influxdata.com", token=token, org=org) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            write_api.write(bucket, org, point)
            client.close()
    except Exception as ex:
        traceback.print_exc()
        


all_files = get_all_files_from(__data_root__)
recent_files = list(all_files.keys())
recent_files.sort()
most_recent_files = recent_files[-1000:]

most_recent_files_list = []
for _iso in most_recent_files:
    if (all_files.get(_iso) is not None):
        for f in all_files.get(_iso, []):
            most_recent_files_list.append(f)
print(len(most_recent_files_list))
print('-'*80)
print()

fpath = '{}/most_recent_files.csv'.format(os.path.dirname(__file__))
with open(fpath, 'w') as fOut:
    print('BEGIN:')
    i = 0
    _header = []
    defaults = '#default,0,0,,,,0,0,0,0,0,0,0,,,,'
    print(defaults, file=fOut)
    for f in most_recent_files_list:
        print(f)
        data = decompress_gzip(f)
        csv = dicts_to_csv(data.get('rows', []), StringIO())
        csv_rows = [l for l in csv.get('csv', '').split('\n') if (len(l) > 0)]
        assert (len(csv_rows) - 1) == len(data.get('rows', [])), 'The number of rows in the csv file is not equal to the number of rows in the decompressed file.'
        _ignores = ['account-id', 'interface-id']
        _quoted = []
        for _ii, l in enumerate(csv_rows[i:]):
            if (_ii > i):
                items = l.split(',')
                _data = dict(list(zip(_header, items)))
                data = []
                for _i,_h in enumerate(_header):
                    if (_h not in _ignores):
                        data.append('{}={}'.format(_h.replace('-', '_'), items[_i] if (not _h in _quoted) else '"{}"'.format(items[_i])))
                        #if (len(data) > 1):
                        #    break
                if (0):
                    l = '{},host={} '.format(_data.get('account-id', 'missing'), _data.get('interface-id', 'missing')) + ' '.join(data)  # mem, aws, accountid works.
                else:
                    point = Point(_data.get('account-id', 'missing')) \
                        .tag("host", _data.get('interface-id', 'missing'))
                        
                    for _d in data:
                        toks = _d.split('=')
                        point.field(toks[0], toks[-1] if (not str(toks[-1]).isdigit()) else eval(toks[-1]))
                        
                    point.time(datetime.utcnow(), WritePrecision.NS)

                if (__options__ == options.files):
                    print(l, file=fOut)
                elif (__options__ == options.db):
                    write_point_to_db(point)
            elif (len(_header) == 0):
                _header = l.split(',')
        if (i == 0):
            i += 1
    print('END!!!')
print(fpath)
