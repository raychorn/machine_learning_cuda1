import os
import sys

import shutil

import datetime
import numpy as np
import pandas as pd

'''
This program splits the monolithic data file (csv) into multiple files for the purpopse of parallelizing the data processing.

csv loader: 7657594
         Unnamed: 0                       _id  version  ...  __metadata__.srcaddr       __metadata__.owner __metadata__.dstaddr
0                 0  610860aa6ad3288383397ffe        2  ...        52.217.160.233                      LAN        172.31.46.113
1                 1  610860aa6ad328838339800d        2  ...          45.79.86.211                      LAN        172.31.46.113
2                 2  610860aa6ad3288383398005        2  ...         172.31.46.113  LINODE-AP Linode LLC US         45.79.86.211
3                 3  610860aa6ad3288383397fff        2  ...          45.79.94.209                      LAN        172.31.46.113
4                 4  610860aa6ad3288383397ffb        2  ...          52.217.161.9                      LAN        172.31.46.113
...             ...                       ...      ...  ...                   ...                      ...                  ...
7657589     7657589  610af5c55b181dc127045bd3        2  ...         172.31.40.160             AMAZON-02 US         52.46.138.63
7657590     7657590  610af5c55b181dc127045bc7        2  ...          61.3.238.248                      LAN         172.31.45.71
7657591     7657591  610af5c55b181dc127045bd6        2  ...         172.31.40.160             AMAZON-02 US         52.46.138.63
7657592     7657592  610af5c55b181dc127045bdc        2  ...          107.2.158.82                      LAN         172.31.33.15
7657593     7657593  610af5c55b181dc127045be0        2  ...        222.186.42.213                      LAN         172.31.45.71

[7657594 rows x 33 columns]
(193672, 33)
/mnt/FourTB/__projects/vyperlogix/machine_learning_cuda1/code-samples/sample-code.py:35: UserWarning: Pandas doesn't allow columns to be created via a new attribute name - see https://pandas.pydata.org/pandas-docs/stable/indexing.html#attribute-access
  df.start_dt = df.start.apply(lambda x: datetime.datetime.fromisoformat(x[:-1]))
/mnt/FourTB/__projects/vyperlogix/machine_learning_cuda1/code-samples/sample-code.py:36: UserWarning: Pandas doesn't allow columns to be created via a new attribute name - see https://pandas.pydata.org/pandas-docs/stable/indexing.html#attribute-access
  df.end_dt = df.end.apply(lambda x: datetime.datetime.fromisoformat(x[:-1]))
/mnt/FourTB/__projects/vyperlogix/machine_learning_cuda1/code-samples/sample-code.py:70: UserWarning: Pandas doesn't allow columns to be created via a new attribute name - see https://pandas.pydata.org/pandas-docs/stable/indexing.html#attribute-access
  df_d.start_dt = df_d.start.apply(lambda x: datetime.datetime.fromisoformat(x[:-1]))
runtime: 2134.83 secs
'''

data_dir = '/mnt/FourTB/data/sx-vpclogss3-filtered-09-25-2021-expanded'

data_file = '{}/sx-vpclogss3-filtered-09-25-2021-expanded.csv'.format(os.path.dirname(data_dir))

sys.path.insert(0, '/mnt/FourTB/__projects/vyperlogix/private_vyperlogix_lib3/')

from vyperlogix.contexts import timer

def load_data_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df

if (os.path.exists(data_file) and os.path.isfile(data_file)):
    if (os.path.exists(data_dir) and os.path.isdir(data_dir)):
        shutil.rmtree(data_dir)

    os.makedirs(data_dir)
    
    with timer.Timer() as timer0:
        df0 = load_data_from_csv(data_file)
        print('csv loader: {}'.format(len(df0)))
        print(df0)
    print('runtime - load data file ("{}"): {:.2f} secs'.format(data_file, timer0.duration))

    chunk = 0
    with timer.Timer() as timer1:
        with timer.Timer() as timer2:
            dataframes = np.array_split(df0, 10)
        print('runtime - np.array_split: {:.2f} secs'.format(timer2.duration))

        for df in dataframes:
            print(df.shape)

            fpath = '{}/sx-vpclogss3-filtered-09-25-2021-expanded-{}.csv'.format(data_dir, chunk)
            df.to_csv(fpath, index=False)

            chunk += 1

    print('runtime to chunk the data into {} chunks: {:.2f} secs'.format(chunk+1, timer1.duration))
