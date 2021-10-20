from genericpath import isdir
import os
import sys

import shutil

import datetime
import numpy as np
import pandas as pd

'''
This program processes one batch at a time using a process for each.

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
processed_dir = '/mnt/FourTB/data/sx-vpclogss3-filtered-09-25-2021-expanded-processed'

sys.path.insert(0, '/mnt/FourTB/__projects/vyperlogix/private_vyperlogix_lib3/')

from vyperlogix.contexts import timer

from sample_funcs import process_data
from sample_funcs import load_data_from_csv

def get_files(dir):
    return [os.path.join(dir, f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]

def process_some_data(data_file, output=None, verbose=False):
    with timer.Timer() as timer0:
        print('processing:', data_file)
        df0 = load_data_from_csv(data_file)
        df0.start = df0.start.apply(lambda x: datetime.datetime.fromisoformat(x[:-1]))
        df0.end = df0.end.apply(lambda x: datetime.datetime.fromisoformat(x[:-1]))
        min_start_dt = df0.start.min()
        max_end_dt = df0.end.max()
        if (verbose):
            print('csv loader: {}'.format(len(df0)))
            print(df0)
        assert min_start_dt < max_end_dt, 'min_start_dt: {} max_end_dt: {}'.format(min_start_dt, max_end_dt)
        process_data(min_start_dt, max_end_dt, df0, fromisoformat=None, verbose=False)
        if (output is not None) and (os.path.exists(output)) and (os.path.isdir(output)):
            new_file = os.path.join(output, os.path.basename(data_file))
            df0.to_csv(new_file, index=False)
    print('runtime - load data file ("{}"): {:.2f} secs'.format(data_file, timer0.duration))
    return {'data_file': data_file, 'runtime': timer0.duration}

if (os.path.exists(data_dir) and os.path.isdir(data_dir)):
    if (os.path.exists(processed_dir) and os.path.isdir(processed_dir)):
        shutil.rmtree(processed_dir)
    os.makedirs(processed_dir)

    import concurrent.futures as concurrent_futures
    from concurrent.futures import ProcessPoolExecutor, wait
    from multiprocessing import cpu_count
    try:
        workers = cpu_count()
    except NotImplementedError:
        workers = 1
    print('workers: {}'.format(workers))

    print('BEGIN:')
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_some_data, f, output=processed_dir) for f in get_files(data_dir)}

        num_runs = 0
        total_runtime = 0.0
        for fut in concurrent_futures.as_completed(futures):
            d = fut.result()
            total_runtime += d.get('runtime', 0.0)
            num_runs += 1
            print(f"The outcome is {fut.result()}")
    print('END!!!')

    print('num_runs: {:d} secs'.format(num_runs))
    print('total runtime: {:.2f} secs'.format(total_runtime))

    if (0):
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
