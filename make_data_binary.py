import os
import sys

import numba
import numpy as np
import pandas as pd

import matplotlib
import matplotlib.pyplot as plt

from itertools import zip_longest

sys.path.insert(0, '/run/media/raychorn/BigDisk11/__projects/vyperlogix/private_vyperlogix_lib3')

from vyperlogix.contexts import timer

data_file = '/run/media/raychorn/BigDisk11/data/sx-vpclogss3-filtered-09-25-2021-expanded.csv'


normalize_numbers = lambda x: [int(i) for i in x if (all([ch.isdigit() for ch in str(i)]))]

#@numba.jit(nopython=True)
def load_data_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df

def group_elements(n, iterable, padvalue='x'):
    return zip_longest(*[iter(iterable)]*n, fillvalue=padvalue)


#@numba.jit(nopython=True)
def frequency_analysis_of(data):
    freqs = {}
    for i in data:
        freqs[i] = freqs.get(i, 0) + 1
    return freqs


def scatter_plot_list(data_list, title='***', xlabel='***', ylabel='***', xlim=None, ylim=None, save_path=None):
    rng = np.random.RandomState(0)
    with timer.Timer() as timer1:
        freq = frequency_analysis_of(data_list)
    print('freq analysis: {} rows --> {:.2f} secs'.format(len(data_list), timer1.duration))
    print('BEGIN: Freq.')
    for k,v in freq.items():
        print('{} : {}'.format(k, v))
    print('END!!! Freq.')
    x = [i for i in freq.keys()]
    y = [i for i in freq.values()]
    colors = rng.rand(len(x))
    sizes = [i*1000 for i in y]

    matplotlib.use('Qt5Agg')
    plt.figure(figsize=(10, 10))

    plt.scatter(x, y, c=colors, s=sizes, alpha=0.3, cmap='viridis')
    plt.colorbar();
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show()


if (os.path.exists(data_file) and os.path.isfile(data_file)):
    with timer.Timer() as timer1:
        df = load_data_from_csv(data_file)
    print('csv loader: {} --> {:.2f} secs'.format(len(df), timer1.duration))
    print(df)

    if (0):
        print('BEGIN:')
        for col in df.columns:
            print(col)
        print('END!!!')

    with timer.Timer() as timer2:
        protocols = df['protocol'].tolist()
    print('protocols: {} --> {} in {:.2f} secs'.format(len(protocols), protocols[:10], timer2.duration))

    with timer.Timer() as timer3:
        protocols_analysis = []
        for protocol_seg in group_elements(10,protocols):
            m = np.mean(normalize_numbers(protocol_seg), dtype=np.float32)
            protocols_analysis.append(m)
            #print('{} --> {}'.format(protocol_seg, m))
    print('protocols_analysis: {} --> {} in {:.2f} secs'.format(len(protocols_analysis), protocols_analysis[:10], timer3.duration))

    scatter_plot_list(protocols_analysis, title='protocols_analysis', xlabel='mean', ylabel='frequency', xlim=None, ylim=None, save_path=None)
