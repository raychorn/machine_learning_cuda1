import os
import sys

import mujson as json

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


def scatter_plot_list(data_list, freqs=None, image_fpath=None, title='***', xlabel='***', ylabel='***', xlim=None, ylim=None, save_path=None):
    rng = np.random.RandomState(0)
    if (freqs is None):
        with timer.Timer() as timer1:
            freqs = frequency_analysis_of(data_list)
        print('freqs analysis: {} rows --> {:.2f} secs'.format(len(data_list), timer1.duration))
    print('BEGIN: freqs.')
    for k,v in freqs.items():
        print('{} : {}'.format(k, v))
    print('END!!! freqs.')
    x = [i for i in freqs.keys()]
    y = [i for i in freqs.values()]
    colors = rng.rand(len(x))
    sizes = [i*1000 for i in y]

    matplotlib.use('Qt5Agg')
    plt.figure(figsize=(10, 10))

    plt.scatter(x, y, c=colors, s=sizes, alpha=0.3, cmap='viridis')
    plt.colorbar();
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if (image_fpath is not None):
        plt.savefig(image_fpath)
    else:
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


    charts_dirname = 'charts'

    iCnt = 0
    with timer.Timer() as timer0:
        for chunk_size in range(2,102,1): # len(protocols)-2
            with timer.Timer() as timer3:
                protocols_analysis = []
                for protocol_seg in group_elements(chunk_size,protocols):
                    m = np.mean(normalize_numbers(protocol_seg), dtype=np.float32)
                    protocols_analysis.append(m)
            print('protocols_analysis: {} --> {} in {:.2f} secs'.format(len(protocols_analysis), protocols_analysis[:10], timer3.duration))

            with timer.Timer() as timer1:
                protocols_freqs = frequency_analysis_of(protocols_analysis)

            ifpath = os.path.join(os.path.dirname(__file__), charts_dirname, 'sx-vpclogss3-filtered-09-25-2021-expanded-protocols-freqs-{}.png'.format(chunk_size))
            scatter_plot_list(protocols_analysis, freqs=protocols_freqs, title='protocols_analysis', xlabel='mean', ylabel='frequency', image_fpath=ifpath, xlim=None, ylim=None, save_path=None)

            jfpath = os.path.splitext(ifpath)[0] + '.json'
            with open(jfpath, 'w') as f:
                pfreqs = {'{:.2f}'.format(k):v for k,v in protocols_freqs.items()}
                json.dump(pfreqs, f)

            assert os.path.exists(ifpath) and os.path.isfile(ifpath), '{} does not exist or is not a file'.format(ifpath)
        iCnt += 1
    print('Analysis: {} iterations --> {:.2f} secs'.format(iCnt, timer0.duration))
