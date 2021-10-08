import os
import sys

import mujson as json

import numba
import numpy as np
import pandas as pd

import matplotlib
import matplotlib.pyplot as plt

import itertools
from itertools import permutations
from itertools import chain, combinations

from sympy.utilities.iterables import multiset_permutations

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


def all_subsets(ss):
    return chain(*map(lambda x: combinations(ss, x), range(0, len(ss)+1)))


def plot_the_data(freqs, image_fpath=None, title='***', xlabel='***', ylabel='***', xlim=None, ylim=None, save_path=None, export_plot=False):
    d_freqs = freqs.to_dict()

    headers = list(freqs.keys().names)
    d_freqs = {','.join(['{}-{}'.format(k,v) for k,v in dict([z for z in zip(headers, list(k))]).items()]):v for k, v in d_freqs.items() if (v > 0)}

    data = {'name' : list(d_freqs.keys()), 'freq' : list(d_freqs.values())}
    df = pd.DataFrame(data, index=None)
    df['percentile'] = df.freq.rank(pct=True)

    pcentile = 99.0
    min_num = len(df) / 100
    outliers = df[df.percentile < pcentile]
    history = []
    while (pcentile > 0.1) and (len(outliers) > min_num):
        outliers = df[df.percentile < pcentile]
        history.append(tuple([len(outliers), pcentile]))
        pcentile -= 0.1
        if (len(history) > 10):
            del history[0:len(history)-10]

    if (export_plot):
        scatter_plot_list([], freqs=d_freqs, title=title, xlabel=xlabel, ylabel=ylabel, image_fpath=image_fpath, xlim=xlim, ylim=ylim, save_path=save_path)

    jfpath = os.path.splitext(image_fpath)[0] + '.json'
    with open(jfpath, 'w') as fOut:
        if (1):
            print(outliers.to_json(orient='records'), file=fOut)
        else:
            pfreqs = {'{:.2f}'.format(k):v for k,v in protocols_freqs.items()}
            json.dump(d_freqs, f)

    assert os.path.exists(jfpath) and os.path.isfile(jfpath), '{} does not exist or is not a file'.format(jfpath)


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

    if (0):
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
    else:
        with timer.Timer() as timer0:
            with timer.Timer() as timer1:
                protocols_freqs = frequency_analysis_of(protocols)

            with timer.Timer() as timer2:
                protocols_freqs2 = df['protocol'].value_counts()

            print('protocols freq analysis: {} --> {:.2f} secs'.format(len(protocols), timer1.duration))

            print(protocols_freqs2)
            print('protocols freq analysis (df): {} --> {:.2f} secs'.format(len(protocols_freqs2), timer2.duration))

            numeric_cols = [n for n in df.select_dtypes(include=np.number).columns.tolist() if (not n.startswith('__metadata__.')) and (n not in ['version', 'account-id']) and (not n.startswith('__dataset_index__')) and (not n.startswith('Unnamed'))]
            print(numeric_cols)

            print('BEGIN: (1) numeric cols permutations')
            with timer.Timer() as timer3:
                d_numeric_cols = dict([tuple([list(item)[-1],list(item)[0]]) for item in enumerate(numeric_cols)])
                d_numeric_by_colnum = dict([item for item in enumerate(numeric_cols)])
                a = np.array(list(d_numeric_cols.values()))
                permutations = []
                if (0):
                    for p in multiset_permutations(a):
                        pp = [d_numeric_by_colnum.get(n) for n in p]
                        permutations.append(pp)
                        print(pp)
                else:
                    for subset in all_subsets(list(d_numeric_cols.values())):
                        if (len(subset) > 1):
                            pp = [d_numeric_by_colnum.get(n) for n in list(subset)]
                            permutations.append(pp)
                            print(pp)
            print('END!!! (1) numeric cols permutations')
            print('(1) numeric cols permutations: --> {:.2f} secs'.format(timer3.duration))

            with timer.Timer() as timer4:
                for p in permutations:
                    df_solution = df.pivot_table(index=p, aggfunc='size')
                    print(df_solution)
                    xtitle = '+'.join(p)
                    ifpath = os.path.join(os.path.dirname(__file__), charts_dirname, 'sx-vpclogss3-filtered-09-25-2021-expanded-protocols-freqs-{}.png'.format(xtitle.replace(' ', '_')))
                    plot_the_data(df_solution, image_fpath=ifpath, title=xtitle, xlabel='***', ylabel='***', xlim=None, ylim=None, save_path=None, export_plot=False)
            print('numeric cols freq analyses: --> {:.2f} secs'.format(timer4.duration))

            if (0):
                ifpath = os.path.join(os.path.dirname(__file__), charts_dirname, 'sx-vpclogss3-filtered-09-25-2021-expanded-protocols-freqs-{}.png'.format(0))
                scatter_plot_list([], freqs=protocols_freqs, title='protocols freqs analysis', xlabel='protocol', ylabel='frequency', image_fpath=ifpath, xlim=None, ylim=None, save_path=None)

                jfpath = os.path.splitext(ifpath)[0] + '.json'
                with open(jfpath, 'w') as f:
                    pfreqs = {'{:.2f}'.format(k):v for k,v in protocols_freqs.items()}
                    json.dump(pfreqs, f)

                assert os.path.exists(ifpath) and os.path.isfile(ifpath), '{} does not exist or is not a file'.format(ifpath)
    print('Analysis: --> {:.2f} secs'.format(timer0.duration))
