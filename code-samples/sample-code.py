import os
import sys

import datetime
import numpy as np
import pandas as pd

'''
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

data_file = '/mnt/FourTB/data/sx-vpclogss3-filtered-09-25-2021-expanded.csv'

sys.path.insert(0, '/mnt/FourTB/__projects/vyperlogix/private_vyperlogix_lib3/')

from vyperlogix.contexts import timer

def load_data_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df

if (os.path.exists(data_file) and os.path.isfile(data_file)):
    df0 = load_data_from_csv(data_file)
    print('csv loader: {}'.format(len(df0)))
    print(df0)

    with timer.Timer() as timer1:
        firstDate = datetime.datetime(2021,6,10,0,0,0)
        lastDate = datetime.datetime(2021,6,17,0,0,0)
        date = firstDate
        date1 = firstDate
        bin = 1

        t_firstDate = firstDate.strftime("%Y-%m-%d %H:%M:%S")
        t_lastDate = lastDate.strftime("%Y-%m-%d %H:%M:%S")
        df = df0[(df0["start"] >= t_firstDate) & (df0["start"] < t_lastDate)]
        print(df.shape)

        df.start_dt = df.start.apply(lambda x: datetime.datetime.fromisoformat(x[:-1]))
        df.end_dt = df.end.apply(lambda x: datetime.datetime.fromisoformat(x[:-1]))

        #df[:1000]

        #empty dataframe
        df_d0 = df[(df["start"] >= '2021-06-14 00:00:00') & (df["start"] < '2021-06-14 00:00:00')]
        while (date < lastDate):
            date1 += datetime.timedelta(minutes=10)
            if date1.minute == 0:
                df_d = df[((df.start_dt.dt.date == date.date()) & (df.start_dt.dt.hour == date.hour) & ((df.start_dt.dt.minute >= date.minute) & (df.start_dt.dt.minute < 60)))]
            else:
                df_d = df[((df.start_dt.dt.date == date.date()) & (df.start_dt.dt.hour == date.hour) & ((df.start_dt.dt.minute >= date.minute) & (df.start_dt.dt.minute < date1.minute)))]
            #print(df_d.shape)
            #df_d = df[(df["start"].dt.date == date.date())]
            df_d = df_d.groupby(["dstport", "start"], as_index=False).sum()
            #print(df_d.shape)

            #BPP
            df_d["bpp"] = df_d["bytes"]/df_d["packets"]
            df_d["bpp_norm"] = np.log(df_d['bpp'])
            df_d["bpp_zscore+log"] = (df_d["bpp_norm"] - df_d["bpp_norm"].mean()) / df_d["bpp_norm"].std()

            #data = [df_d["dstport"], df_d["packets"], df_d["bytes"], df_d["bpp"]]
            #headers = ["dstport", "packets", "bytes", "bpp"]
            #df1 = pd. concat(data, axis=1, keys=headers)

            #downloading dfs on datetime strings
            #date_time = date.strftime('%Y-%m-%d %H:%M:%S') + ".csv"
            #date_time = date.strftime('%Y-%m-%d) + ".csv"
            #str1 = date_time+".csv"
            #df_d.to_csv(date_time)

            #MAKING BINS
            #df_d[["date", "hour"]] = df_d[["date", "hour"]].astype(str)
            df_d.start_dt = df_d.start.apply(lambda x: datetime.datetime.fromisoformat(x[:-1]))
            df_d["bin"] = df_d["start"] + " h-" + '{}'.format(df_d.start_dt.dt.hour) + " b-" + str(bin)

            df_d0 = df_d0.append(df_d, sort=False)

            #print(date1)
            #print(date)
            bin += 1
            if bin > 6:
                bin = 1
            date += datetime.timedelta(minutes=10)
        date -= datetime.timedelta(days=1)
        date_time = date.strftime('%Y-%m-%d') + "-10m-agg.csv"

        #str1 = date_time+".csv"
        df_d0.drop(df_d0.columns[[ 0, 1, 2, 4, 7, 8, 10, 11, 12]], axis = 1, inplace=True)
        df_d0.to_csv(date_time)

    print('runtime: {:.2f} secs'.format(timer1.duration))
