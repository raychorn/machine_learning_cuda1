import datetime

import pandas as pd
import numpy as np

def load_data_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df

def process_data(firstDate, lastDate, df0, fromisoformat=datetime.datetime.fromisoformat, verbose=False):
    date = firstDate
    date1 = firstDate
    bin = 1

    __is_doing_date_conversions = False
    if (callable(fromisoformat)):
        __is_doing_date_conversions = True
        t_firstDate = firstDate.strftime("%Y-%m-%d %H:%M:%S")
        t_lastDate = lastDate.strftime("%Y-%m-%d %H:%M:%S")
        df = df0[(df0["start"] >= t_firstDate) & (df0["start"] < t_lastDate)]
        if (verbose):
            print(df.shape)

        df.start = df.start.apply(lambda x: fromisoformat(x[:-1]))
        df.end = df.end.apply(lambda x: fromisoformat(x[:-1]))
    else:
        df = df0[(df0["start"] >= firstDate) & (df0["start"] < lastDate)]

    #empty dataframe
    df_d0 = df[(df["start"] >= '2021-06-14 00:00:00') & (df["start"] < '2021-06-14 00:00:00')]
    while (date < lastDate):
        date1 += datetime.timedelta(minutes=10)
        if date1.minute == 0:
            df_d = df[((df.start.dt.date == date.date()) & (df.start.dt.hour == date.hour) & ((df.start.dt.minute >= date.minute) & (df.start.dt.minute < 60)))]
        else:
            df_d = df[((df.start.dt.date == date.date()) & (df.start.dt.hour == date.hour) & ((df.start.dt.minute >= date.minute) & (df.start.dt.minute < date1.minute)))]
        df_d = df_d.groupby(["dstport", "start"], as_index=False).sum()

        #BPP
        df_d["bpp"] = df_d["bytes"]/df_d["packets"]
        df_d["bpp_norm"] = np.log(df_d['bpp'])
        df_d["bpp_zscore+log"] = (df_d["bpp_norm"] - df_d["bpp_norm"].mean()) / df_d["bpp_norm"].std()

        if (__is_doing_date_conversions):
            df_d.start = df_d.start.apply(lambda x: datetime.datetime.fromisoformat(x[:-1]))
        df_d["bin"] = (df_d.start.dt.date.to_string() if (not __is_doing_date_conversions) else df_d["start"]) + " h-" + '{}'.format(df_d.start.dt.hour) + " b-" + str(bin)

        df_d0 = df_d0.append(df_d, sort=False)

        bin += 1
        if bin > 6:
            bin = 1
        date += datetime.timedelta(minutes=10)
    date -= datetime.timedelta(days=1)
    date_time = date.strftime('%Y-%m-%d') + "-10m-agg.csv"

    df_d0.drop(df_d0.columns[[ 0, 1, 2, 4, 7, 8, 10, 11, 12]], axis = 1, inplace=True)
    
    return df_d0
