import datetime 
import numpy as np
firstDate = datetime.datetime(2021,6,14,0,0,0)
lastDate  = datetime.datetime(2021,6,15,0,0,0)
 
date = firstDate
date1 = firstDate
empty_df = pd.DataFrame()
bin = 1
while date < lastDate:
   #for upto minutes
   #date1+= datetime.timedelta(hours=1)
   #if date1.minute == 0:
   #  df_d = df[((df["start"].dt.date == date.date()) & (df["hour"] == date.hour) & ((df["minute"] >= date.minute) & (df["minute"] < 60)))]
   #else:
 
   
   #For tranforming data into min max  agg on dst port
   date_l = date.strftime('%Y-%m-%d') + "-10m-agg.csv"
 
   df_tran = pd.read_csv(date_l)
   print(df_tran.shape)
   df_trans = df_tran[df_tran["hour"] == date.hour]
   print(df_trans.shape)
   df_d = df[((df["start"].dt.date == date.date()) & (df["hour"] == date.hour)) ]
   print(df_d.shape)
   #destination port hourly frequency
   df_d = df_d.groupby("dstport", as_index=False).agg({"protocol":"count"}).rename({'protocol': 'dstport_hf'}, axis=1)
   result = df_trans.merge( df_d, on="dstport")
   df_d = result.groupby(['dstport', "date", "hour", "dstport_hf"], as_index=False).agg({'packets':['min', 'max', "mean", "median", "std"],
                         'bytes':['min', 'max', "mean", "median", "std"], 
                         'bpp':['min', 'max', "mean", "median", "std"]})
   #df_d = df_d.groupby(["dstport", "date", "hour"],  as_index=False).sum()
   
   #print(bin)
 
   #BPP
   #df_d["bpp"] = df_d["bytes"]/df_d["packets"] 
   #df_d["bpp_norm"] =  np.log(df_d['bpp'])
   #df_d["bpp_zscore+log"] = (df_d["bpp_norm"] - df_d["bpp_norm"].mean()) / df_d["bpp_norm"].std() 
   #df1 = pd. concat(data, axis=1, keys=headers)
 
   #new feature
   #df_d["saf_d"] = df_d.groupby('srcaddr')['srcaddr'].transform('count')
   #print(df_d.shape)
 
   #df_d["bsaf_d"] =  df_d["bytes"] / df_d["saf_d"]
   #df_d["psaf_d"] = df_d["packets"] / df_d["saf_d"]
 
   #Packet and bytes normalization
   #df_d["pack_norm"] =  np.log(df_d['packets'])
   #df_d["bytes_norm"] =  np.log(df_d['bytes'])
   #df_d["pack_zscore+log"] = (df_d["pack_norm"] - df_d["pack_norm"].mean()) / df_d["pack_norm"].std() 
   #df_d["byte_zscore+log"] = (df_d["bytes_norm"] - df_d["bytes_norm"].mean()) / df_d["bytes_norm"].std()
 
 
   #MAKING BINS 
   #df_d[["date", "hour"]] = df_d[["date", "hour"]].astype(str)
   #df_d["bin"] = df_d["date"] +" h-"+ df_d["hour"] + " b-" + str(bin)
 
   #APPENDING RESULTS IN CASE OF AGGREGATION
   empty_df = empty_df.append(df_d, sort=False)
   
   #BPP ANOMALIES
   
   #x = df_d["bpp_zscore+log"]
   #sigma=3 * (np.std(x))
   #meu = x.mean()
   #p3sd = meu + sigma
   #m3sd = meu - sigma
   
 
   date += datetime.timedelta(hours=1)
#print(df_d)
date_time = date.strftime('%Y-%m-%d') + "-rangedata.csv"
#str1 = date_time+".csv"
#df_d0.drop(df_d0.columns[[ 0,1, 2, 4, 7, 8, 10, 11, 12]], axis = 1, inplace=True)
#print(df_d.shape)
 
print(empty_df.columns)
empty_df.columns = empty_df.columns.to_flat_index()
#empty_df= empty_df.columns.to_flat_index()
print(empty_df.columns)
empty_df.to_csv(date_time, index=False)
 
