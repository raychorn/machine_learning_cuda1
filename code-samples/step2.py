import datetime 
import numpy as np
firstDate = datetime.datetime(2021,6,14,0,0,0)
lastDate  = datetime.datetime(2021,6,15,0,0,0)
 
date = firstDate
date1 = firstDate
bin = 1
#empty dataframe
df_d0 = df[(df["start"] >= '2021-06-14 00:00:00') & (df["start"] < '2021-06-14 00:00:00')]
while date < lastDate:
   date1+= datetime.timedelta(minutes=10)
   if date1.minute == 0:
     df_d = df[((df["start"].dt.date == date.date()) & (df["hour"] == date.hour) & ((df["minute"] >= date.minute) & (df["minute"] < 60)))]
   else:
     df_d = df[((df["start"].dt.date == date.date()) & (df["hour"] == date.hour) & ((df["minute"] >= date.minute) & (df["minute"] < date1.minute)))]
   #print(df_d.shape)
   
   #df_d = df[(df["start"].dt.date == date.date())]
   df_d = df_d.groupby(["dstport", "date", "hour"],  as_index=False).sum()
   print(df_d.shape)
   #print(bin)
   #for i in df_d.index:
   #print("packets: ", df_d.packets)
   #print(df_d.index.value_counts())
   #print("packets:", df_d["packets"])
   #print("bytes" , df_d["bytes"])
 
   #BPP
   df_d["bpp"] = df_d["bytes"]/df_d["packets"] 
   df_d["bpp_norm"] =  np.log(df_d['bpp'])
   df_d["bpp_zscore+log"] = (df_d["bpp_norm"] - df_d["bpp_norm"].mean()) / df_d["bpp_norm"].std()   
 
   #data = [df_d["dstport"], df_d["packets"], df_d["bytes"], df_d["bpp"]]
   #headers = ["dstport", "packets", "bytes", "bpp"]
   #df1 = pd. concat(data, axis=1, keys=headers)
   #df_d["pack_norm"] =  np.log(df_d['packets'])
 
   #df_d["bytes_norm"] =  np.log(df_d['bytes'])
   #df_d["pack_zscore+log"] = (df_d["pack_norm"] - df_d["pack_norm"].mean()) / df_d["pack_norm"].std() 
   #df_d["byte_zscore+log"] = (df_d["bytes_norm"] - df_d["bytes_norm"].mean()) / df_d["bytes_norm"].std()
 
   #plt.figure()
   #sns.distplot(df_d["pack_norm"], bins = 30,color="b"); # kde = False,
   #plt.figure()
   #sns.distplot(df_d["bytes_norm"], bins = 30,color="r");
   #print(df1)
 
   #downloading dfs on datetime strings 
   #date_time = date.strftime('%Y-%m-%d %H:%M:%S') + ".csv"
   #date_time = date.strftime('%Y-%m-%d) + ".csv"
   #str1 = date_time+".csv"
   #df_d.to_csv(date_time)
 
   #MAKING BINS 
   df_d[["date", "hour"]] = df_d[["date", "hour"]].astype(str)
   df_d["bin"] = df_d["date"] +" h-"+ df_d["hour"] + " b-" + str(bin)
   
   
   df_d0 = df_d0.append(df_d, sort=False)
   #x = df_d["bpp_zscore+log"]
   #sigma=3 * (np.std(x))
   #meu = x.mean()
   #p3sd = meu + sigma
   #m3sd = meu - sigma
   
   #df_d["3SD_anom_bpp"] = df_d["bpp_zscore+log"].apply(lambda x: "1" if ((x > p3sd) | (x < m3sd)) else "0")
   #print(df_d["3SD_anom_bpp"].value_counts())
   #df_d["3SD_anom_bpp"] = pd.to_numeric(df_d["3SD_anom_bpp"])
   #df_d =   df_d[df_d["3SD_anom_bpp"] < 1]
   
   #data_points = df_d["bpp_zscore+log"]
   #sm.qqplot(data_points, line ='45')
   #py.show() 
   print(date1)
   print(date) 
   bin+=1
   if bin > 6:
     bin = 1
   date += datetime.timedelta(minutes=10)
date -= datetime.timedelta(days=1)
date_time = date.strftime('%Y-%m-%d') + "-10m-agg.csv"
#str1 = date_time+".csv"
df_d0.drop(df_d0.columns[[ 0,1, 2, 4, 7, 8, 10, 11, 12]], axis = 1, inplace=True)
df_d0.to_csv(date_time)
