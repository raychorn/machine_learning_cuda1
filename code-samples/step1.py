# Imports
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from scipy.stats import norm
import statsmodels.api as sm
 
import pylab as py

df  =  pd.read_csv('/content/vpc-flowlogs-filtered.csv')
# Dropping unwanted columns
df.drop(df.columns[[ 0,1, 2, 3, 13, 14, 15, 16, 17, 18, 19, 20]], axis = 1, inplace=True)

 
#converting unix timestamp to datatime 
df['start'] = pd.to_datetime(df['start'],unit='s')
df['end'] = pd.to_datetime(df['end'],unit='s')
df['hour']   = df.start.dt.hour.astype('uint8')
df['minute'] = df.start.dt.minute.astype('uint8')
 
df['second'] = df.start.dt.second.astype('uint8')
df['duration'] = df['end'] - df['start']
 
df["date"] = df.start.dt.date

