import os
import sys
import datetime

import pandas as pd
import numpy as np

date = '20220531'
imb_file = '/home/yuzhong/data/imbalance/%s/NyseImb_%s.csv.gz' % (date[:4], date)

time_1550 = datetime.datetime.strptime(date + ' 15:50:00', '%Y%m%d %H:%M:%S')
time_1600 = datetime.datetime.strptime(date + ' 16:00:00', '%Y%m%d %H:%M:%S')

df_imb = pd.read_csv(imb_file)
df_imb['timestamp'] = pd.to_datetime(df_imb['date']) + pd.to_timedelta(df_imb['time'])
df_imb['sym'] = df_imb['sym'].str.lstrip("'b").str.rstrip("'").str.replace('/', '.')
df_imb['side'] = df_imb['side'].str.lstrip("'b").str.rstrip("'")

df_imb = df_imb[(df_imb['timestamp'] >= time_1550) & (df_imb['timestamp'] <= time_1600)].reset_index(drop=True)


print(df_imb.columns)


print(df_imb)



