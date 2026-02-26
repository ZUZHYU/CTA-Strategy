import numpy as np
import pandas as pd
import requests
import pickle
from glob import glob
from datetime import datetime
from tqdm import tqdm

"""
Run the file 'All_coins_price_collection_minute.py' first to collect all coins minute-level price data.

Then run this script to handling price data. 
Include (1) remove duplicate data. Sort the data by 'Close Time' first, then remove duplicate data by 'Close time'. 
At last, set the Close time as index
(2) build the full-date index for close time and reset it as index
(3) insert missing data with interpolate methods
"""

file_path = '/local/scratch/yuzhang_utxo/token_price'


price_level = 'minute' #or 'daily'
step_size = 60000 if price_level=='minute' else 60000*60*24

files = glob(file_path+'/*_'+price_level+'_price.csv')             #all coins' price time series data
print(len(files))

for f in tqdm(files):
    try:
        coin_usdt = pd.read_csv(f,header=0)
    except:
        print(f)
        continue

    #(1) remove duplicate rows
    coin_usdt = coin_usdt.sort_values('Close Time', ignore_index=True)
    coin_usdt = coin_usdt.drop_duplicates(subset=['Close Time'], ignore_index=True)

    min_close_timestamp = int(coin_usdt.loc[0]['Close Time'])  #2017-08-17 00:00:00
    max_close_timestamp = int(coin_usdt.loc[len(coin_usdt)-1]['Close Time'])  #1771113659999  #2026-02-12 00:00:00
    standard_timestamp_ruler = np.arange(min_close_timestamp,max_close_timestamp,step=step_size)
    standard_timestamp_ruler = np.concatenate([standard_timestamp_ruler,[max_close_timestamp]])

    #(2) insert missing dates
    coin_usdt = coin_usdt.set_index('Close Time')
    coin_usdt = coin_usdt.reindex(standard_timestamp_ruler)
    # columns = np.array(coin_usdt.columns)
    columns = ['Open', 'High', 'Low' ,'Close', 'Volume' ,'Quote Asset Volume', 
                'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume']

    #(3) insert values with interpolate methods
    coin_usdt = coin_usdt.interpolate(method='linear')
    # coin_usdt[columns] = coin_usdt[columns].interpolate()

    nf = f[:40] + 'new_'+ f[40:]
    coin_usdt.to_csv(nf)

print('finished')