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
Include (1) remove duplicate data
(2) insert missing data with interpolate methods
"""

file_path = '/local/scratch/yuzhang_utxo/token_price'
files = glob(file_path+'/*_minute_price.csv')             #all coins' price time series data
print(len(files))

for f in tqdm(files):

    coin_usdt = pd.read_csv(f,header=0)

    #(1) remove duplicate rows
    coin_usdt = coin_usdt.drop_duplicates(subset=['Close Time'], ignore_index=True)

    min_close_timestamp = int(coin_usdt.loc[0]['Close Time'])  #2017-08-17 00:00:00
    max_close_timestamp = int(coin_usdt.loc[len(coin_usdt)-1]['Close Time'])  #1771113659999  #2026-02-12 00:00:00
    standard_timestamp_ruler = np.arange(min_close_timestamp,max_close_timestamp,step=60000)
    standard_timestamp_ruler = np.concatenate([standard_timestamp_ruler,[max_close_timestamp]])

    #(2) insert missing dates
    coin_usdt = coin_usdt.reindex(standard_timestamp_ruler)
    columns = np.array(coin_usdt.columns)

    #(3) insert values with interpolate methods
    coin_usdt = coin_usdt.interpolate(method='linear')
    # coin_usdt[columns] = coin_usdt[columns].interpolate()

    with open(f,'wb') as f:
        pickle.dump(coin_usdt,f)

print('finished')