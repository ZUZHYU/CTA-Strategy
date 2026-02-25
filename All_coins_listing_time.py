# Extracting all coins first listing times on Binance using their price data.
# The earliest date when the coin's price is shown on Binance is considered its listing time.

import pandas as pd
from datetime import datetime
import pickle
from glob import glob

file = '/local/scratch/yuzhang_utxo/token_price'    #directory where all tokens' price time series data are located.
files = glob(file+'/*_minute_price.csv')            #we downloaded minute-level price data and daily-level price data.
print(len(files))

token_list_time = dict()

for f in files:
    print(f)
    symbol = f.split("/")[-1].split("_")[0]
    tp = pd.read_csv(f,header=0)
    timestamp = int(tp.loc[0][1]/1000)
    dt = datetime.fromtimestamp(timestamp)
    token_list_time[symbol]=dt

with open(file+'/all_tokens_listing_time.csv','wb') as f:
    pickle.dump(token_list_time,f)

print('finised')
