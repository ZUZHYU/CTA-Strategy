import numpy as np
import pandas as pd
from tqdm import tqdm
import requests
import time

url = "https://api.binance.com"
stable_coins = ["USDT"]
# Other possible stablecoins: USD1, USDP, BFUSD, USDE,"USDC", "BUSD", "TUSD", "FDUSD",'DAI'

# 1. Get all trading symbols
exchange_info = requests.get(f"{url}/api/v3/exchangeInfo").json()

symbol_pairs = set()
for s in exchange_info["symbols"]:
    if s["status"] == "TRADING" and s["isSpotTradingAllowed"]:
        if s["quoteAsset"] in stable_coins:
            symbol_pairs.add(s["symbol"])
print(f"Total symbols: {len(symbol_pairs)}")


def fetch_binance_ohlcv(symbol, interval, start_time, end_time):
    url = "https://api.binance.com/api/v3/klines"
    all_data = []

    while start_time < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 1000  # Max limit per request
        }
        response = requests.get(url, params=params)
        data = response.json()
        all_data.extend(data)

        if not data:
            break
        start_time = data[-1][0] + 1  # Add 1ms to avoid duplicates
    return all_data


def data_collection(token_symbol, interval='1m',start="2017-01-01",end='2026-02-19',freq='7D'):

    columns = ["Open Time", "Open", "High", "Low", "Close", "Volume", "Close Time", "Quote Asset Volume",
                "Number of Trades", "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"]


    dates = pd.date_range(start=start, end=end, freq=freq)
    date_strings = dates.strftime('%Y-%m-%d').tolist()

    price_data = pd.DataFrame()

    for i in range(len(date_strings)):
        if i==0:
            continue
        
        else:
            start_time=int(pd.Timestamp(date_strings[i-1]).timestamp() * 1000)
            end_time = int(pd.Timestamp(date_strings[i]).timestamp() * 1000)

            p = fetch_binance_ohlcv(token_symbol, interval, start_time, end_time)
            p = pd.DataFrame(p,columns=columns)
            price_data = pd.concat([price_data,p], ignore_index=True)


    price_data.to_csv(f'/local/scratch/yuzhang_utxo/token_price/{token_symbol}_minute_price.csv')

for token in tqdm(symbol_pairs):
    print(token)
    data_collection(token)
    time.sleep(5)
print('finished')

