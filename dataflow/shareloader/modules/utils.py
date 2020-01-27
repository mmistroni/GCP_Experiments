import json
import pandas as pd
from pandas.tseries.offsets import BDay
import pandas_datareader.data as dr
import numpy as np
from datetime import datetime, date
import requests

def get_all_shares_dataframe():
  all_shares = requests.get('https://k1k1xtrm88.execute-api.us-west-2.amazonaws.com/test/query-shares').json()
  ds = [d for d in all_shares if d['QTY'] > 1]
  return pd.DataFrame.from_dict(ds)


def get_latest_price_yahoo(symbol, cob_date):
    try:  #
        print('--latest price for{}'.format(symbol))
        start_date = cob_date - BDay(1)
        dfy = dr.get_data_yahoo(symbol, start_date, start_date)[['Adj Close']]
        dft = dr.get_data_yahoo(symbol, cob_date, cob_date)[['Adj Close']]
        dfy['symbol'] = symbol
        dft['symbol'] = symbol
        print(dfy.shape)
        print(dft.shape)

        merged = pd.merge(dft, dfy, on='symbol', suffixes=('_t', '_y'), )
        merged['diff'] = merged['Adj Close_t'] - merged['Adj Close_y']
        print('Merged shap eis:{}'.format(merged.shape))
        return merged.iloc[0].to_dict()


    except Exception as e:
        print('Unable to find data for {}'.format(symbol))
        return pd.DataFrame.from_dict(
            {'symbol': [symbol], 'Adj Close_t': [0], 'Adj Close_y': [0], 'diff': [0]}).to_dict()


def get_prices(symbols):
    prices_dfs = (get_latest_price_yahoo(symbol, date.today()) for symbol in symbols)
    all_data = pd.concat(prices_dfs)
    return all_data

