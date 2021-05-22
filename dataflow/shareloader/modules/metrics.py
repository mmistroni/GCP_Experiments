import logging
logger = logging.getLogger(__name__)
from math import sqrt
import requests
import pandas as pd
import apache_beam as beam
from itertools import product
from functools import reduce
from datetime import date
import argparse
import logging
import re
import urllib
import json
import pandas as pd
from pandas.tseries.offsets import BDay
import pandas_datareader.data as dr
from datetime import datetime, date
import numpy as np
import math



def calculate_daily_returns(prices):
  return prices.pct_change(1)

def calculate_daily_cumulative_returns(daily_pc):
  return (1 + daily_pc).cumprod()

def compute_standard_deviation(daily):
  return daily.loc[:,daily.columns[0]].std()

def compute_sharpe_ratio(s_prices):
  # This function should be used in final dataframe
  # USE THSI FUNCTION
  dret = calculate_daily_returns(s_prices)
  avg = dret.loc[:,dret.columns[0]].mean()
  std = compute_standard_deviation(dret)
  return (sqrt(252) * avg) / std

def compute_moving_averages(prices, day):
  return prices.rolling(window=day).mean()

def check_prices_vs_moving_averages(prices, day=30):
  # This Function should be used  in final dataframe
  ma30 = compute_moving_averages(prices, 30)
  ticker_col = ma30.columns[0]
  m30_col = '{}M30'.format(ticker_col)
  ma30_renamed = ma30.rename({ticker_col: m30_col}, axis=1)
  concats = pd.concat([prices, ma30_renamed], axis=1)
  concats['AboveM30'] = concats[ticker_col] > concats[m30_col]
  above_m30 = concats[concats['AboveM30'] == True]
  total_prices = prices.shape[0]
  total_m30 = above_m30.shape[0]
  pcnt = 1.0*total_m30/total_prices
  return pcnt

def compute_data_performance(historical_df, ticker):
  # Use this FUNCTION CALL
  try:
    start = historical_df[ticker].values[0]
    end = historical_df[ticker].values[-1]
    return end*1.0/start - 1
  except Exception as e:
    logging.info('Could not find anything for {}:{}'.format(ticker, str(e)))
    return 0.0

def compute_metrics(prices):
  # Add Trade Volumne to spot momentum stocks
  ticker = prices.columns[0]

  perf_dict = {}
  perf_dict['Ticker'] = ticker
  try:
    perf_dict['Performance'] = compute_data_performance(prices, ticker)
    print('after perf calclation')
    perf_dict['Start_Price'] = prices[ticker].values[0]
    perf_dict['End_Price'] = prices[ticker].values[-1]
    #perf_dict['AboveMovingAvgPcnt'] = check_prices_vs_moving_averages(prices)
    #perf_dict['SharpeRatio'] = compute_sharpe_ratio(prices)
  except Exception as e:
    logging.info('Exception:{}'.format(str(e)))
    perf_dict['Performance'] = 0.0
    perf_dict['Start_Price'] = 0.0
    perf_dict['End_Price'] = 0.0
    perf_dict['AboveMovingAvgPcnt'] = 0.0
    perf_dict['SharpeRatio'] = 0.0

    #news_dict =calculate_news_sentiment(ticker)
  #news_measure = sum(news_dict['positive']) + sum(news_dict['negative'])
  #perf_dict['News_Sentiment'] = news_measure
  return pd.DataFrame([perf_dict])

def hurst_f(input_ts, lags_to_test=20):
  # interpretation of return value
  # hurst < 0.5 - input_ts is mean reverting
  # hurst = 0.5 - input_ts is effectively random/geometric brownian motion
  # hurst > 0.5 - input_ts is trending
  tau = []
  lagvec = []
  #  Step through the different lags
  for lag in range(2, lags_to_test):
      #  produce price difference with lag
      pp = np.subtract(input_ts[lag:], input_ts[:-lag])
      #  Write the different lags into a vector
      lagvec.append(lag)
      #  Calculate the variance of the differnce vector
      tau.append(np.sqrt(np.std(pp)))
  #  linear fit to double-log graph (gives power)
  m = np.polyfit(np.log10(lagvec), np.log10(tau), 1)
  # calculate hurst
  hurst = m[0]*2

  return hurst



def infer_ratings(json):
  rating_scale = float(json.get('ratingScaleMark'))
  txt = ''
  if rating_scale < 1.5:
    txt = 'STRONG-BUY'
  elif rating_scale <2:
    txt =  'BUY'
  elif rating_scale <2.5:
    txt =  'HOLD'
  else:
    txt =  'SELL'
  return '({}={})'.format(rating_scale, txt)


def get_marketcap(data_dict, token):
  ticker = data_dict['Ticker']
  logging.info('Enhancing with MarketCap..for :{}'.format(ticker))
  res = requests.get('https://cloud.iexapis.com/stable/stock/{}/stats/?token={}'.format(ticker, token)).json()
  mcap = res.get('marketcap', 0)
  beta = res.get('beta', 0)
  peRatio = res.get('peRatio', 0)
  new_dict = data_dict
  new_dict['marketCap'] = mcap
  new_dict['beta'] = beta
  new_dict['peRatio'] = peRatio
  return new_dict

def get_analyst_recommendations(data_dict, token):

  ticker = data_dict['Ticker']
  new_dict = data_dict
  try:
    analyst_url = \
      'https://cloud.iexapis.com/stable/stock/{symbol}/recommendation-trends?token={token}'.format(symbol=ticker, token=token)
    logging.info('Analyst URL is:{}.'.format(analyst_url))
    json = requests.get(analyst_url).json()
    logging.info('======calling analys trecomm. for {}.got:{}'.format(ticker, json))

    if len(json) > 0:
      ratings =  infer_ratings(json[0])
    else:
      ratings= 'N/A'
    new_dict['Ratings'] = ratings
    logging.info('data dict is:{}'.format(new_dict))

  except Exception as e :
    logging.info('Could not find print ratings for:{}:{}'.format(ticker, str(e)))
    new_dict['Ratings'] = 'N/A'
  return get_marketcap(new_dict, token)

class AnotherLeftJoinerFn(beam.DoFn):

  def __init__(self):
    super(AnotherLeftJoinerFn, self).__init__()

  def process(self, row, **kwargs):
    print('kw args is :{}'.format(kwargs))
    right_dict = dict(kwargs['right_list'])
    left_key = row[0]
    left = row[1]
    if left_key in right_dict:
      print('Row is:{}'.format(row))
      right = right_dict[left_key]
      left.update(right)
      yield (left_key, left)

class Display(beam.DoFn):
  def process(self, element):
    logging.info(str(element))
    yield element

def output_fields(input_dict):
    return dict((k,v) for k,v in input_dict.items())

def merge_dicts(input):
    d1, d2 = input
    d1.update(d2)
    return d1

def join_lists(lst):
  logging.info('Input list is:{}'.format(lst))
  k, v = lst
  if v.get('perflist') and v.get('edgarlist'):
    return product(v['perflist'], v['edgarlist'])



def get_date_ranges(prev_bus_days):
    logging.info('Checking result sfor {}'.format(prev_bus_days))
    end_date = date.today()
    start_date = end_date - BDay(
        prev_bus_days)  # go back 3 months to find crossovers. Start to plan to move to Dataflow
    return start_date, end_date


def get_historical_data_yahoo(symbol, sector, start_dt, end_dt):
    try:
        logging.info('Finding dta between {} and {}'.format(start_dt, end_dt))
        data = dr.get_data_yahoo(symbol, start_dt, end_dt)[['Adj Close']]
        df = data.rename(columns={'Adj Close': symbol})
        df['COB'] = date.today().strftime('%Y-%m-%d')
        #df['sector'] = sector
        return df
    except Exception as e:
        return pd.DataFrame(columns=[symbol])


def get_historical_data_yahoo_2(symbol, sector, start_dt, end_dt):
  try:
    data = dr.get_data_yahoo(symbol, start_dt, end_dt)[['Adj Close']]
    ret = data['Adj Close'].values[0]
    logging.info('YH2.Finding dta between {} and {} = {}'.format(start_dt, end_dt, ret))
    return ret
  except Exception as e:
    return 0


def get_return(ticker, start_date, end_date):
  try:
    logging.info('Getting Return between {} and {}'.format(start_date, end_date))
    data = dr.get_data_yahoo(ticker, start_date, end_date)[['Adj Close']]
    all_dt = data.iloc[[0, -1]]
    return (np.log(all_dt) - np.log(all_dt.shift(1)))['Adj Close'].values[-1]

  except Exception as e:
    logging.info('Exception for {},  {}:{}'.format(ticker, start_date, str(e)))
    return None

