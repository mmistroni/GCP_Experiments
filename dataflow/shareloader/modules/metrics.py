

import logging
logger = logging.getLogger(__name__)
from math import sqrt
from pprint import pprint
from datetime import date
import pandas as pd

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
    perf_dict['Start_Price'] = prices[ticker].values[0]
    perf_dict['End_Price'] = prices[ticker].values[-1]
    perf_dict['AboveMovingAvgPcnt'] = check_prices_vs_moving_averages(prices)
    perf_dict['SharpeRatio'] = compute_sharpe_ratio(prices)
  except Exception as e:
    perf_dict['Performance'] = 0.0
    perf_dict['Start_Price'] = 0.0
    perf_dict['End_Price'] = 0.0
    perf_dict['AboveMovingAvgPcnt'] = 0.0
    perf_dict['SharpeRatio'] = 0.0

    #news_dict =calculate_news_sentiment(ticker)
  #news_measure = sum(news_dict['positive']) + sum(news_dict['negative'])
  #perf_dict['News_Sentiment'] = news_measure
  return pd.DataFrame([perf_dict])
