import requests
import apache_beam as beam
import logging
from itertools import chain

def get_all_stocks():
    full_url = 'https://financialmodelingprep.com/api/v3/company/stock/list'
    result = requests.get(full_url).json()["symbolsList"]
    stocks = list(map(lambda d: d['symbol'], result))
    logging.info('We fetched {} TICKERS'.format(len(stocks)))
    return stocks


def get_prices(ticker, iexapikey):
    try:
        iexurl = 'https://cloud.iexapis.com/stable/stock/{ticker}/quote?token={token}'.format(
                                    ticker=ticker, token=iexapikey)
        all_data =  requests.get(iexurl).json()

        return (ticker, all_data['close'], all_data['change'],
                all_data['week52High'], all_data['week52Low'],
                all_data['ytdChange'])

    except Exception as e:
        logging.info('Cannot find data for {}:{}'.format(ticker, str(e)))
        return ()

def is_above_52wk(input):
    if input[1] and input[3]:
        return input[1] > input[3]
    return False

def is_below_52wk(input):
    if input[1] and input[4]:
        return input[1] < input[4]
    return False

class MarketBreadthCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return (0.0, 0.0)

  def add_input(self, accumulator, input):
    higher = 1 if input[2]  and input[2] > 0 else 0
    lower = 1 if input[2] and input[2] < 0 else 0
    (hi_stock, lo_stock) = accumulator
    return hi_stock + higher, lo_stock + lower

  def merge_accumulators(self, accumulators):
    hi, lo = zip(*accumulators)
    return sum(hi), sum(lo)

  def extract_output(self, sum_count):
    (hi, lo) = sum_count
    return 'MARKET BREADTH:Higher:{}, Lower:{}, Breadth:{}'.format(hi,lo, hi/lo if lo !=0 else 1)

def combine_movers(values, label):
    return ','.join(values)


class Market52Week(beam.CombineFn):
  def create_accumulator(self):
    return (0.0, 0.0)

  def add_input(self, accumulator, input):
    (hi_stock, lo_stock) = accumulator
    if input[1] and input[3] and input[1] > input[3]:
        hi_stock.append(input[0])
    if input[1] and input[4] and input[1] < input[4]:
        lo_stock.append(input[0])

    return hi_stock, lo_stock

  def merge_accumulators(self, accumulators):
    hi, lo = zip(*accumulators)

    all_hi = chain(*hi)
    all_low = chain(*lo)
    return all_hi, all_low

  def extract_output(self, sum_count):
    (hi, lo) = sum_count
    return (hi,lo)



