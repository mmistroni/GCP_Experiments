import requests
import apache_beam as beam
import logging
from itertools import chain

def get_all_stocks(iexapikey):
    logging.info('Getting all stocks')
    all_stocks =  get_all_us_stocks(iexapikey)
    logging.info('We got:{}'.format(len(all_stocks)))
    return all_stocks

def get_all_us_stocks(token, security_type='cs', nasdaq=True):

    logging.info('Getting all stocks...')
    nyse_symbols = requests.get('https://cloud.iexapis.com/stable/ref-data/exchange/nys/symbols?token={token}'.format(token=token)).json()
    logging.info('Got:{}'.format(len(nyse_symbols)))
    nas_symbols = requests.get('https://cloud.iexapis.com/stable/ref-data/exchange/nas/symbols?token={token}'.format(token=token)).json()
    logging.info('Got:{}'.format(len(nas_symbols)))
    all_symbols = nyse_symbols + nas_symbols
    stocks =  [d['symbol'] for d in all_symbols  if d['isEnabled'] and d['type'].lower() in ['ad', 'cs', 'et']]
    logging.info('We picked up:{} out of {}'.format(len(stocks), len(all_symbols)))
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



