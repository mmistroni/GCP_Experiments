import requests
import apache_beam as beam
import logging
from itertools import chain
from bs4 import BeautifulSoup# Move to aJob
import requests
from itertools import chain
from io import StringIO
from datetime import date, timedelta
from pandas.tseries.offsets import BDay



def create_bigquery_ppln(p, label):
    cutoff_date = (date.today() - BDay(5)).date().strftime('%Y-%m-%d')
    logging.info('Cutoff is:{}'.format(cutoff_date))
    edgar_sql = """SELECT AS_OF_DATE, LABEL, VALUE  FROM `datascience-projects.gcp_shareloader.market_stats` 
WHERE  PARSE_DATE("%F", AS_OF_DATE) > PARSE_DATE("%F", "{cutoff}")  
AND LABEL IN ('MANUFACTURING-PMI', 'PMI','NASDAQ GLOBAL SELECT_MARKET BREADTH',
  'VIX', 'NEW YORK STOCK EXCHANGE_MARKET BREADTH' ) 
ORDER BY LABEL ASC, PARSE_DATE("%F", AS_OF_DATE) ASC 
  """.format(cutoff=cutoff_date, label=label)
    logging.info('executing SQL :{}'.format(edgar_sql))
    return (p | 'Reading-{}'.format(label) >> beam.io.Read(beam.io.BigQuerySource(query=edgar_sql, use_standard_sql=True))
              
           )

class InnerJoinerFn(beam.DoFn):
    def __init__(self):
        super(InnerJoinerFn, self).__init__()

    def process(self, row, **kwargs):
        right_dict = dict(kwargs['right_list'])
        left_key = row[0]
        left = row[1]
        if left_key in right_dict:
            right = right_dict[left_key]
            left.update(right)
            yield (left_key, left)


def get_all_stocks(iexapikey):
    logging.info('Getting all stocks')
    all_stocks =  get_all_us_stocks(iexapikey)
    logging.info('We got:{}'.format(len(all_stocks)))
    return all_stocks


def get_all_us_stocks(token, security_type='cs', nasdaq=True):
    logging.info('GEt All Us stocks..')
    all_dt = requests.get('https://financialmodelingprep.com/api/v3/available-traded/list?apikey={}'.format(token)).json()
    us_stocks =  [d['symbol'] for d in all_dt if d['exchange'] in ["New York Stock Exchange", "Nasdaq Global Select"]]
    logging.info('Got:{} Stocks'.format(len(us_stocks)))
    return us_stocks


def get_all_us_stocks2(token, exchange):
    logging.info('GEt All Us stocks..')
    all_dt = requests.get('https://financialmodelingprep.com/api/v3/available-traded/list?apikey={}'.format(token)).json()
    us_stocks =  [d['symbol'] for d in all_dt if d['exchange']  == exchange] # ["New York Stock Exchange", "Nasdaq Global Select"]]
    logging.info('Got:{} Stocks'.format(len(us_stocks)))
    return us_stocks

def get_all_prices_for_date(apikey, asOfDate):
    import pandas as pd
    url = 'https://financialmodelingprep.com/api/v4/batch-request-end-of-day-prices?date={}&apikey={}'.format(asOfDate,
                                                                                                        apikey)
    s = requests.get(url).content
    bulkRequest = pd.read_csv(StringIO(s.decode('utf-8')), header=0)
    return bulkRequest.to_dict('records')

class PutCallRatio(beam.DoFn):
    def get_putcall_ratios(self):
        r = requests.get('https://markets.cboe.com/us/options/market_statistics/daily/')
        bs = BeautifulSoup(r.content, 'html.parser')
        from itertools import chain
        div_item = bs.find_all('div', {"id": "daily-market-stats-data"})[0]
        ratios_table = div_item.find_all('table', {"class": "data-table--zebra"})[0]

        data = [[item.text for item in row.find_all('td')] for row in ratios_table.find_all('tr')]

        return [tuple(lst) for lst in data if lst]

    def process(self, element):
        return self.get_putcall_ratios()


class ParseManufacturingPMI(beam.DoFn):

    def get_manufacturing_pmi(self):
        r = requests.get('https://tradingeconomics.com/united-states/business-confidence',
                         headers={'user-agent': 'my-app/0.0.1'})
        bs = BeautifulSoup(r.content, 'html.parser')
        div_item = bs.find_all('div', {"id": "ctl00_ContentPlaceHolder1_ctl00_ctl01_Panel1"})[0]  #
        tbl = div_item.find_all('table', {"class": "table"})[0]
        vals = [[item.text.strip() for item in row.find_all('td')] for row in tbl.find_all('tr')]
        good_ones = [lst for lst in vals if lst and 'Business Confidence' in lst]
        if good_ones:
            return [{'Last': good_ones[0][1]}]
        return []

    def process(self, element):
        try:
            result = self.get_manufacturing_pmi()
            return result
        except Exception as e:
            print('Failed to get PMI:{}'.format(str(e)))
            return []


class ParsePMI(beam.DoFn):

    def process_pmi(self, ratios_table):
        dt = [[item.text.strip() for item in row.find_all('th')] for row in ratios_table.find_all('thead')]
        vals = [[item.text.strip() for item in row.find_all('td')] for row in ratios_table.find_all('tr')]

        keys = chain(*dt)
        values = chain(*vals)
        return dict((k, v) for k, v in zip(keys, values))

    def get_latest_pmi(self):
        r = requests.get('https://tradingeconomics.com/united-states/non-manufacturing-pmi',
                            headers={'user-agent': 'my-app/0.0.1'})
        bs = BeautifulSoup(r.content, 'html.parser')
        div_item = bs.find_all('div', {"id": "ctl00_ContentPlaceHolder1_ctl00_ctl01_Panel1"})[0]  #
        tbl = div_item.find_all('table', {"class": "table"})[0]
        return self.process_pmi(tbl)

    def process(self, element):
        try:
            result = self.get_latest_pmi()
            return [result]
        except Exception as e:
            print('Failed to get PMI:{}'.format(str(e)))
            return [{'Last' : 'N/A'}]


def get_vix(key):
  base_url = 'https://financialmodelingprep.com/api/v3/quote-short/{}?apikey={}'.format('^VIX', key)
  print('Url is:{}'.format(base_url))
  return requests.get(base_url).json()[0]['price']




def get_prices2(tpl, fmprepkey):
    try:
        ticker = tpl
        stat_url = 'https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={token}'.format(symbol=ticker,
                                                                                                   token=fmprepkey)
        historical_data = requests.get(stat_url).json()[0]
        return (ticker, historical_data['price'], historical_data['change'],
                historical_data['yearHigh'], historical_data['yearLow'],
                0.0)
    except Exception as e :
        logging.info('Excepiton for {}:{}'.format(tpl, str(e)))
        return ()

def get_economic_calendar(fmprepkey):
    startDate = date.today()  - BDay(1)
    toEowDays = 7 - (startDate.weekday() + 1)
    eow = startDate + timedelta(days=toEowDays)
    economicCalendarUrl = f"https://financialmodelingprep.com/api/v3/economic_calendar?from={startDate.strftime('%Y-%m-%d')}&to={eow.strftime('%Y-%m-%d')}&apikey={fmprepkey}"
    data = requests.get(economicCalendarUrl).json()
    return [d for d in data if d['country'] == 'US' and d['impact'] in ['High','Medium']][::-1]




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



