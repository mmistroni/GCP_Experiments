import requests
import apache_beam as beam
import logging
from itertools import chain
from bs4 import BeautifulSoup
import requests
from itertools import chain
from io import StringIO
from datetime import date, timedelta, datetime
from pandas.tseries.offsets import BDay
import statistics
from .news_util import get_user_agent

def create_bigquery_ppln(p, label):
    cutoff_date = (date.today() - BDay(5)).date().strftime('%Y-%m-%d')
    logging.info('Cutoff is:{}'.format(cutoff_date))
    edgar_sql = """SELECT AS_OF_DATE, LABEL, VALUE  FROM `datascience-projects.gcp_shareloader.market_stats` 
WHERE  PARSE_DATE("%F", AS_OF_DATE) > PARSE_DATE("%F", "{cutoff}")  
AND LABEL IN ('NASDAQ GLOBAL SELECT_MARKET BREADTH',
  'VIX', 'NEW YORK STOCK EXCHANGE_MARKET BREADTH',  'EQUITY_PUTCALL_RATIO' , 'MARKET_MOMENTUM', 'SECTOR ROTATION(GROWTH/VALUE)') 
ORDER BY LABEL ASC, PARSE_DATE("%F", AS_OF_DATE) ASC 
  """.format(cutoff=cutoff_date, label=label)
    logging.info('executing SQL :{}'.format(edgar_sql))
    return (p | 'Reading-{}'.format(label) >> beam.io.Read(beam.io.BigQuerySource(query=edgar_sql, use_standard_sql=True))
              
           )


def create_bigquery_ppln_cftc(p):
    logging.info('Querying CFTC HISTORIC')
    edgar_sql = """SELECT * FROM (SELECT AS_OF_DATE, LABEL, VALUE FROM `datascience-projects.gcp_shareloader.market_stats` 
WHERE LABEL LIKE '%CFTC%' ORDER BY PARSE_DATE("%F", AS_OF_DATE) DESC
LIMIT 5 ) ORDER BY AS_OF_DATE ASC
  """
    logging.info('executing SQL :{}'.format(edgar_sql))
    return (p | 'Reading-CFTC historic' >> beam.io.Read(
        beam.io.BigQuerySource(query=edgar_sql, use_standard_sql=True))

            )

def create_bigquery_manufpmi_bq(p):
    edgar_sql = """SELECT DISTINCT LABEL,  VALUE, 
                DATE(CONCAT(EXTRACT (YEAR FROM DATE(AS_OF_DATE)), '-', 
                  EXTRACT (MONTH FROM DATE(AS_OF_DATE)), '-01')) AS AS_OF_DATE  FROM `datascience-projects.gcp_shareloader.market_stats` 
                WHERE LABEL = 'MANUFACTURING-PMI' 
                GROUP BY LABEL, VALUE, AS_OF_DATE
                ORDER BY LABEL, AS_OF_DATE DESC
                LIMIT 5
      """
    logging.info('executing SQL :{}'.format(edgar_sql))
    return (p | 'Reading-PMI historic' >> beam.io.Read(
        beam.io.BigQuerySource(query=edgar_sql, use_standard_sql=True))

            )

def create_bigquery_nonmanuf_pmi_bq(p):
    edgar_sql = """SELECT DISTINCT LABEL,  VALUE, 
                 DATE(CONCAT(EXTRACT (YEAR FROM DATE(AS_OF_DATE)), '-', 
                  EXTRACT (MONTH FROM DATE(AS_OF_DATE)), '-01')) AS AS_OF_DATE  FROM `datascience-projects.gcp_shareloader.market_stats` 
                WHERE LABEL = 'NON-MANUFACTURING-PMI' 
                GROUP BY LABEL, VALUE, AS_OF_DATE
                ORDER BY LABEL, AS_OF_DATE DESC
                LIMIT 7
      """
    logging.info('executing SQL :{}'.format(edgar_sql))
    return (p | 'Reading-PMI historic-NONMANUF' >> beam.io.Read(
        beam.io.BigQuerySource(query=edgar_sql, use_standard_sql=True))

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



class ParseNonManufacturingPMI(beam.DoFn):
    '''
    Parses non manufacturing PMI
    '''

    def process_pmi(self, ratios_table):
        dt = [[item.text.strip() for item in row.find_all('th')] for row in ratios_table.find_all('thead')]
        vals = [[item.text.strip() for item in row.find_all('td')] for row in ratios_table.find_all('tr')]

        keys = chain(*dt)
        values = chain(*vals)
        pmiDict =  dict((k, v) for k, v in zip(keys, values))
        pmiDict['Last'] = pmiDict.get('Actual', -1)
        return pmiDict

    def get_latest_pmi(self):
        r = requests.get('https://tradingeconomics.com/united-states/non-manufacturing-pmi',
                            headers={'user-agent': 'my-app/0.0.1'})
        bs = BeautifulSoup(r.content, 'html.parser')
        div_item = bs.find_all('div', {"id": "ctl00_ContentPlaceHolder1_ctl00_ctl02_Panel1"})[0]  #
        tbl = div_item.find_all('table', {"class": "table"})[0]
        return self.process_pmi(tbl)

    def process(self, element):
        try:
            result = self.get_latest_pmi()
            return [result]
        except Exception as e:
            print('Failed to get PMI:{}'.format(str(e)))
            return [{'Last' : 'N/A'}]

'''
== FEAR AND GREED


MARKET MOMENTUM  X( SP500 VS AVG)

STOCK PRICE STRENGTH (52WK HIGH VS 52 WK LOW, we can get it)  X

STOCK PRICE BREADTH (NYSE)  X

PUT CALL RATIO X

MARKET VOLATILITY (VIX)

SAFE HEAVEN DEMAND   www.thebalance.com/stocks-vs-bonds-the-long-term-performance-data-416861

JUNK BOND DEMAND = YIeld spread: junk bonds vs investment grade
check investopedia.comm  high yield bond spread


'''

def get_market_momentum(key):
    hist_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/^GSPC?apikey={}'.format(key)
    lastDays =  requests.get(hist_url).json().get('historical')[0:125]
    close = [d['close'] for d in lastDays]
    day125avg = statistics.mean(close)
    latest = close[0]

    status = 'FEAR' if latest < day125avg else 'GREED'

    return [f'S&PClose:(125MVGAVG:{day125avg})|{latest}|STATUS:{status}']

def get_cftc_spfutures(key):
    ''' wE NEED TO ADD THE following query to the marketstats
    SELECT *  FROM `datascience-projects.gcp_shareloader.market_stats`
        WHERE LABEL LIKE 'CFTC%'
        ORDER BY AS_OF_DATE DESC
        LIMIT 5
    '''
    # Investigate this URL https://www.cftc.gov/files/dea/history/dea_fut_xls_2022.zip
    base_url = f'https://financialmodelingprep.com/api/v4/commitment_of_traders_report_analysis/VI?apikey={key}'
    data = requests.get(base_url).json()[0]
    return f"ChangeInNetPosition:{data['changeInNetPosition']}, Sentiment:{data['marketSentiment']}"

def get_vix(key):
    try:
        base_url = 'https://financialmodelingprep.com/api/v3/quote-short/{}?apikey={}'.format('^VIX', key)
        logging.info('Url is:{}'.format(base_url))
        return requests.get(base_url).json()[0]['price']
    except Exception as e:
        logging.info(f'Exception in getting vix:{str(e)}')
        return 0.0


def get_senate_disclosures(key):
    url = f'https://financialmodelingprep.com/api/v4/senate-disclosure-rss-feed?page=0&apikey={key}'

    data = requests.get(url).json()
    yesterday = (date.today() - BDay(5)).date()
    holder = []

    for dataDict in data:
        asOfDate = datetime.strptime(dataDict['disclosureDate'], '%Y-%m-%d' ).date()
        logging.info(f'Processing senate disclosures....{dataDict}.')
        if asOfDate < yesterday:
            break
        else:
            value  = f"Ticker:{dataDict['ticker']}|Type:{dataDict['type']}"
            label = 'SENATE_DISCLOSURES'
            holder.append({'AS_OF_DATE' : asOfDate.strftime('%Y-%m-%d'), 'LABEL' : label, 'VALUE' : value})
    return holder









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


def get_equity_putcall_ratio():
    from .news_util import get_user_agent
    r = requests.get('https://ycharts.com/indicators/cboe_equity_put_call_ratio', headers={'User-Agent': get_user_agent()})
    bs = BeautifulSoup(r.content, 'html.parser')
    div_item = bs.find('div', {"class": "key-stat-title"})
    import re
    data = div_item.text
    values = re.findall(r'\d+\.\d+', data)
    if not values:
        return 1
    return float(values[0])


def get_skew_index():
    #https: // edition.cnn.com / markets / fear - and -greed
    from datetime import date, datetime
    from pandas.tseries.offsets import BDay
    import pandas as pd
    try:
        prevBDay = date.today() - BDay(1)
        prevTs = int(prevBDay.timestamp())
        currentTs = int(datetime.now().timestamp())
        skewUrl = f'https://query1.finance.yahoo.com/v7/finance/download/%5ESKEW?period1={prevTs}&period2={currentTs}&interval=1d&events=history&includeAdjustedClose=true'
        print(skewUrl)
        df = pd.read_csv(skewUrl)
        return df['Close'].values[0]
    except Exception as e:
        logging.info(f'Excepiton in getting skew{str(e)}')

def get_sector_rotation_indicator(key):
    growth = 'IVW'
    value = 'IVE'
    hist_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/{}?apikey={}'
    lastDaysGrowth = requests.get(hist_url.format(growth, key)).json().get('historical')[0:125]
    growthClose = [d['close'] for d in lastDaysGrowth]
    lastDaysValue = requests.get(hist_url.format(value, key)).json().get('historical')[0:125]
    valueClose = [d['close'] for d in lastDaysValue]
    ratios = [growth/value for growth, value in zip(growthClose, valueClose)]

    dayavg = statistics.mean(ratios)
    latest = ratios[0]

    return f'|125MVGAVG:{dayavg}|LATEST:{latest}'


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



