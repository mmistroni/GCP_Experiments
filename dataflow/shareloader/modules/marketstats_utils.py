import apache_beam as beam
import logging
import pandas as pd
from bs4 import BeautifulSoup
import requests
from itertools import chain
from io import StringIO
from datetime import date, timedelta, datetime
from pandas.tseries.offsets import BDay
import statistics
from .news_util import get_user_agent
from .fred_utils import get_high_yields_spreads
from .finviz_utils import get_high_low
import math
from bs4 import BeautifulSoup

def create_bigquery_ppln(p, label):
    cutoff_date = (date.today() - BDay(5)).date().strftime('%Y-%m-%d')
    logging.info('Cutoff is:{}'.format(cutoff_date))
    edgar_sql = """SELECT AS_OF_DATE, LABEL, VALUE  FROM `datascience-projects.gcp_shareloader.market_stats` 
WHERE  PARSE_DATE("%F", AS_OF_DATE) > PARSE_DATE("%F", "{cutoff}")  
AND LABEL IN ('NASDAQ GLOBAL SELECT_MARKET BREADTH',
  'VIX', 'NEW YORK STOCK EXCHANGE_MARKET BREADTH',  'EQUITY_PUTCALL_RATIO' , 'MARKET_MOMENTUM', 'SECTOR ROTATION(GROWTH/VALUE)',
  'FED_FUND_RATES', 'NEW_HIGH_NEW_LOW', 'JUNK_BOND_DEMAND') 
ORDER BY LABEL ASC, PARSE_DATE("%F", AS_OF_DATE) ASC 
  """.format(cutoff=cutoff_date, label=label)
    logging.info('executing SQL :{}'.format(edgar_sql))
    return (p | 'Reading-{}'.format(label) >> beam.io.Read(
        beam.io.BigQuerySource(query=edgar_sql, use_standard_sql=True))

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


def create_bigquery_pipeline(p, label):
    logging.info(f'Running BQ for {label}')
    edgar_sql = f"""SELECT DISTINCT LABEL,  VALUE, 
                 DATE(CONCAT(EXTRACT (YEAR FROM DATE(AS_OF_DATE)), '-', 
                  EXTRACT (MONTH FROM DATE(AS_OF_DATE)), '-01')) AS AS_OF_DATE  FROM `datascience-projects.gcp_shareloader.market_stats` 
                WHERE LABEL = '{label}' 
                GROUP BY LABEL, VALUE, AS_OF_DATE
                ORDER BY LABEL, AS_OF_DATE DESC
                LIMIT 7
      """
    logging.info('executing SQL :{}'.format(edgar_sql))
    return (p | 'Reading-PMI historic-{label}' >> beam.io.Read(
        beam.io.BigQuerySource(query=edgar_sql, use_standard_sql=True))

            )


def get_latest_manufacturing_pmi_from_bq(p):
    bq_sql = """SELECT LABEL, AS_OF_DATE, VALUE FROM `datascience-projects.gcp_shareloader.market_stats` 
                WHERE LABEL = 'MANUFACTURING-PMI'
                ORDER BY PARSE_DATE('%F', AS_OF_DATE) DESC
                LIMIT 1"""

    logging.info('executing SQL :{}'.format(bq_sql))
    return (p | '1Reading latest manufacturing PMI ' >> beam.io.Read(
        beam.io.BigQuerySource(query=bq_sql, use_standard_sql=True))
            )


def get_latest_non_manufacturing_pmi_from_bq(p):
    bq_sql = """SELECT LABEL, AS_OF_DATE, VALUE FROM `datascience-projects.gcp_shareloader.market_stats` 
                WHERE LABEL = 'NON-MANUFACTURING-PMI'
                ORDER BY PARSE_DATE('%F', AS_OF_DATE) DESC
                LIMIT 1"""

    logging.info('executing SQL :{}'.format(bq_sql))
    return (p | '2Reading latest manufacturing PMI ' >> beam.io.Read(
        beam.io.BigQuerySource(query=bq_sql, use_standard_sql=True))
            )


def get_latest_consumer_sentiment_from_bq(p):
    bq_sql = """SELECT LABEL, AS_OF_DATE, VALUE FROM `datascience-projects.gcp_shareloader.market_stats` 
                WHERE LABEL = 'CONSUMER_SENTIMENT_INDEX'
                ORDER BY PARSE_DATE('%F', AS_OF_DATE) DESC
                LIMIT 1"""

    logging.info('executing SQL :{}'.format(bq_sql))
    return (p | '2Reading latest manufacturing PMI ' >> beam.io.Read(
        beam.io.BigQuerySource(query=bq_sql, use_standard_sql=True))
            )

def get_latest_price(fmpKey, ticker):
    # get yesterday price
    stat_url = f'https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={fmpKey}'
    return  requests.get(stat_url).json()



class PMIJoinerFn(beam.DoFn):
    def __init__(self):
        super(PMIJoinerFn, self).__init__()
        logging.info('----- Instantiated Joiners.....')

    def process(self, row, **kwargs):
        logging.info('---- Processing-----')
        right_dict = dict(kwargs['right_list'])

        logging.info(f'Row:{row}....')
        if len(row) > 1:
            left_key = row[0]
            left = row[1]
            logging.info(f'Left dict:{left}')
            logging.info(f'Right idct:{right_dict}')
            if left_key in right_dict:
                storedDateStr = right_dict[left_key].get('AS_OF_DATE')
                currentDateStr = left.get('AS_OF_DATE')
                storedDate = datetime.strptime(storedDateStr, '%Y-%m-%d')

                logging.info(f'Stored data: {storedDateStr}, Current Date: {currentDateStr}')

                currentDate = datetime.strptime(currentDateStr, '%Y-%m-%d')

                if currentDate > storedDate:
                    logging.info(f'We need to store  {left_key} in BQ..')
                    yield (left_key, left)
                else:
                    logging.info(f'{currentDateStr} is same as {storedDateStr}. No action')
            else:
                logging.info(f'No data in BQ need to store  {left_key} in BQ..')
                logging.info('REturning {left_key} {left}')
                yield  (left_key, left)



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
    all_stocks = get_all_us_stocks(iexapikey)
    logging.info('We got:{}'.format(len(all_stocks)))
    return all_stocks


def get_all_us_stocks(token, security_type='cs', nasdaq=True):
    logging.info('GEt All Us stocks..')
    all_dt = requests.get(
        'https://financialmodelingprep.com/api/v3/available-traded/list?apikey={}'.format(token)).json()
    us_stocks = [d['symbol'] for d in all_dt if d['exchange'] in ["New York Stock Exchange", "Nasdaq Global Select"]]
    logging.info('Got:{} Stocks'.format(len(us_stocks)))
    return us_stocks


def get_all_us_stocks2(token, exchange):
    logging.info(f'GEt All Us stocks  2 for :{exchange} using token {token}        ..')
    all_dt = requests.get(
        'https://financialmodelingprep.com/api/v3/available-traded/list?apikey={}'.format(token)).json()
    us_stocks = [d['symbol'] for d in all_dt if
                 d['exchange'] == exchange]  # ["New York Stock Exchange", "Nasdaq Global Select"]]
    logging.info('Got:{} Stocks'.format(len(us_stocks)))
    return us_stocks


def get_all_prices_for_date(apikey, asOfDate):
    import pandas as pd
    url = 'https://financialmodelingprep.com/api/v4/batch-request-end-of-day-prices?date={}&apikey={}'.format(asOfDate,
                                                                                                              apikey)
    s = requests.get(url).content
    bulkRequest = pd.read_csv(StringIO(s.decode('utf-8')), header=0)
    return bulkRequest.to_dict('records')


def get_latest_fed_fund_rates():
    try:

        import requests
        from bs4 import BeautifulSoup
        link = 'https://finance.yahoo.com/quote/ZQ%3DF/history?p=ZQ%3DF'
        r = requests.get(link, headers={'user-agent': 'my-app/0.0.1'})
        bs = BeautifulSoup(r.content, 'html.parser')
        t = bs.find_all('table', {"class": "W(100%) M(0)"})[0]

        rows = t.find_all('tr')
        row = rows[1]
        return row.find_all('td')[5].text
    except Exception as e:
        return 'N/A'

def parse_consumer_sentiment_index():

    ua = get_user_agent()
    try:
        r = requests.get('https://tradingeconomics.com/united-states/consumer-confidence', headers={'User-Agent': ua})
        bs = BeautifulSoup(r.content, 'html.parser')

        items = bs.find_all('tr')

        valids = [i for i in items if i.get('data-category') is not None and 'Consumer Confidence' in i.get('data-category')]

        lastVal = None
        lastDate = None
        for valid in valids:
            tds  = valid.find_all('td')
            testDate = tds[0].text
            testVal = tds[4].text.strip()
            if testVal:
                lastDate = testDate
                lastVal = testVal

        return {'Last': lastVal}
    except Exception as e:
        return {'Last': f'N/A-{str(e)}'}


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
            logging.info('Failed to get PMI:{}'.format(str(e)))
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
        pmiDict = dict((k, v) for k, v in zip(keys, values))
        pmiDict['Last'] = pmiDict.get('Last', -1)
        return pmiDict

    def get_latest_pmi(self):
        r = requests.get('https://tradingeconomics.com/united-states/non-manufacturing-pmi',
                         headers={'user-agent': 'my-app/0.0.1'})
        bs = BeautifulSoup(r.content, 'html.parser')
        div_item = bs.find_all('div', {"id": "ctl00_ContentPlaceHolder1_ctl00_ctl01_PanelComponents"})[0]  #
        tbl = div_item.find_all('table')[0]
        return self.process_pmi(tbl)

    def process(self, element):
        try:
            result = self.get_latest_pmi()
            logging.info(f'Result is:{result}')
            return [result]
        except Exception as e:
            logging.info('Failed to get PMI:{}'.format(str(e)))
            return [{'Last': 'N/A'}]


class ParseConsumerSentimentIndex(beam.DoFn):
    '''
    Parses non manufacturing PMI
    '''

    def process(self, element):
        try:
            result = parse_consumer_sentiment_index()
            return [result]
        except Exception as e:
            print('Failed to get PMI:{}'.format(str(e)))
            return []


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
market_momentum_dict = {'^GSPC' : 'S&PClose', 'QQQ' : 'Nasdaq100Close',
                        'IWM': 'Russell2000Close', '^RUT' : 'Rusell2000Close'
                        }

def get_market_momentum(key, ticker='^GSPC'):
    hist_url = f'https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?apikey={key}'
    lastDays = requests.get(hist_url).json().get('historical')[0:125]
    close = [d['close'] for d in lastDays]
    day125avg = statistics.mean(close)
    latest = close[0]

    status = 'FEAR' if latest < day125avg else 'GREED'

    prefix = market_momentum_dict[ticker]
    return [f'{prefix}:(125MVGAVG:{day125avg})|{latest}|STATUS:{status}']


def get_cftc_spfutures(key):
    ''' wE NEED TO ADD THE following query to the marketstats
    SELECT *  FROM `datascience-projects.gcp_shareloader.market_stats`
        WHERE LABEL LIKE 'CFTC%'
        ORDER BY AS_OF_DATE DESC
        LIMIT 5
    '''
    # Investigate this URL https://www.cftc.gov/files/dea/history/dea_fut_xls_2022.zip
    base_url = f'https://financialmodelingprep.com/api/v4/commitment_of_traders_report_analysis/VX?apikey={key}'
    all_data = requests.get(base_url).json()
    if len(all_data) > 0:
        data = all_data[0]
        return f"ChangeInNetPosition:{data['changeInNetPosition']}, Sentiment:{data['marketSentiment']}"
    else:
        return "ChangeInNetPosition:N/A, Sentiment:N/A"


def get_sp500():
    tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    sp500_df = tables[0]
    sp500_df["Symbol"] = sp500_df["Symbol"].map(lambda x: x.replace(".", "-"))

    sp500_df = sp500_df[['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']]
    return sp500_df.to_dict('records')

def get_mcclellan(ticker):
    from datetime import date
    today = date.today()
    try:
          YEARS = 25
          URL = f'https://stockcharts.com/c-sc/sc?s={ticker}&p=D&yr={YEARS}&mn=0&dy=0&i=t3757734781c&img=text&inspector=yes'
          data = requests.get(URL, headers={'user-agent': get_user_agent()}).text

          tmpdata = data.split('<pricedata>')
          if tmpdata:
            data = tmpdata[1].replace('</pricedata>', '')
          else:
              raise Exception('Data from request does not have enough info')
          lines = data.split('|')
          data = []
          for line in lines:
              cols = line.split(' ')
              date = datetime(int(cols[1][0:4]), int(cols[1][4:6]), int(cols[1][6:8]))
              value = float(cols[3])

              if not math.isnan(value):
                  data.append({'date': date, 'value': value})

          df = pd.DataFrame.from_dict(data)

          return {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'),
                  'LABEL' : ticker,
                  'VALUE' : df.tail(1).to_dict('records')[0]['value']
                  }
    except Exception as e:
        logging.info(f'Failed to get data for {ticker}:{str(e)}')
        return {'AS_OF_DATE': today.strftime('%Y-%m-%d'),
         'LABEL': ticker,
         'VALUE': 0.0
         }


def get_vix(key):
    try:
        base_url = 'https://financialmodelingprep.com/api/v3/quote-short/{}?apikey={}'.format('^VIX', key)
        logging.info('Url is:{}'.format(base_url))
        return requests.get(base_url).json()[0]['price']
    except Exception as e:
        logging.info(f'Exception in getting vix:{str(e)}')
        return 0.0

def get_junkbonddemand(fred_key):
    try:

        fred_data = get_high_yields_spreads(fred_key)

        last = fred_data['observations'][-1]['value']
        penultimate = fred_data['observations'][-2]['value']

        return float(last) - float(penultimate)
    except Exception as e:
        logging.info(f'Exception in getting junk bond demand:{str(e)}')
        return 0.0


def get_senate_disclosures(key):
    url = f'https://financialmodelingprep.com/api/v4/senate-disclosure-rss-feed?page=0&apikey={key}'

    data = requests.get(url).json()
    yesterday = (date.today() - BDay(5)).date()
    holder = []

    for dataDict in data:
        asOfDate = datetime.strptime(dataDict['disclosureDate'], '%Y-%m-%d').date()
        logging.info(f'Processing senate disclosures....{dataDict}.')
        if asOfDate < yesterday:
            break
        else:
            value = f"Ticker:{dataDict.get('ticker', 'NA')}|Type:{dataDict['type']}"
            label = 'SENATE_DISCLOSURES'
            holder.append({'AS_OF_DATE': asOfDate.strftime('%Y-%m-%d'), 'LABEL': label, 'VALUE': value})
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
    except Exception as e:
        logging.info('Excepiton for {}:{}'.format(tpl, str(e)))
        return ()


def get_economic_calendar(fmprepkey):
    startDate = date.today() - BDay(1)
    toEowDays = 7 - (startDate.weekday() + 1)
    eow = startDate + timedelta(days=toEowDays)
    economicCalendarUrl = f"https://financialmodelingprep.com/api/v3/economic_calendar?from={startDate.strftime('%Y-%m-%d')}&to={eow.strftime('%Y-%m-%d')}&apikey={fmprepkey}"
    data = requests.get(economicCalendarUrl).json()
    return [d for d in data if d['country'] == 'US' and d['impact'] in ['High', 'Medium']][::-1]


def get_equity_putcall_ratio():
    # replace with
    from .news_util import get_user_agent
    r = requests.get('https://ycharts.com/indicators/cboe_equity_put_call_ratio',
                     headers={'User-Agent': get_user_agent()})
    bs = BeautifulSoup(r.content, 'html.parser')
    div_item = bs.find('div', {"class": "key-stat-title"})
    import re
    try:
        data = div_item.text
        values = re.findall(r'\d+\.\d+', data)
        if not values:
            return 1
        return float(values[0])
    except Exception as e:
        logging.info(f'Exception:{str(e)}')
        return 1


def get_skew_index():
    # https: // edition.cnn.com / markets / fear - and -greed
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
    ratios = [growth / value for growth, value in zip(growthClose, valueClose)]

    dayavg = statistics.mean(ratios)
    latest = ratios[0]

    return f'|125MVGAVG:{dayavg}|LATEST:{latest}'


def get_prices(ticker, iexapikey):
    try:
        iexurl = 'https://cloud.iexapis.com/stable/stock/{ticker}/quote?token={token}'.format(
            ticker=ticker, token=iexapikey)
        all_data = requests.get(iexurl).json()

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
        higher = 1 if input[2] and input[2] > 0 else 0
        lower = 1 if input[2] and input[2] < 0 else 0
        (hi_stock, lo_stock) = accumulator
        return hi_stock + higher, lo_stock + lower

    def merge_accumulators(self, accumulators):
        hi, lo = zip(*accumulators)
        return sum(hi), sum(lo)

    def extract_output(self, sum_count):
        (hi, lo) = sum_count
        return 'MARKET BREADTH:Higher:{}, Lower:{}, Breadth:{}'.format(hi, lo, hi / lo if lo != 0 else 1)


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
        return (hi, lo)


def parse_date(date_string):
  """Parses a date string in the format "Aug. 1, 2024" or "Aug. 10, 2024".

  Args:
    date_string: The date string to parse.

  Returns:
    A datetime object.
  """
  try:
      month_dict = {'Jan' : 1, 'Feb' : 2, 'Mar' : 3, 'Apr' : 4, 'May' : 5,
                    'Jun' : 6, 'Jul' : 7, 'Aug' : 8, 'Sep' : 9, 'Oct' : 10,
                    'Nov' : 11, 'Dec' : 12
                    }

      month_and_day = date_string.split(',')[0]
      year = int(date_string.split(',')[1].strip())
      month, day = month_and_day.split()
      day = int(day)
      month_int = month_dict.get(month[0:3], date.today().month)

      return date(year, month_int, day)
  except Exception as  e:
      logging.info(f'exception in parsing date:{str(e)}')
      return (date.today() - BDay(5)).date()



def get_cramer_picks(fmpkey, numdays):

    baseUrl = 'https://www.quiverquant.com/cramertracker/'

    req = requests.get(baseUrl, headers={'User-Agent': get_user_agent()})
    soup = BeautifulSoup(req.text, "html.parser")

    table = soup.find_all('div', {"class": "holdings-table table-inner"})[0]

    holder = []


    for row in table.find_all('tr'):
        tds = row.find_all('td')
        if not tds:
            continue
        else:
            ticker  = tds[0].text

            quote = get_latest_price(fmpkey, ticker)

            try:
                if quote and len(quote) > 0:
                    price = quote[0]['price']
                else:
                    price = 0
            except Exception as e:
                logging.info(f'Coul,d no retrieve quote for {ticker}:{quote}')


            direction = tds[1].text

            cob = parse_date(tds[2].text)
            # need to fetch current price
            if (date.today() - cob).days > numdays:
                continue
            else:
                holder.append(dict(DATE=cob, TICKER=ticker, RECOMMENDATION=direction, PRICE=price))

    return holder

class NewHighNewLowLoader(beam.DoFn):
    def __init__(self, key):
        self.key = key

    def get_quote(self, ticker):
        # get yesterday price
        stat_url = f'https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={self.key}'
        return requests.get(stat_url).json()[0]

    def process(self, elements):

        high_low_dict = get_high_low()

        logging.info(f'------\n{high_low_dict}' )

        return [high_low_dict]
