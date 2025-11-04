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
from .finviz_utils import get_high_low, get_advance_decline, get_advance_decline_sma
import math
from bs4 import BeautifulSoup
from collections import OrderedDict
from openbb_fmp.models.economic_calendar import FMPEconomicCalendarFetcher
from openbb_fmp.models.index_historical import FMPIndexHistoricalFetcher
from openbb_fmp.models.equity_historical import FMPEquityHistoricalFetcher
from openbb_finviz.models.equity_screener import FinvizEquityScreenerFetcher
from openbb_yfinance.models.index_historical import YFinanceIndexHistoricalFetcher
from openbb_benzinga.models.world_news import BenzingaWorldNewsFetcher

import asyncio

def create_bigquery_ppln(p, label):
    cutoff_date = (date.today() - BDay(5)).date().strftime('%Y-%m-%d')
    logging.info('Cutoff is:{}'.format(cutoff_date))
    edgar_sql = """SELECT AS_OF_DATE, LABEL, VALUE  FROM `datascience-projects.gcp_shareloader.market_stats` 
WHERE  PARSE_DATE("%F", AS_OF_DATE) > PARSE_DATE("%F", "{cutoff}")  
AND LABEL IN ('NASDAQ_ADVANCE_DECLINE',
  'VIX', 'NYSE_ADVANCE_DECLINE',  'EQUITY_PUTCALL_RATIO' , 'MARKET_MOMENTUM', 'SECTOR ROTATION(GROWTH/VALUE)',
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
market_momentum_dict = {'^GSPC' : 'S&PClose', '^IXIC' : 'Nasdaq100Close',
                        'IWM': 'Russell2000Close', '^RUT' : 'Rusell2000Close',
                        '^NYA' : 'NYSE Composite'
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


def generate_cotc_data(cotc_dict):
    fields = ['symbol', 'as_of_date_in_form_yymmdd' ,
              'noncomm_positions_long_all', 'noncomm_positions_short_all',
              'comm_positions_long_all', 'comm_positions_short_all']
    updated = dict((k,v) for k,v in cotc_dict.items() if k in fields)

    net_noncomm = updated['noncomm_positions_long_all'] - updated['noncomm_positions_short_all']
    net_comm = updated['comm_positions_long_all'] - updated['comm_positions_short_all']
    updated['net_non_commercial'] = net_noncomm
    updated['net_commercial'] = net_comm
    updated['date'] = datetime.strptime(updated['as_of_date_in_form_yymmdd'], '%y%m%d')

    return updated

def get_cot_futures(key, symbol):
    ''' wE NEED TO ADD THE following query to the marketstats
    SELECT *  FROM `datascience-projects.gcp_shareloader.market_stats`
        WHERE LABEL LIKE 'CFTC%'
        ORDER BY AS_OF_DATE DESC
        LIMIT 5
    '''
    # Investigate this URL https://www.cftc.gov/files/dea/history/dea_fut_xls_2022.zip
    base_url = f'https://financialmodelingprep.com/api/v4/commitment_of_traders_report/?apikey={key}'
    all_data = requests.get(base_url).json()
    from pprint import pprint

    vx = [d for d in all_data if d['symbol'] == symbol]



    if len(vx) > 0:
        try:
            return [generate_cotc_data(d) for d in vx]
        except Exception as e:
            logging.info(f'Excepetion nc otc:{str(e)}')
        data = all_data[0]
        return f"ChangeInNetPosition:{data['changeInNetPosition']}, Sentiment:{data['marketSentiment']}"
    else:
        return []




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

def get_obb_vix(key):
    try:
        credentials = {'fmp_key' : key}

        start = (date.today() - BDay(1)).date()
        end = date.today()
        params = {
            "symbol": "^VIX",
            "start_date": start,
            "end_date": end,
        }

        data = FMPIndexHistoricalFetcher.fetch_data(params, credentials)
        result =  [d.model_dump(exclude_none=True) for d in data]
        # process and get latest price
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

def get_shiller_indexes():
    shillers = []

    shiller_dict = OrderedDict()
    shiller_dict['SHILLER_1Y_CONFIDENCE'] = 'https://shiller-data-public.s3.amazonaws.com/icf_stock_market_confidence_index_table.csv'
    shiller_dict['CRASH_CONFIDENCE_INDEX'] = 'https://shiller-data-public.s3.amazonaws.com/icf_stock_market_crash_index_table.csv'
    shiller_dict[
        'BUY_ON_DIP_CONFIDENCE'] = 'https://shiller-data-public.s3.amazonaws.com/icf_stock_market_dips_index_table.csv'
    shiller_dict[
        'VALUATION_CONFIDENCE_INDEX'] = 'https://shiller-data-public.s3.amazonaws.com/icf_stock_market_valuation_index_table.csv'


    shillers = []

    for label, url in shiller_dict.items():
        data  = pd.read_csv(url, header=1).to_dict('records')[0:2]

        latest_data = data[0]
        prev_data = data[1]

        cob = latest_data['Unnamed: 0']
        value = latest_data['Index Value']
        prev = prev_data['Index Value']

        shillers.append({'AS_OF_DATE' : cob,'LABEL' : label, 'VALUE' : f'{value}-Prev({prev})'})


    return shillers

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

class AdvanceDecline(beam.DoFn):
    def process(self, elements):

        adv_decline =  get_advance_decline(elements)

        logging.info(f'------\n{adv_decline}' )

        return [adv_decline]

class AdvanceDeclineSma(beam.DoFn):
    def __init__(self, exchange, numDays):
        self.exchange = exchange
        self.numDays = numDays
        self.fetcher = FinvizEquityScreenerFetcher


    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')
        try:
            adv_decline = get_advance_decline_sma(self.exchange, self.numDays)

            logging.info(f'------\n{adv_decline}')

            return [adv_decline]

        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return  [{'VALUE': f'{str(e)}'}]

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))

class AsyncFetcher(beam.DoFn):
    def __init__(self, fmp_key):#
        self.fmp_key = fmp_key
        self.fetcher = FinvizEquityScreenerFetcher


    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')
        try:
            credentials = {'fmp_api_key' : self.fmp_key}

            start = (date.today() - BDay(1)).date()
            end = date.today()
            params = {
                "symbol": element,
                "start_date": start,
                "end_date": end,
            }

            data = await FMPIndexHistoricalFetcher.fetch_data(params, credentials)
            result =  [d.model_dump(exclude_none=True) for d in data]
            return result   

        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return  [{'VALUE': f'{str(e)}'}]

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))
        

class BenzingaNews(beam.DoFn):

    def __init__(self, key):
        self.key = key
    
    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')
        try:
            credentials = {'benzinga_api_key' : self.key}

            start = (date.today() - BDay(1)).date()
            end = date.today()
            params = {
                "start_date": start,
                "end_date": end
                
            }

            data = await BenzingaWorldNewsFetcher.fetch_data(params, {})
            result =  [d.model_dump(exclude_none=True) for d in data]
            
            return result

        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return  [{'VALUE': f'{str(e)}'}]

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))

class OBBMarketMomemtun(beam.DoFn):
    def __init__(self, key):
        self.fmp_api_key = key

    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')
        try:
            credentials = {'fmp_api_key' : self.fmp_api_key}

            start = (date.today() - BDay(200)).date()
            end = date.today()
            params = {
                "symbol": element,
                "start_date": start,
                "end_date": end,
            }

            data = await FMPIndexHistoricalFetcher.fetch_data(params, credentials)
            result =  [d.model_dump(exclude_none=True) for d in data]
            
            last_days = result[-125:]

            close = [d['close'] for d in last_days]
            day125avg = statistics.mean(close)
            latest = close[-1]

            status = 'FEAR' if latest < day125avg else 'GREED'

            prefix = market_momentum_dict[element]
            return [f'{prefix}:(125MVGAVG:{day125avg})|{latest}|STATUS:{status}']

        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return  [{'VALUE': f'{str(e)}'}]

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))

class AsyncSectorRotation(beam.DoFn):
    def __init__(self, fmp_key):#
        self.fmp_key = fmp_key
        self.fetcher = FinvizEquityScreenerFetcher

    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')
        try:
            credentials = {'fmp_api_key' : self.fmp_key}
            growth = 'IVW'
            value = 'IVE'
            params = {
                "symbol": growth,
                
            }
            growth_data = await FMPEquityHistoricalFetcher.fetch_data(params, credentials)
            growth_result =  [d.model_dump(exclude_none=True) for d in growth_data][-125:]
            params = {
                "symbol": value,
                
            }                                                          
            value_data = await FMPEquityHistoricalFetcher.fetch_data(params, credentials)
            value_result =  [d.model_dump(exclude_none=True) for d in value_data][-125:]
            
            growthClose = [d['close'] for d in growth_result]
            valueClose = [d['close'] for d in value_result]
            ratios = [growth / value for growth, value in zip(growthClose, valueClose)]

            dayavg = statistics.mean(ratios)
            latest = ratios[-1]

            return [f'|125MVGAVG:{dayavg}|LATEST:{latest}']

        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return  [{'VALUE': f'{str(e)}'}]

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))

class AsyncEconomicCalendar(beam.DoFn):
    def __init__(self, fmp_key):#
        self.fmp_key = fmp_key
        self.fetcher = FMPEconomicCalendarFetcher

    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')
        try:
            credentials = {'fmp_api_key' : self.fmp_key}
            startDate = date.today() - BDay(1)
            toEowDays = 7 - (startDate.weekday() + 1)
            eow = startDate + timedelta(days=toEowDays)
            params = {
                "start_date": startDate,
                "end_date" : eow
                
            }
            value_data = await self.fetcher.fetch_data(params, credentials)
            value_result =  [d.model_dump(exclude_none=True) for d in value_data ]
            relevants = [d for d in value_result if d.get('country', '') == 'US' and d.get('importance', '') in ['High', 'Medium']][::-1]
            return relevants

        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return  [{'VALUE': f'{str(e)}'}]

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))

import matplotlib.pyplot as plt

import pandas as pd
import numpy as np  # Need numpy for the helper function


class SentimentCalculator:  # Assuming your original function is part of a class/script

    def _assign_cot_signal(self, row: pd.Series) -> str:
        """
        Assigns a VIX COT Signal based on VIX price and Percentile Rank thresholds.
        These thresholds are typically used for contrarian signals.
        """
        vix = row['VIX_Close']
        rank = row['Percentile_Rank']

        # --- Thresholds based on general market/VIX analysis ---
        # VIX Price:
        LOW_VIX = 15.0
        HIGH_VIX = 25.0

        # Percentile Rank (for VIX COT Net Position):
        # A low rank means speculators are historically net-short (complacent).
        EXTREME_COMPLACENCY_RANK_MAX = 0.15  # Top 15% historically short
        ELEVATED_COMPLACENCY_RANK_MAX = 0.30  # Top 30% historically short

        # A high rank means speculators are historically net-long (fearful).
        EXTREME_FEAR_RANK_MIN = 0.85  # Bottom 15% historically short (i.e., long)
        ELEVATED_FEAR_RANK_MIN = 0.70  # Bottom 30% historically short

        # 1. Extreme Complacency (Strong Bearish Signal for Stocks / Buy VIX)
        if (vix < LOW_VIX) and (rank < EXTREME_COMPLACENCY_RANK_MAX):
            return 'Extreme Complacency (Contrarian Buy Volatility)'

        # 2. Extreme Fear (Strong Bullish Signal for Stocks / Sell VIX)
        elif (vix > HIGH_VIX) and (rank > EXTREME_FEAR_RANK_MIN):
            return 'Extreme Fear (Contrarian Sell Volatility)'

        # 3. Elevated Complacency
        elif (vix < LOW_VIX) and (rank < ELEVATED_COMPLACENCY_RANK_MAX):
            return 'Elevated Complacency'

        # 4. Elevated Fear
        elif (vix > 20.0) and (rank > ELEVATED_FEAR_RANK_MIN):  # Using a mid-range VIX threshold for Elevated Fear
            return 'Elevated Fear'

        # 5. Volatility Spike (High VIX but not Extreme Speculator Positioning)
        elif vix > HIGH_VIX:
            return 'VIX Spike (Price-Driven Fear)'

        # 6. Low Volatility (Low VIX but not Extreme Speculator Positioning)
        elif vix < LOW_VIX:
            return 'Low Volatility (Price-Driven Complacency)'

        # 7. Neutral/Mean-Reversion
        return 'Neutral Sentiment'

    def calculate_sentiment(self, data: list[dict], vix_prices_df: pd.DataFrame) -> pd.DataFrame | None:
        """
        Processes COT data and VIX price data, merges them, and calculates the
        COT Percentile Rank and VIX COT Signal.

        Args:
            data: A list of dictionaries (COT data).
            vix_prices_df: A pandas DataFrame containing VIX daily price data.

        Returns:
            A merged and calculated pandas DataFrame, or None on error.
        """
        COT_DATE_COLUMN = 'as_of_date_in_form_yymmdd'
        INDEX_VALUE_COLUMN = 'net_non_commercial'
        VIX_DATE_COLUMN = 'date'
        VIX_CLOSE_COLUMN = 'close'

        # --- 1. Load and Prepare COT Data ---
        try:
            df = pd.DataFrame(data=data)
            df['Date'] = pd.to_datetime(df[COT_DATE_COLUMN], format='%y%m%d')
            analysis_df = df.set_index('Date').sort_index()
            analysis_df = analysis_df[[INDEX_VALUE_COLUMN]].rename(columns={INDEX_VALUE_COLUMN: 'Net_Position'})
            print(f"COT DataFrame loaded with {len(analysis_df)} records.")

            # --- 2. Preparing and Merging VIX Price Records (Weekly Resampling) ---
            print(f"\n--- 2. Preparing and Merging VIX Price Records ---")

            if VIX_CLOSE_COLUMN not in vix_prices_df.columns or VIX_DATE_COLUMN not in vix_prices_df.columns:
                print(f"Error: VIX price DataFrame must contain '{VIX_DATE_COLUMN}' and '{VIX_CLOSE_COLUMN}' columns.")
                return None

            vix_prices_df[VIX_DATE_COLUMN] = pd.to_datetime(vix_prices_df[VIX_DATE_COLUMN])
            vix_prices_df = vix_prices_df.set_index(VIX_DATE_COLUMN).sort_index()

            vix_weekly_close = vix_prices_df[VIX_CLOSE_COLUMN].resample('W-TUE').last()
            vix_weekly_close.rename('VIX_Close', inplace=True)

            analysis_df = analysis_df.merge(vix_weekly_close, left_index=True, right_index=True, how='inner')

            if len(analysis_df) == 0:
                print("Error: No common dates found between COT and VIX data after resampling.")
                return None

            print(f"Data successfully merged. Final records for analysis: {len(analysis_df)}")

            # --- 3. Calculating Key Analytical Metrics ---
            analysis_df['Percentile_Rank'] = analysis_df['Net_Position'].rank(pct=True)
            analysis_df['WoW_Change'] = analysis_df['Net_Position'].diff()
            analysis_df['VIX_WoW_Change'] = analysis_df['VIX_Close'].diff()
            analysis_df['VIX_WoW_Pct_Change'] = analysis_df['VIX_Close'].pct_change()

            # --- 4. NEW: Calculate VIX COT Signal ---
            print(f"\n--- 4. Calculating VIX COT Signal ---")
            analysis_df['VIX_COT_Signal'] = analysis_df.apply(self._assign_cot_signal, axis=1)

            print("\nAnalysis DataFrame prepared successfully.")
            return analysis_df

        except Exception as e:
            print(f"\nAn error occurred during data processing: {e}")
            return None




    @staticmethod
    def plot_cot_vix_relationship(analysis_df: pd.DataFrame, file_name: str = 'cot_vix_plot.png'):
        """
        Generates a dual-axis plot showing VIX Close Price and COT Percentile Rank,
        marking extreme Percentile Rank levels (5%, 50%, 95%) for guidance.
        """
        if analysis_df.empty:
            print("Cannot plot: Input DataFrame is empty.")
            return

        fig, ax1 = plt.subplots(figsize=(14, 7))

        # --- Primary Axis: VIX Closing Price (Blue Line) ---
        color_vix = 'tab:blue'
        ax1.set_xlabel('Date')
        ax1.set_ylabel('VIX Close Price', color=color_vix, fontweight='bold')
        ax1.plot(analysis_df.index, analysis_df['VIX_Close'], color=color_vix, label='VIX Close Price', linewidth=2)
        ax1.tick_params(axis='y', labelcolor=color_vix)
        ax1.grid(True, linestyle='--', alpha=0.6)

        # --- Secondary Axis: COT Percentile Rank (Red Dashed Line) ---
        ax2 = ax1.twinx()
        color_cot = 'tab:red'
        ax2.set_ylabel('COT Percentile Rank (Sentiment)', color=color_cot, fontweight='bold')
        ax2.plot(analysis_df.index, analysis_df['Percentile_Rank'], color=color_cot, linestyle='--',
                 label='COT Percentile Rank', linewidth=1.5, alpha=0.7)
        ax2.tick_params(axis='y', labelcolor=color_cot)

        # Set COT Percentile Rank (ax2) limits from 0 to 1 for visual clarity
        ax2.set_ylim(0, 1)

        # --- Marking Extremes on Secondary Axis (Percentile Rank) ---

        # Extreme High Sentiment (95% - Historically Bearish for VIX, Bullish for S&P 500)
        ax2.axhline(y=0.95, color='darkred', linestyle='-', linewidth=1, alpha=0.9, label='95% Extreme High Sentiment')
        ax2.text(analysis_df.index[-1], 0.95, 'Extreme High (95%)', color='darkred', ha='right', va='bottom',
                 fontsize=9, bbox=dict(facecolor='white', alpha=0.7, edgecolor='none'))

        # Neutral Level
        ax2.axhline(y=0.50, color='gray', linestyle=':', linewidth=1, alpha=0.7, label='50% Neutral')

        # Extreme Low Sentiment (5% - Historically Bullish for VIX, Bearish for S&P 500)
        ax2.axhline(y=0.05, color='darkgreen', linestyle='-', linewidth=1, alpha=0.9, label='5% Extreme Low Sentiment')
        ax2.text(analysis_df.index[-1], 0.05, 'Extreme Low (5%)', color='darkgreen', ha='right', va='top', fontsize=9,
                 bbox=dict(facecolor='white', alpha=0.7, edgecolor='none'))

        # --- Final Plot Aesthetics ---

        # Title and legend
        plt.title('VIX Price vs. COT Non-Commercial Net Position Percentile Rank', fontweight='bold', pad=15)

        # Combine legends from both axes
        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines + lines2, labels + labels2, loc='upper left', frameon=True, fontsize='small')

        # Save and show
        plt.tight_layout()
        plt.savefig(file_name)
        plt.show()
        print(f"\nPlot generated and saved to {file_name}")