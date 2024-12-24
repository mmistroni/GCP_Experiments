import apache_beam as beam
import logging
import requests
from itertools import chain
from io import StringIO
from datetime import date, timedelta, datetime
from pandas.tseries.offsets import BDay
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
from .marketstats_utils import get_senate_disclosures
from functools import reduce
from collections import OrderedDict


def fetch_performance(sector, ticker, key, start_date):
    end_date = date.today().strftime('%Y-%m-%d')
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={start_date}&to={end_date}&apikey={key}"
    historical = requests.get(url).json().get('historical')
    df = pd.DataFrame(data=historical[::-1])[['date', 'adjClose']]
    df['date'] = pd.to_datetime(df.date)
    df = df.rename(columns={'adjClose': sector})
    df = df.set_index('date')
    return df
def get_sector_rankings(key):
    # sample from https://wire.insiderfinance.io/unlocking-sector-based-momentum-strategies-in-asset-allocation-8560187f3ae3
    sector_tickers = OrderedDict([('XLK', 'Technology'), ('XLF', 'Financials'), ('XLE', 'Energy'),
                                  ('XLV', 'Health Care'), ('XLI', 'Industrials'), ('XLP', 'Consumer Staples'),
                                  ('XLU', 'Utilities'), ('XLY', 'Consumer Discretionary'), ('XLB', 'Materials'),
                                  ('XLRE', 'Real Estate'), ('XLC', 'Communication Services'), ('^GSPC', 'S&P500')
                                  ])
    holder = []

    start_date  = (date.today() - BDay(300)).date().strftime('%Y-%m-%d')

    for ticker, sec in sector_tickers.items():
         data = fetch_performance(sec, ticker, key, start_date=start_date)
         holder.append(data)

    data = pd.concat(holder, axis=1)

    # Define momentum periods
    momentum_periods = {
        '1M': 21,  # 1 month
        '3M': 63,  # 3 months
        '6M': 126,  # 6 months
        '12M': 252  # 12 months
    }

    # Calculate momentum and rankings
    momentum_data = {}
    sector_names = list(sector_tickers.values())
    for period_name, period_days in momentum_periods.items():
        momentum = data[sector_names].pct_change(period_days)
        momentum = momentum.loc[start_date:]
        momentum_rank = momentum.rank(axis=1, ascending=False, method='first')
        momentum_rank = momentum_rank.shift(1)
        momentum_data[period_name] = momentum_rank

    holder = []
    for key in momentum_periods.keys():
        data = momentum_data[key]
        #data = data.rename(index=sector_tickers)
        holder.append(data.tail(1))
    alldf = pd.concat(holder)

    index_map = {0: '1M', 1: '3M', 2: '6M', 3: '1Y'}

    alldf = alldf.reset_index(drop=True)

    transposed = alldf.T.rename(columns=index_map)

    cols = transposed.columns

    data = transposed[cols[::-1]]
    return data.reset_index().to_dict('records')


class ETFHistoryCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    logging.info('Adding{}'.format(input))
    logging.info('acc is:{}'.format(accumulator))
    accumulator.append(input)
    return accumulator

  def merge_accumulators(self, accumulators):

    return list(chain(*accumulators))

  def extract_output(self, sum_count):
    return sum_count


class SectorsEmailSender(beam.DoFn):
  def __init__(self, recipients, key):
      self.recipients = recipients.split(',')
      self.key = key

  def _build_personalization(self, recipients):
      personalizations = []
      for recipient in recipients:
          logging.info('Adding personalization for {}'.format(recipient))
          person1 = Personalization()
          person1.add_to(Email(recipient))
          personalizations.append(person1)
      return personalizations

  def _build_html_message(self, rows):
      html = '<table border="1">'
      header_row = "<tr><th>Sector</th><th>1Y</th><th>6M</th><th>3M</th><th>1M</th></tr>"
      row_template = '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'

      for dct in rows:
          data = [dct['index'], dct['1Y'], dct['6M'], dct['3M'], dct['1M']]
          html += row_template.format(*data)
      html += '</table>'
      return html

  def process(self, element):
      sector_returns = element
      logging.info(f'Processing returns:\n{sector_returns}')
      data = self._build_html_message(element)
      content = \
            '''
            <html>
               <body>
                 <p>Compare Results against informations here https://www.investopedia.com/articles/trading/05/020305.asp</p>
                 <br><br>{}
               </body></html>'''.format(data)

      message = Mail(
          from_email='gcp_cloud_mm@outlook.com',
          subject='Sectors Return for last 4 Months',
          html_content=content)

      personalizations = self._build_personalization(self.recipients)
      for pers in personalizations:
          message.add_personalization(pers)

      sg = SendGridAPIClient(self.key)

      response = sg.send(message)
      logging.info('Mail Sent:{}'.format(response.status_code))
      logging.info('Body:{}'.format(response.body))

class SectorRankGenerator:
    GROUPBY_COL = 'GICS Sector'  # Use 'GICS Sector' or 'GICS Sub-Industry'
    S_AND_P_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

    def __init__(self, fmpKey, numPerGroup):
        self.key = fmpKey
        self.numPerGroup = numPerGroup
        self.end_date = date.today()
        self.start_date = (self.end_date - BDay(120)).date()

    def get_latest_price(self, ticker, key):
        stat_url = 'https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={token}'.format(symbol=ticker,
                                                                                                   token=key)
        res = requests.get(stat_url).json()[0]
        return res['price']

    def get_historical(self, ticker, key, start_date, end_date):
        hist_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/{}?apikey={}'.format(ticker, key)
        data = requests.get(hist_url).json().get('historical')
        df = pd.DataFrame(data=data)
        df = df[['date', 'close']].rename(columns={'close' : ticker})
        return df[ (df.date > start_date) & (df.date < end_date)][::-1]

    def get_sandp_historicals(self):
        return pd.read_html(self.S_AND_P_URL)[0]

    def get_ticker_prices(self, symbols):
        ticker_data = []
        start_date = self.start_date.strftime('%Y-%m-%d')
        end_date = self.end_date.strftime('%Y-%m-%d')

        for symbol in symbols:
            result = self.get_historical(symbol, self.key, start_date, end_date)
            ticker_data.append(result)

        return reduce(lambda acc, item: acc.merge(item, on='date', how='left'),
                            ticker_data[1:], ticker_data[0])

    def get_rank(self):
        # Need to learn how this work
        ## historical consituents from https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=79d4f398184fb636fa32ac1f95ed67e6
        key = self.key
        ticker_info = self.get_sandp_historicals()
        tickers = [
            ticker.replace('.', '-')
            for ticker in ticker_info['Symbol'].unique().tolist()
        ]
        ticker_prices = self.get_ticker_prices(tickers)
        ticker_prices = ticker_prices.dropna().reset_index().drop(columns='date')

        growth = 100 * (ticker_prices.iloc[-1] / ticker_prices.iloc[0] - 1)
        growth = (
            growth
                .to_frame()
                .reset_index()
                # .drop(columns=['level_0'])
                .rename(columns={'index': 'Symbol', 0: 'Growth'})
        )

        growth = growth.merge(
            ticker_info[['Symbol', self.GROUPBY_COL]],
            on='Symbol',
            how='left',
        )

        # Find the ranking of each stock per sector
        growth['sector_rank'] = (
            growth
                .groupby(self.GROUPBY_COL)
            ['Growth']
                .rank(ascending=False)
        )

        # Filter to only the winning stocks, and sort the values
        growth = (
            growth[growth['sector_rank'] <= self.numPerGroup]
                .sort_values(
                [self.GROUPBY_COL, 'Growth'],
                ascending=False,
            )
        )

        tickers = [v for v in growth.Symbol.values if 'index' not in v]
        latest = map(lambda t: {'Symbol': t, 'Latest' : self.get_latest_price(t, key)}, tickers)
        df = pd.DataFrame(data = latest)
        res = pd.merge(growth, df, on='Symbol')
        oldest = ticker_prices.tail(1).T.reset_index().rename(columns={"index": "Symbol"})
        mgd = pd.merge(res, oldest, on='Symbol')

        return mgd







