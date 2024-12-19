
import unittest
import requests
from lxml import etree
from io import StringIO, BytesIO
from shareloader.modules.sector_loader import run_my_pipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from shareloader.modules.sectors_utils import SectorRankGenerator
from unittest.mock import patch
from shareloader.modules.sector_loader import run_my_pipeline
from pandas.tseries.offsets import BDay

from datetime import date
import os
import pandas as pd

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestSectorLoader(unittest.TestCase):

    def setUp(self):
        self.notEmptySink = Check(is_not_empty())
        self.printSink = beam.Map(print)



    def test_run_my_pipeline(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_my_pipeline(p, key)
            res | self.printSink

    def test_sector_ranks(self):
        key = os.environ['FMPREPKEY']
        s = SectorRankGenerator(key, 10)

        res = s.get_rank()

        print(res)

    def test_compute_etf_historical(self):
        key = os.environ['FMPREPKEY']

        with TestPipeline() as p:
            res = run_my_pipeline(p, key)
            res   | self.notEmptySink

    def fetch_performance(self, sector, ticker, key):
        endDate = date.today()
        startDate = (endDate - BDay(252)).date()
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={startDate.strftime('%Y-%m-%d')}&to={endDate.strftime('%Y-%m-%d')}&apikey={key}"
        historical = requests.get(url).json().get('historical')
        df = pd.DataFrame(data=historical[::-1])
        df['date'] = pd.to_datetime(df.date)
        df['ticker'] = ticker
        df = df.set_index('date')
        return df

    def test_sector_ranking(self):
        # sample from https://wire.insiderfinance.io/unlocking-sector-based-momentum-strategies-in-asset-allocation-8560187f3ae3
        key = os.environ['FMPREPKEY']
        from collections import OrderedDict
        sectorsETF = OrderedDict({
            'Technology': 'XLK',
            'Health Care': 'XLV',
            'Financials': 'XLF',
            'Real Estate': 'SCHH',
            'Energy': 'XLE',
            'Materials': 'XLB',
            'Consumer Discretionary': 'XLY',
            'Industrials': 'VIS',
            'Utilities': 'VPU',
            'Consumer Staples': 'XLP',
            'Telecommunications': 'XLC',
            'S&P 500': '^GSPC'
        })

        momentum_periods = {
         '1M': 21,    # 1 month
         '3M': 63,    # 3 months
         '6M': 126,   # 6 months
         #    '12M': 252   # 12 months
         }

        holder = {}
        for sec, ticker in sectorsETF.items():
             data = self.fetch_performance(sec, ticker, key)
             holder[ticker] = data

        print('out')

    def test_yfinance(self):
        import yfinance as yf
        import pandas as pd
        import numpy as np

        # Define sector ETFs and benchmark
        sector_tickers = ['XLK',
                          'XLF',
                          'XLE',
                          'XLV',
                          'XLI',
                          'XLP',
                          'XLU',
                          'XLY',
                          'XLB',
                          'XLRE',
                          'XLC',
                          '^GSPC']
        sector_names = ['Technology',
                        'Financials',
                        'Energy',
                        'Health Care',
                        'Industrials',
                        'Consumer Staples',
                        'Utilities',
                        'Consumer Discretionary',
                        'Materials',
                        'Real Estate',
                        'Communication Services',
                        'S&P500']


        all_tickers = sector_tickers

        sector_dict = dict(zip(sector_tickers, sector_names))

        start_date = '2023-01-01'

        # Download historical data starting from January 2019
        data = yf.download(all_tickers, start=start_date, end=date.today().strftime('%Y-%m-%d'))['Adj Close']
        rename_dict = dict((k, v) for k, v in zip(all_tickers, sector_names))
        # Calculate daily returns
        daily_returns = data.pct_change().dropna()

        # Define momentum periods
        momentum_periods = {
            '1M': 21,  # 1 month
            '3M': 63,  # 3 months
            '6M': 126,  # 6 months
            '12M': 252  # 12 months
        }

        # Calculate momentum and rankings
        momentum_data = {}
        for period_name, period_days in momentum_periods.items():
            momentum = data[sector_tickers].pct_change(period_days)
            momentum = momentum.loc[start_date:]
            momentum_rank = momentum.rank(axis=1, ascending=False, method='first')
            momentum_rank = momentum_rank.shift(1)
            momentum_data[period_name] = momentum_rank

        holder = []
        for key in momentum_periods.keys():
            data = momentum_data[key]
            data = data.rename(index=sector_dict)
            holder.append(data.tail(1))
        alldf = pd.concat(holder)

        index_map = { 0 : '1M', 1 : '3M', 2   :'6M', 3 : '1Y'}


        alldf = alldf.reset_index(drop=True).rename(columns=sector_dict)


        transposed = alldf.T.rename(columns=index_map)

        cols = transposed.columns

        print(transposed[cols[::-1]])

