
import unittest
import requests
from lxml import etree
from io import StringIO, BytesIO
from shareloader.modules.sectors_pipelines import run_sector_loader_pipeline, run_sector_loader_finviz, run_pipelines
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from shareloader.modules.sectors_utils import SectorRankGenerator, get_sector_rankings ,SectorsEmailSender, \
                                            get_finviz_performance, fetch_index_data
from unittest.mock import patch
from pandas.tseries.offsets import BDay
import yfinance as yf

import pandas as pd
import numpy as np
from collections import  OrderedDict

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
            res = run_pipelines(p, key, '')


            res | self.printSink

    def test_sector_ranks(self):
        key = os.environ['FMPREPKEY']
        s = SectorRankGenerator(key, 10)

        res = s.get_rank()

        print(res)

    def test_compute_etf_historical(self):
        key = os.environ['FMPREPKEY']

        with TestPipeline() as p:
            res = run_sector_loader_pipeline(p, key)
            res   | self.notEmptySink

    def test_sector_ranking(self):
        # sample from https://wire.insiderfinance.io/unlocking-sector-based-momentum-strategies-in-asset-allocation-8560187f3ae3
        key = os.environ['FMPREPKEY']

        res = get_sector_rankings(key)

        from pprint import pprint
        pprint(res)




    def download_from_yf(self, tickers, start_date, end_date):
        return yf.download(tickers, start=start_date, end=end_date)['Adj Close']


    def test_yfinance(self):

        # Define sector ETFs and benchmark
        sector_tickers = OrderedDict([('XLK', 'Technology'),
                                      ('XLF','Financials'),
                                      ('XLE', 'Energy'),
                                      ('XLV', 'Health Care'),
                                      ('XLI',  'Industrials'),
                                      ('XLP', 'Consumer Staples'),
                                      ('XLU', 'Utilities'),
                                      ('XLY', 'Consumer Discretionary'),
                                      ('XLB', 'Materials'),
                                      ('XLRE', 'Real Estate'),
                                      ('XLC', 'Communication Services'),
                                      ('^GSPC' , 'S&P500')
                                      ])
        all_tickers = list(sector_tickers.keys())

        sector_dict = sector_tickers

        start_date = '2023-01-01'
        end_date = date.today().strftime('%Y-%m-%d')

        # Download historical data starting from January 2019

        data = self.download_from_yf(all_tickers, start_date, end_date)


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


    def test_get_finviz_result(self):
        res = get_finviz_performance()
        from pprint import pprint
        pprint(res)

    def test_finviz_pipeline(self):
        import logging
        debug = beam.Map(logging.info)
        with TestPipeline() as p:

            res = run_sector_loader_finviz(p)
            res   | debug

    def test_historicals(self):
        key = os.environ['FMPREPKEY']
        sink = beam.Map(print)
        with TestPipeline() as p:
            res =  (p | 'Starting fvz' >> beam.Create([
                                                            #'^GSPC', '^NDX', '^DJI','^RUT',
                                                         '^NYA'
                                                        ])
                        | 'Fetch data 2' >> beam.Map(lambda ticker: fetch_index_data(ticker, key))
                        | 'To Sink' >> sink
                        )
    def test_fetchistring(self):
        pass


        

