import math
import unittest
import numpy as np
from shareloader.modules.superperf_metrics import get_all_data, get_descriptive_and_technical, \
                get_financial_ratios, get_fmprep_historical

from shareloader.modules.premarket_loader import TrendTemplateLoader

from itertools import chain
from pandas.tseries.offsets import BDay
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
import os
import requests
import pandas as pd
from collections import OrderedDict
from datetime import date, datetime
import logging
from unittest.mock import patch



class TestPremarketLoader(unittest.TestCase):

    def setUp(self):
        self.patcher = patch('shareloader.modules.sector_loader.XyzOptions._add_argparse_args')
        self.mock_foo = self.patcher.start()


    def tearDown(self):
        self.patcher.stop()


    def get_historical(self, ticker, key, start_date, end_date):
        hist_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/{}?apikey={}'.format(ticker, key)
        data = requests.get(hist_url).json().get('historical')
        df=  pd.DataFrame(data=data)

        df = df[['date', 'close']].rename(columns={'close' : ticker})

        return df[ (df.date > start_date) & (df.date < end_date)][::-1]


    def test_get_fmprep_historical(self):
        key = os.environ['FMPREPKEY']
        res = get_fmprep_historical('AAPL', key, numdays=2, colname=None)

        data = [dict( (k, v)  for k, v in d.items() if k in ['date', 'symbol', 'open', 'adjClose', 'volume']) for d in res]

        df = pd.DataFrame(data=data)

        df['date'] = pd.to_datetime(df['date']).date()

        df['symbol'] = 'AAPL'

        print(df)

        self.assertTrue(df.shape[0] > 0)

        # cloud build test https://stackoverflow.com/questions/55022058/running-python-unit-test-in-google-cloud-build
    def test_get_rank(self):

        ## historical consituents from https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=79d4f398184fb636fa32ac1f95ed67e6

        def get_latest_price(ticker, key):
            stat_url = 'https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={token}'.format(symbol=ticker,
                                                                                                               token=key)
            res = requests.get(stat_url).json()[0]
            return res['price']

        key = os.environ['FMPREPKEY']

        GROUPBY_COL = 'GICS Sector'  # Use 'GICS Sector' or 'GICS Sub-Industry'
        S_AND_P_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        NUM_PER_GROUP = 3  # The top n winning stocks per group

        ticker_info = pd.read_html(S_AND_P_URL)[0]

        # Replace any dots with dashes in ticker names to prevent errors in
        # downloading A and B stocks
        tickers = [
            ticker.replace('.', '-')
            for ticker in ticker_info['Symbol'].unique().tolist()
        ]

        symbols = tickers[0:100]

        start_date = date(2022, 1, 1).strftime('%Y-%m-%d')
        end_date = date(2022, 12, 31).strftime('%Y-%m-%d')

        ticker_data = []

        for symbol in symbols:
            result = self.get_historical(symbol, key, start_date, end_date)
            ticker_data.append(result)
        from functools import reduce

        ticker_prices = reduce(lambda acc, item: acc.merge(item, on='date', how='left'), ticker_data[1:],
                               ticker_data[0])

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
            ticker_info[['Symbol', GROUPBY_COL]],
            on='Symbol',
            how='left',
        )

        # Find the ranking of each stock per sector
        growth['sector_rank'] = (
            growth
                .groupby(GROUPBY_COL)
            ['Growth']
                .rank(ascending=False)
        )

        # Filter to only the winning stocks, and sort the values
        growth = (
            growth[growth['sector_rank'] <= NUM_PER_GROUP]
                .sort_values(
                [GROUPBY_COL, 'Growth'],
                ascending=False,
            )
        )

        print(growth)

        tickers = [v for v in growth.Symbol.values if 'index' not in v]
        latest = map(lambda t: {'Symbol': t, 'Latest' : get_latest_price(t, key)}, tickers)

        historical = map(lambda t: {'Symbol': t, 'Latest' : get_latest_price(t, key)}, tickers)

        df = pd.DataFrame(data = latest)

        res = pd.merge(growth, df, on='Symbol')

        oldest = ticker_prices.tail(1).T.reset_index.rename(columns={"index": "Symbol"})

        mgd = pd.merge(res, oldest, on='Symbol')
        print(mgd)



    @patch('shareloader.modules.premarket_loader.PremarketEmailSender.send')
    def test_premarketcombiner(self, send_mock):
        import io
        from shareloader.modules.premarket_loader import PreMarketCombineFn, PremarketEmailSender, send_email_pipeline
        TESTDATA = '''date,ticker,close,200_ma,150_ma,50_ma,slope,52_week_low,52_week_high,trend_template
                      20210101,AAPL, 50.0,48.1,49.1,49.5,1.0,44.1,40.1,true
                      20210101,MSFT, 150.0,148.1,149.1,149.5,11.0,144.1,140.1,true'''

        send_mock.return_value = True

        df = pd.read_csv(io.StringIO(TESTDATA), sep=",")

        records = df.to_dict('records')

        key = os.environ['SENDGRIDKEY']

        with TestPipeline() as p:
            res = (p | 'START' >> beam.Create(records))
            send_email_pipeline(res, key)

        self.assertEquals(1, send_mock.call_count)

    def test_gettrendtemplate(self):
        key = os.environ['FMPREPKEY']

        with TestPipeline() as p:
            res = (p | 'START' >> beam.Create(['MCD'])
                   | 'Getting fundamentals' >> beam.ParDo(TrendTemplateLoader(key, numdays='500'))

                   )
























