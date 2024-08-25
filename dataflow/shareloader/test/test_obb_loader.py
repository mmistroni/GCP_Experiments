import unittest
from shareloader.modules.finviz_utils import get_universe_stocks, get_canslim, get_leaps,\
                                            get_graham_defensive, get_graham_enterprise,\
                                            get_extra_watchlist, get_new_highs, FinvizLoader, \
                                            get_high_low
from pprint import pprint
import os
from shareloader.modules.superperf_metrics import get_dividend_paid
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import requests
import zipfile
import xml.etree.ElementTree as ET
from io import  BytesIO
import camelot


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.debugSink = beam.Map(print)
        self.credentials = {
          'benzinga_api_key': os.environ['BENZINGAKEY'],
         'econdb_api_key': None,
         'fmp_api_key': os.environ['FMPREPKEY'],
         'fred_api_key': os.environ['FREDKEY'],
         'intrinio_api_key': os.environ['INTRINIOKEY'],
         'nasdaq_api_key': os.environ['NASDAQKEY']
        }


    async def fetch_obb(self):

        from openbb_yfinance.models.equity_historical import YFinanceEquityHistoricalFetcher as fetcher
        params = dict(symbol="AAPL")

        data = await fetcher.fetch_data(params, self.credentials)
        return [d.model_dump(exclude_none=True) for d in data]

    def test_sample(self):

        records = self.fetch_obb()
        print(records)




if __name__ == '__main__':
    unittest.main()
