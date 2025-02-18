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
from datetime import date
from apache_beam.options.pipeline_options import PipelineOptions
from shareloader.modules.obb_utils import AsyncProcess, AsyncProcessSP500Multiples, ProcessHistorical
from shareloader.modules.launcher import StockSelectionCombineFn



import asyncio
import apache_beam as beam


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.debugSink = beam.Map(print)

    def test_sample_pipeline(self):
        credentials = {'key' : os.environ['FMPREPKEY']}
        cob = date(2024, 10, 4)
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL,NVDA,AMZN,T'])
                     | 'Run Loader' >> beam.ParDo(AsyncProcess(credentials, cob ,price_change=0.00001))
                     | 'combining' >> beam.CombineGlobally(StockSelectionCombineFn())
                     | self.debugSink
                     )

    def test_sample_pipeline2(self):
        credentials = {'fmp_api_key' : os.environ['FMPREPKEY']}
        cob = date(2024, 10, 4)
        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create(['shiller_pe_month', 'pe_month', 'earnings_growth_year'])
                     | 'Run Loader' >> beam.ParDo(AsyncProcessSP500Multiples(credentials))
                     | 'Combine sp' >> beam.CombineGlobally(lambda x: '<br><br>'.join(x))
                     | self.debugSink
                     )

    def test_sample_pipeline3(self):
        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create(['AAPL,NVDA,AMZN,T'])
                     | 'Run Loader' >> beam.ParDo(ProcessHistorical(os.environ['FMPREPKEY'], date.today()))
                     | self.debugSink
                     )
    def test_combine_pipeline(self):
        credentials = {'fmp_api_key' : os.environ['FMPREPKEY']}
        cob = date(2024, 10, 4)
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL,NVDA,AMZN,T'])
                     | 'Run Loader' >> beam.ParDo(AsyncProcess(credentials, cob ,price_change=0.001))
                     | 'maps' >> beam.Map(lambda d: d['ticker'])
                     | 'combiining' >> beam.CombineGlobally(lambda x: ','.join(x))
                     | 'Run LoaderHist' >> beam.ParDo(ProcessHistorical(os.environ['FMPREPKEY'], date.today()))
                     | self.debugSink)


if __name__ == '__main__':
    unittest.main()
