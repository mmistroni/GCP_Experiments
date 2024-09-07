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
from shareloader.modules.obb_standalone import OBBStandaloneClient



import asyncio
import apache_beam as beam

class AsyncProcess(beam.DoFn):

    def __init__(self, credentials):
        self.credentials = credentials
        self.client = OBBStandaloneClient(keys_dict=credentials)



    async def fetch_data(self, element: str):
        from openbb_yfinance.models.equity_profile import YFinanceEquityProfileFetcher as fetcher
        from openbb_yfinance.models.equity_quote import YFinanceEquityQuoteFetcher as quote_fetcher
        from openbb_fmp.models.equity_quote import FMPEquityQuoteFetcher as fmpquote_fetcher
        from openbb_fmp.models.equity_profile import FMPEquityProfileFetcher as fmpfetcher

        params = dict(symbol="AAPL")

        profile = await fmpfetcher.fetch_data(params, self.credentials)

        #quote = await fmpquote_fetcher.fetch_data(params, self.credentials)
        quote_data = await self.client.quote(element)

        profile_data = [d.model_dump(exclude_none=True) for d in profile]
        #quote_data = [d.model_dump(exclude_none=True) for d in quote]

        return [{'profile' : profile_data, 'quote' :quote_data}]



    def process(self, element: str):
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))

class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.debugSink = beam.Map(print)

    def test_sample_pipeline(self):
        credentials = {'fmp_api_key' : os.environ['FMPREPKEY']}
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                     | 'Run Loader' >> beam.ParDo(AsyncProcess(credentials))
                     | self.debugSink
                     )

if __name__ == '__main__':
    unittest.main()
