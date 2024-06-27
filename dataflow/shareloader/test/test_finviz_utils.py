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


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.debugSink = beam.Map(print)

    def test_canslim(self):
        res = get_canslim()
        pprint(res)

    def test_leaps(self):
        res = get_leaps()
        pprint(res)

    def test_universe(self):
        rres = get_universe_stocks()
        print(rres)


    def filter_defensive(self, input_dict):
        if ('debtOverCapital' in input_dict and input_dict['debtOverCapital'] < 0) \
             and ('dividendPaid' in input_dict and input_dict['dividendPaid']  == True) \
                 and ('epsGrowth' in input_dict and input_dict['epsGrowth'] >= 0.33) \
                 and ('positiveEps' in input_dict and  input_dict['positiveEps'] > 0) \
                 and ('priceToBookRatio' in input_dict and input_dict['priceToBookRatio'] > 0) :
            return True
        return False

    def filter_enterprise(self, input_dict):
        if ('debtOverCapital' in input_dict and input_dict['debtOverCapital'] < 0) \
             and ('dividendPaid' in input_dict and input_dict['dividendPaid']  == True) \
                 and ('epsGrowth' in input_dict and input_dict['epsGrowth'] >= 0.33) \
                 and ('positiveEps' in input_dict and  input_dict['positiveEps'] > 0) \
                 and ('priceToBookRatio' in input_dict and input_dict['priceToBookRatio'] > 0) :
            return True
        return False



    def test_gdefensive(self):
        key = os.environ['FMPREPKEY']

        res = get_graham_defensive(key)

        for data in res:
            if self.filter_defensive(data):
                pprint(data)

    def test_genterprise(self):
        key = os.environ['FMPREPKEY']

        res = get_graham_enterprise(key)

        print(res)

    def test_extra_watchlist(self):
        key = os.environ['FMPREPKEY']

        res = get_extra_watchlist()

        print(res)

    def test_new_high(self):

        res = get_new_highs()
        print(res)

    def test_finvizloader(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                     | 'Run Loader' >> beam.ParDo(FinvizLoader(key))
                     | self.debugSink
                     )

    def test_highlow(self):
        res = get_high_low()
        print(res)

if __name__ == '__main__':
    unittest.main()
