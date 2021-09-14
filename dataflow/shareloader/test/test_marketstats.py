import os
import unittest
from shareloader.modules.marketstats_utils import get_all_stocks
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date
from shareloader.modules.marketstats_utils import get_all_stocks, get_prices2


class TestShareLoader(unittest.TestCase):
    def test_all_stocks(self):
        key = os.environ['FMPREPKEY']
        print(get_all_stocks(key))

    def test_run_sample(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
                 (p
                  | 'Get List of Tickers' >> beam.Create(get_all_stocks(key))
                  | 'Getting Prices' >> beam.Map(lambda symbol: get_prices2(symbol, key))
                  #| 'Filtering blanks' >> beam.Filter(lambda d: len(d) > 0)
                  | 'Print out' >> beam.Map(print)
                  )


if __name__ == '__main__':
    unittest.main()