
import unittest
from shareloader.modules.superperformers import load_all, filter_universe, extract_data_pipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
import os

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)



class TestSuperPerformers(unittest.TestCase):


    def test_loadall(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create([('AAPL', 'Something')]))
            res = load_all(input, key)

            res | 'Printing out' >> beam.Map(print)

    def test_filter_universe(self):
        sample_data1 = {'marketCap': 2488538759168.0, 'price': 149.62, 'avgVolume': 77642960, 'priceAvg50': 146.61543,
                        'priceAvg200': 132.82138, 'eps': 5.108, 'pe': 29.472397, 'sharesOutstanding': 16632393792,
                        'yearHigh': 151.68, 'yearLow': 103.1, 'exchange': 'NASDAQ', 'change': -0.0900116,
                        'open': 149.45, 'priceAvg20': 143, 'allTimeHigh': 151.12, 'allTimeLow': 0.049107,
                        'weeks52High': 151.68, 'ticker': 'AAPL', 'changeFromOpen': 0.17000000000001592,
                        'eps_growth_this_year': 0.10609857978279026,
                        'eps_growth_past_5yrs': 0.5856287425149701,
                        'eps_growth_next_year': 3.665, 'eps_growth_qtr_over_qtr': 0.5,
                        'net_sales_qtr_over_qtr': 0.29097606715484909, 'grossProfitMargin': 0.38233247727810865,
                        'returnOnEquity': 0.8786635853012749, 'dividendPayoutRatio': 0.2452665865426486,
                        'dividendYield': 0.007053332328502797, 'institutionalHoldings': 9490514216,
                        'institutionalHoldingsPercentage': 0.5706042277910011, 'sharesFloat': 16513305231.0}


        sink = Check(equal_to([sample_data1]))

        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create([sample_data1]))
        res = filter_universe(input)

        res | sink












