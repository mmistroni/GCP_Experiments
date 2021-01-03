from mock import patch, Mock
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from mock import patch, Mock
from edgar_flow.modules.edgar_utils import  cusip_to_ticker, ParseForm4, EdgarCombineFn
from edgar_flow.modules.edgar_daily_form4 import find_current_day_url


import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestEdgarDailyForm4Pipeline(unittest.TestCase):

    def test_enhance_form4(self):

        sample_list = [('20201009', 'https://www.sec.gov/Archives/edgar/data/925741/0001437749-20-021024.txt'),
                       ('20201009', 'https://www.sec.gov/Archives/edgar/data/925741/0001437749-20-021025.txt'),
                       ('20201009', 'https://www.sec.gov/Archives/edgar/data/925741/0001437749-20-021026.txt')]
        with TestPipeline() as p:
            ( p | beam.Create(sample_list)
              | 'parsing form 4 filing' >> beam.ParDo(ParseForm4())
              | 'Combining all ' >> beam.CombinePerKey(sum)
              #| 'Mapping to Tuple' >> beam.Map(lambda tpl: (tpl[0][0], tpl[0][1], tpl[1]))
              | 'Mapping to be lin line withedgar fn' >> beam.Map(lambda tpl: ['', '' ,tpl[0], tpl[1]])

              | 'Combining to get top 30' >> beam.CombineGlobally(EdgarCombineFn())

              | 'Printing out' >> beam.Map(print)#
              )

    def test_find_current_day_url(self):
        from datetime import date
        from pandas.tseries.offsets import BDay
        dt  = date.today() - BDay(1)
        print('Current daY URL:{}'.format(find_current_day_url(dt)))
