
import unittest
from shareloader.modules.share_datset_loader import get_industry, GetAllTickers, run_my_pipeline
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



class TestSharesDsetLoader(unittest.TestCase):


    def test_GetAllTickers(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create(['starting'])
                       | 'Getting All Tickers' >> beam.ParDo(GetAllTickers(key))
                       | 'Sample N elements' >> beam.combiners.Sample.FixedSizeGlobally(20)
                       | beam.Map(print))

    def test_get_industry(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                     | 'Get Industry' >> beam.Map(lambda t: get_industry(t, key))
                     | beam.Map(print))

    def test_write_to_sink(self):
        key = os.environ['FMPREPKEY']
        expected = [('AAPL', 1, 'Consumer Electronics')]
        sink = Check(equal_to(expected))

        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create([('AAPL', 1, 'Consumer Electronics')])
                     | 'Get Industry' >> beam.Map(lambda t: get_industry(t, key))
                     | beam.Map(print))


    def test_run_my_pipeline(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create(['FOO']))
            res = run_my_pipeline(input, key)
            (res | 'Print' >> beam.Map(print))


