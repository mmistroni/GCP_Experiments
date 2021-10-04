
import unittest
from shareloader.modules.superperformers import load_all, filter_universe, extract_data_pipeline
from shareloader.modules.superperf_metrics import get_all_data, get_descriptive_and_technical
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
            def tickerCombiners(input):
                return ','.join(input)

            input = (p | 'Start' >> beam.Create([('TSCO', 'Something')])
                       | 'Extracting only ticker and Industry' >> beam.Map(lambda item: (item[0]))
                     )
            res = load_all(input, key)

            res | 'Printing out' >> beam.Map(print)

    def test_filter_universe(self):
        key = os.environ['FMPREPKEY']

        sample_data1 = get_all_data('TSCO', key)

        sink = Check(equal_to([sample_data1]))

        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create([sample_data1]))
        res = filter_universe(input)

        res | sink

    def test_mini_pipeline(self):
        key = os.environ['FMPREPKEY']

        printingSink = beam.Map(print)

        with TestPipeline() as p:
            tickers = (p | 'Starting' >> beam.Create([('TSCO', 'TmpIndustry')]))
            all_data = load_all(tickers, key)
            filtered = filter_universe(all_data)
            filtered | printingSink

    def test_getalldata(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print('Key is:{}|'.format(key))
        with TestPipeline() as p:
            tickers = (p | 'Starting' >> beam.Create([('TSCO', 'TmpIndustry'), ('AAPL', 'foo')])
                         | 'Map ' >> beam.Map(lambda i: i[0])
                         | 'Filter out Nones' >> beam.Filter(lambda item: item is not None)
                       )
            all_data = load_all(tickers, key)
            all_data  | printingSink















