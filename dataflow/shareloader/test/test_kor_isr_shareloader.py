from unittest.mock import patch, Mock
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date
from shareloader.modules.kor_isr_shareloader import find_diff,  \
                            map_ticker_to_html_string, combine_to_html_rows, run_my_pipeline
from functools import reduce
import unittest
import os
from apache_beam.options.pipeline_options import PipelineOptions
from unittest import skip


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)



def test_combine_data(elements):
    return reduce(lambda acc, current: acc + current, elements, '')


class TestKorIsrShareLoaderPipeline(unittest.TestCase):

    def setUp(self):
        self.patcher = patch('shareloader.modules.sector_loader.XyzOptions._add_argparse_args')
        self.mock_foo = self.patcher.start()


    def tearDown(self):
        self.patcher.stop()

    def test_run_my_pipeline(self):
        key = os.environ['FMPREPKEY']
        print('KEY IS:{}'.format(key))
        filter_function = lambda d: d['changesPercentage'] > 2
        with beam.Pipeline(options=PipelineOptions()) as p:
            result = (p | 'Start' >> beam.Create(['VEDL.NS', 'WIPRO.NS', '2378.HK',
                                                   '6160.HK', '9618.HK'])
                      )
            res = run_my_pipeline(result, key, filter_function)
            res | 'Print out' >> beam.Map(print)



    def test_map_ticker_to_html_string(self):
        tpls = [('AMZN', 'AMZN1', 'AMNZ2', '.5'),
                ('AAPL', 'AAPL1', 'AAPL2', '.9')]
        print(list(map_ticker_to_html_string(tpls)))



    @skip('to review')
    def test_combine_to_html_rows(self):
        test_elems = ['<tr><td>AMZN</td><td>AMZN1</td><td>AMNZ2</td><td>.5</td></tr>', '<tr><td>AAPL</td><td>AAPL1</td><td>AAPL2</td><td>.9</td></tr>']
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | beam.Create(test_elems)
                     | 'Combining..' >> beam.CombineGlobally(combine_to_html_rows)
                     | 'Out' >> beam.Map(print))





