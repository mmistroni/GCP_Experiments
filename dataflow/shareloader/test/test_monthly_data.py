from mock import patch, Mock
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from testing.email_pipeline import run
from hamcrest.core.core.allof import all_of
from apache_beam.transforms import util
from shareloader.modules.monthly_data import run_my_pipeline, map_to_dict, write_data
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import date


import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class MyTestOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key', default='mykey')
        parser.add_argument('--iexkey', default='none')

class TestBeamFunctions(unittest.TestCase):

    def test_run_my_pipelinec(self):
        with TestPipeline() as p:
            input = p | beam.Create(['AMZN,Consumer Cyclical'])
            res = run_my_pipeline(input)
            final = (res | 'Map' >> beam.Map(lambda x: x[['Ticker', 'Start_Price', 'End_Price', 'Performance']].to_dict())
                        | 'TODICT' >> beam.Map(lambda d: dict((k, v[0]) for k, v in d.items()))
                        | 'PRINT' >> beam.Map(print))

    def test_map_to_dict(self):
        import pandas as pd
        test_dict = {'Ticker': 'AMZN', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125, 'Performance': 0.046234802447688406,
                     'COB' : date.today().strftime('%Y-%m-%d')}
        dicts = [test_dict]
        df = pd.DataFrame(dicts)
        with TestPipeline() as p:
            input = p | beam.Create([df])
            res = map_to_dict(input)
            assert_that(res, equal_to([{'Ticker': 'AMZN', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125, 'Performance': 0.046234802447688406}]))

    def test_write_data(self):
        test_dict = {'Ticker': 'AMZN', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125, 'Performance': 0.046234802447688406}
        test_writing_set = [test_dict]
        sink = Check(equal_to(test_writing_set))

        with TestPipeline() as p:
            input = p | beam.Create([test_dict])
            write_data(input, sink)






