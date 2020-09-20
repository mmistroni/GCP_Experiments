from mock import patch, Mock
from testing.email_pipeline import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from testing.email_pipeline import run
from hamcrest.core.core.allof import all_of
from apache_beam.transforms import util
from testing.email_pipeline import   split_fields, run_my_pipeline
from testing.bq_read_pipeline import join_sinks
import logging

import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class LeftJoinerFn(beam.DoFn):

    def __init__(self):
        super(LeftJoinerFn, self).__init__()

    def process(self, row, **kwargs):

        right_dict = dict(kwargs['right_list'])
        left_key = row[0]

        if left_key in right_dict:
            for each in row[1]:
                yield each + right_dict[left_key]

        else:
            for each in row[1]:
                yield each

class InnerJoinerFn(beam.DoFn):

    def __init__(self):
        super(InnerJoinerFn, self).__init__()

    def process(self, row, **kwargs):

        right_dict = dict(kwargs['right_list'])
        left_key = row[0]

        if left_key in right_dict:
            for each in row[1]:
                yield (left_key, each + right_dict[left_key])


class Display(beam.DoFn):
    def process(self, element):
        print(str(element))
        yield element

class MyTestOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key', default='mykey')
        parser.add_argument('--iexkey', default='none')

class TestBeamFunctions(unittest.TestCase):

    @patch('testing.email_pipeline.ReadFromText')
    def test_create_pipelne(self, mock_read_from_text):
        sample_list = ['One,Two,Three']
        with TestPipeline() as p:
            res = (
                    p
                    | beam.Create(sample_list)
                    | beam.Map(split_fields))

            assert_that(res, equal_to(['One']))

    @patch('testing.email_pipeline.get_prices')
    def test_create_pipeline2(self, mock_get_prices):
        get_prices_return = [{'High': 3098, 'Low': 3015.77001953125, 'Open': 3062, 'Close': 3055.2099609375, 'Volume': 4026365, 'AdjClose': 3055.2099609375, 'Ticker': 'AMZN'}]
        mock_get_prices.return_value = get_prices_return
        options = MyTestOptions()
        sink = Check(equal_to([get_prices_return]))
        with TestPipeline(options=options) as p:
            input = p |beam.Create(['AMZN,1,1'])
            run_my_pipeline(input, p.options, sink)

    def test_left_joins(self):

        with TestPipeline() as p:
            pcoll1 = p| beam.Create(
                   [ ('AMZN', [['a', 10, 20]]),
                      ('key2', [[('a', 12)], [('b', 21)], [('c', 13)]]),
                      ('key3', [[('a', 21)], [('b', 23)], [('c', 31)]])
                      ])
            pcoll2 = [('AMZN', [410]),
                      ('key2', [[('x', 20)]])]

            left_joined = (
                    pcoll1
                    | 'InnerJoiner: JoinValues' >> beam.ParDo(InnerJoinerFn(), right_list=pcoll2)
                    | 'Display' >> beam.ParDo(Display())
            )
            #p.run()




