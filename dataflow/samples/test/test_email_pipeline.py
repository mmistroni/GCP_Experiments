from unittest.mock import patch, Mock
from samples.email_pipeline import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from samples.email_pipeline import run
from hamcrest.core.core.allof import all_of
from apache_beam.transforms import util
from samples.email_pipeline import   split_fields, run_my_pipeline
from samples.bq_read_pipeline import join_sinks
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
                print(each)
                yield each + right_dict[left_key]

        else:
            for each in row[1]:
                yield each

class AnotherLeftJoinerFn(beam.DoFn):

    def __init__(self):
        super(AnotherLeftJoinerFn, self).__init__()

    def process(self, row, **kwargs):

        right_dict = dict(kwargs['right_list'])
        left_key = row[0]
        left = row[1]
        print('Left is of tpe:{}'.format(type(left)))
        if left_key in right_dict:
            print('Row is:{}'.format(row))
            right = right_dict[left_key]
            left.update(right)
            yield (left_key, left)


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

    @patch('samples.email_pipeline.ReadFromText')
    def test_create_pipelne(self, mock_read_from_text):
        sample_list = ['One,Two,Three']
        with TestPipeline() as p:
            res = (
                    p
                    | beam.Create(sample_list)
                    | beam.Map(split_fields))

            assert_that(res, equal_to(['One']))

    @patch('samples.email_pipeline.get_prices')
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
            pcoll1 = p| 'Create coll1' >> beam.Create(
                   [ ('AMZN', {'price': 20, 'performance' : 20}),
                      ('key2', {'price': 2, 'performance' : 1}),
                      ('key3', {'price': 3, 'performance' : 3})
                      ])
            pcoll2 = p| 'Create coll2' >> beam.Create([('AMZN', {'count':410}),
                      ('key2', {'count':4})]
                                    )
            left_joined = (
                    pcoll1
                    | 'InnerJoiner: JoinValues' >> beam.ParDo(AnotherLeftJoinerFn(),
                                                right_list=beam.pvalue.AsIter(pcoll2))
                    | 'Display' >> beam.Map(print)
            )

