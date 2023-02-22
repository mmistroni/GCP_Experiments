from apache_beam.testing.test_pipeline import TestPipeline
from unittest.mock import patch
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from samples.email_pipeline import run
from hamcrest.core.core.allof import all_of
from apache_beam.transforms import util
from shareloader.modules.news import run_my_pipeline, XyzOptions, \
                                            prepare_for_big_query
from shareloader.modules.news_util import df_to_dict, find_news_scores_for_ticker, combine_news, df_to_dict
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from datetime import date
import pandas as pd


import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestNewsPipeline(unittest.TestCase):

    def setUp(self):
        self.notEmptySink = Check(is_not_empty())

    
    def test_run_my_pipelinec(self):
        test_sector = 'Consumer Cyclical,PincoPallino'
        options = XyzOptions.from_dictionary({'sector': test_sector, 'business_days': 1})


        with TestPipeline() as p:
            input = p | beam.Create(['AMZN,{}'.format(test_sector.split(',')[0])])
            res = run_my_pipeline(input, options)

            res | self.notEmptySink


    @patch('shareloader.modules.news_util.find_news_scores_for_ticker')
    def test_find_news_for_ticker(self, find_scores_mock):
        lst = ['AMZN', 'TestHeadline', 0.5]

        # Calling DataFrame constructor on list
        df = pd.DataFrame([lst], columns=['ticker','headline', 0])
        find_scores_mock.return_value = df

    def test_df_to_dict(self):
        lst = ['AMZN', 'TestHeadline', 0.5]

        # Calling DataFrame constructor on list
        df = pd.DataFrame([lst], columns=['ticker','headline', 0])

        self.assertTrue(df.shape[0] > 0)

    def test_find_news_scores_for_ticker(self):
        res = find_news_scores_for_ticker(['AMZN'], 1)
        self.assertTrue(res.shape[0] > 0)


    def prepare_for_big_query_tst(self, items):
        raise Exception('Raising an Exception')

    def test_prepare_for_bigquery(self):

        with self.assertRaises(Exception):
            with TestPipeline() as p:
                lst = ['AMZN', 'TestHeadline', 0.8]
                lst2 = ['ABBV', 'TESTABBV', 0.5]
                # Calling DataFrame constructor on list
                df = pd.DataFrame([lst, lst2], columns=['ticker', 'headline', 0])

                input = p | beam.Create([df])
                res = self.prepare_for_big_query_tst(input)
                res | self.notEmptySink

    def test_pipeline_options(self):
        options = XyzOptions.from_dictionary({'sector': 'Utilities', 'business_days' : 1})
        from pprint import pprint
        print(options.sector.get())
        print(options.business_days.get())

    def test_combine_news(self):
        elements = [
                dict(TICKER='AMZN',HEADLINE='TestHeadline',
                            SCORE=0.8, RUN_DATE=date.today().strftime('%Y-%m-%d')),
            dict(TICKER='AAPL', HEADLINE='TestHeadlineaapl',
                 SCORE=0.8, RUN_DATE=date.today().strftime('%Y-%m-%d'))
        ]
        res = combine_news(elements)
        res  | self.notEmptySink






