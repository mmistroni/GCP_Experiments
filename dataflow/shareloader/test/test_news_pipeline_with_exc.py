from mock import patch, Mock
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from samples.email_pipeline import run
from hamcrest.core.core.allof import all_of
from apache_beam.transforms import util
from shareloader.modules.news import run_my_pipeline, XyzOptions, \
                                            prepare_for_big_query
from shareloader.modules.news_util import df_to_dict, find_news_scores_for_ticker, combine_news
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import date
import pandas as pd
from shareloader.modules.metrics import get_historical_data_yahoo


import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestNewsPipelineWithExc(unittest.TestCase):

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

    def test_yahoo(self):
        df = get_historical_data_yahoo('AMZNX', '', date.today(), date.today())
        print(df['AMZNX'].values[0] if df.shape[0] > 0 else  0)
