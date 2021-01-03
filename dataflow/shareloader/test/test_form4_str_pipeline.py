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
from shareloader.modules.metrics import get_historical_data_yahoo
import pandas as pd


import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)

from datetime import datetime
from pandas.tseries.offsets import BDay
class TestForm4StrPipeline(unittest.TestCase):

    def test_run_my_pipelinec(self):
        test_sector = 'Consumer Cyclical,PincoPallino'
        options = XyzOptions.from_dictionary({'sector': test_sector, 'business_days': 1})

        with TestPipeline() as p:
            input = (p | beam.Create([{'TICKER':'AMZN', 'COB': '2020-12-04'}])
                     | 'Extractign only what we need..' >> beam.Map(
                        lambda elem: (elem['TICKER'],
                                      elem['COB']))
                     | 'Adding cob Prices' >> beam.Map(lambda tpl: (tpl[0], tpl[1],
                                                                    get_historical_data_yahoo(tpl[0], '', tpl[1],
                                                                                              tpl[1]),
                                                                    tpl[2]))
                     | 'Extracting Price ' >> beam.Map(lambda tpl: (tpl[0], tpl[1],
                                                                    tpl[2]['AMZN'].values[0] if tpl[2].shape[
                                                                                                    0] > 0 else 0,
                                                                    tpl[3]))
                     | 'Filtering Out Volatile Prices' >> beam.Filter(lambda tpl: tpl[2] > 0)
                     | 'Printing Out' >> beam.Map(print)
                     )


