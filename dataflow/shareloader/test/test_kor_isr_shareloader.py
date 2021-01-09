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
from shareloader.modules.utils import get_isr_and_kor, get_usr_adrs, get_latest_price_yahoo
from shareloader.modules.kor_isr_shareloader import find_diff, create_us_and_foreign_dict, \
                            map_ticker_to_html_string
from datetime import datetime
from pandas.tseries.offsets import BDay
from functools import reduce
import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)



def test_combine_data(elements):
    return reduce(lambda acc, current: acc + current, elements, '')


class TestKorIsrShareLoaderPipeline(unittest.TestCase):

    def test_get_usr_adrs(self):
        print(create_us_and_foreign_dict('<token>'))

    def test_remap(self):
        print(find_diff('AMZN', date(2021,1,6)))

    def test_map_ticker_to_html_string(self):
        tpls = [('AMZN', 'AMZN1', 'AMNZ2', '.5'),
                ('AAPL', 'AAPL1', 'AAPL2', '.9')]
        print(map_ticker_to_html_string(tpls))




