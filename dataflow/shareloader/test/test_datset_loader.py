
import unittest
import argparse
from shareloader.modules.share_datset_loader import get_industry, GetAllTickers, run_my_pipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import os
from unittest.mock import patch
from apache_beam.options.pipeline_options import PipelineOptions

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)



class TestSharesDsetLoader(unittest.TestCase):

    def setUp(self) -> None:
        self.notEmptySink = Check(is_not_empty())
        self.patcher = patch('shareloader.modules.share_datset_loader.XyzOptions._add_argparse_args')
        self.mock_foo = self.patcher.start()
        parser = argparse.ArgumentParser(add_help=False)

    def tearDown(self):
        self.patcher.stop()

    # https://beam.apache.org/documentation/pipelines/test-your-pipeline/
    def test_GetAllTickers(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['starting'])
                       | 'Getting All Tickers' >> beam.ParDo(GetAllTickers(key))
                       | 'Sample N elements' >> beam.combiners.Sample.FixedSizeGlobally(20)
                       | self.notEmptySink
                     )

    def test_get_industry(self):
        key = os.environ['FMPREPKEY']
        expected = ['Consumer Electronics']
        industrySink = Check(equal_to(expected))

        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                     | 'Get Industry' >> beam.Map(lambda t: get_industry(t, key))
                     | industrySink
                     )

    def test_write_to_sink(self):
        key = os.environ['FMPREPKEY']
        expected = ['Consumer Electronics']
        industrySink = Check(equal_to(expected))
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                     | 'Get Industry' >> beam.Map(lambda t: get_industry(t, key))
                     | industrySink
                     )

    @patch('shareloader.modules.share_datset_loader.get_industry')
    @patch('shareloader.modules.share_datset_loader.GetAllTickers.process')
    def test_run_my_pipeline(self, processMock, getIndustryMock):
        key = os.environ['FMPREPKEY']

        processMock.return_value =  ['AMZN']
        getIndustryMock.return_value = 'Consumer Durables'
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['start']))
            res = run_my_pipeline(input, key)
            final = res | self.notEmptySink


