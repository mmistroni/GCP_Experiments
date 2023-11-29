
import unittest
import argparse
from shareloader.modules.dftester_utils import DfTesterLoader
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



class TestDfTesterLoader(unittest.TestCase):

    def setUp(self) -> None:
        self.notEmptySink = Check(is_not_empty())
        self.debugSink = beam.Map(print)
        self.patcher = patch('shareloader.modules.share_datset_loader.XyzOptions._add_argparse_args')
        self.mock_foo = self.patcher.start()
        parser = argparse.ArgumentParser(add_help=False)

    def tearDown(self):
        self.patcher.stop()

    # https://beam.apache.org/documentation/pipelines/test-your-pipeline/
    def test_run_pipeline(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                       | 'Run Loader' >> beam.ParDo(DfTesterLoader(key, period='annual'))
                       | self.notEmptySink
                     )


