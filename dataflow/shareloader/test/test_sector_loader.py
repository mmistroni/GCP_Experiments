
import unittest
import requests
from lxml import etree
from io import StringIO, BytesIO
from shareloader.modules.sector_loader import run_my_pipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from unittest.mock import patch
from datetime import date
import os

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestSectorLoader(unittest.TestCase):

    def setUp(self):
        self.notEmptySink = Check(is_not_empty())
        self.patcher = patch('shareloader.modules.sector_loader.XyzOptions._add_argparse_args')
        self.mock_foo = self.patcher.start()
        self.printSink = beam.Map(print)


    def tearDown(self):
        self.patcher.stop()



    def test_run_my_pipeline(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_my_pipeline(p, key)
            res | self.printSink