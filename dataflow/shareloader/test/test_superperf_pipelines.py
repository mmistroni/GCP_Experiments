
import unittest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from shareloader.modules.superperf_pipelines import run_leaps, run_canslim, run_buffetsix ,combine_pipelines
from collections import  OrderedDict

from datetime import date
import os
import pandas as pd

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestSuperPerfPipelines(unittest.TestCase):

    def setUp(self):
        self.notEmptySink = Check(is_not_empty())
        self.printSink = beam.Map(print)



    def test_run_leaps(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_leaps(p)
            res | self.printSink

    def test_run_canslim(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_canslim(p)
            res | self.printSink

    def test_buffetsix(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_buffetsix(p)
            res | self.printSink

    def test_combine_pipelines(self):
        with TestPipeline() as p:
            res = combine_pipelines(p)
            res | self.printSink



