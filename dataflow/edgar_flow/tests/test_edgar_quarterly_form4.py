from mock import patch, Mock
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from mock import patch, Mock
from edgar_flow.modules.edgar_utils import  cusip_to_ticker, ParseForm4, EdgarCombineFn
from edgar_flow.modules.edgar_quarterly_form4 import find_quarter_urls
import urllib


import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestEdgarQuarterlyForm4Pipeline(unittest.TestCase):

    def test_find_quarter_urls(self):
        print(find_quarter_urls('foo'))

    def test_readRemote(self):
        element = 'https://www.sec.gov/Archives/edgar/daily-index/2020/QTR4/master.20201112.idx'
        data = urllib.request.urlopen(element)  # it's a file like object and works just like a file
        data = [line for line in data]
        print(data)



