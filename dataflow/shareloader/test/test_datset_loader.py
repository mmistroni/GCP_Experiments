
import unittest
import requests
from lxml import etree
from io import StringIO, BytesIO
from shareloader.modules.share_datset_loader import get_prices
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date
import os

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestSharesDsetLoader(unittest.TestCase):


    def test_get_prices(self):
        print(get_prices('AAPL')[0:10])

    def test_pipeline(self):
        with TestPipeline() as p:
            input = (p | 'CREATE' >> beam.Create(['MSFT'])
                        | 'Getting PRICE' >> beam.FlatMap(lambda t: get_prices(t))
                        | beam.Map(print))
