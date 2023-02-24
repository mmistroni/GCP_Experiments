
import unittest
import requests
from lxml import etree
from io import StringIO, BytesIO
from shareloader.modules.shares_to_sector import get_stocks_for_sector, get_industry,\
                                                get_all_sectors, run_my_pipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date
import os

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestSharesToSector(unittest.TestCase):

    def setUp(self):
        self.notEmptySink = Check(is_not_empty())


    def test_get_all_sectors(self):
        self.assertTrue(get_all_sectors('https://www.stockmonitor.com/sectors'))

    def test_get_stocks_for_sector(self):
        sectors_tpl = ('Basic Materials Sector',
                                    'https://www.stockmonitor.com/sector/basic-materials/')
        self.assertTrue(get_stocks_for_sector(sectors_tpl))

    def test_pipeline(self):

        with TestPipeline() as p:
            input = (p | 'get Sectors Url' >> beam.Create(['https://www.stockmonitor.com/sectors'])
                        | 'Getting Sectors' >> beam.FlatMap(lambda url: get_all_sectors(url))
                        | 'Get Stocks For Sector' >> beam.Map(lambda tpl: get_stocks_for_sector(tpl))
                        | self.notEmptySink
                     )
