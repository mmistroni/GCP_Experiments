
import unittest
import requests
from lxml import etree
from io import StringIO, BytesIO
from shareloader.modules.shares_to_sector import get_stocks_for_sector, get_industry,\
                                                get_all_sectors, run_my_pipeline
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


class TestSharesToSector(unittest.TestCase):


    def test_get_all_sectors(self):
        print(get_all_sectors('https://www.stockmonitor.com/sectors'))

    def test_get_stocks_for_sector(self):
        sectors_tpl = ('Basic Materials Sector',
                                    'https://www.stockmonitor.com/sector/basic-materials/')
        print(get_stocks_for_sector(sectors_tpl))

    def test_get_industry(self):
        iexkey = os.environ['IEXAPI_KEY']
        stocks = [('asix', 'AdvanSix Inc', 'Basic Materials Sector'),
                  ('aem', 'Agnico Eagle Mines Limited', 'Basic Materials Sector'),
                  ('apd', 'Air Products and Chemicals, Inc', 'Basic Materials Sector')
                ]
        for tpl in stocks:
            print(get_industry(tpl, iexkey))

    def test_pipeline(self):
        with TestPipeline() as p:
            input = (p | 'get Sectors Url' >> beam.Create(['https://www.stockmonitor.com/sectors'])
                        | 'Getting Sectors' >> beam.FlatMap(lambda url: get_all_sectors(url))
                        | 'Get Stocks For Sector' >> beam.Map(lambda tpl: get_stocks_for_sector(tpl))
                        | beam.Map(print))
