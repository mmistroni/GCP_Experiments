
import unittest
import requests
from lxml import etree
from io import StringIO, BytesIO
from shareloader.mypackage.obb_utils import OBBLoader
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date, datetime
from unittest.mock import patch
#from shareloader.modules.finviz_utils import get_leaps

import os
from shareloader.modules.finviz_utils import get_universe_stocks
from shareloader.mypackage.launcher import run_obb_pipeline

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestEdgarUtils(unittest.TestCase):

    def setUp(self):
        self.notEmptySink = Check(is_not_empty())
        self.patcher = patch('shareloader.modules.sector_loader.XyzOptions._add_argparse_args')
        self.mock_foo = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()


    def test_generate_initial_feeds(self):
        pass
        #print(generate_initial_feeds(as_of_date=date(2021,3,3)))

    def test_run_my_pipeline(self):
        sink = beam.Map(print)
        key = os.environ['FMPREPKEY']

        with TestPipeline() as p:
            (p
             | 'Start' >> beam.Create([{'Ticker' : 'AAPL'}])
             | 'Mapping ticks' >> beam.Map(lambda d: d['Ticker'])
             | 'combining' >> beam.CombineGlobally(lambda x: ','.join(x))
             | 'Get all List' >> beam.ParDo(OBBLoader(key))
             |  sink
             )

    def test_run_obb_pipeline(self):
        sink = beam.Map(print)
        key = os.environ['FMPREPKEY']

        with TestPipeline() as p:
            res = run_obb_pipeline(p, key)

            res |  sink


    def test_cramer_inverse(self):
        from bs4 import BeautifulSoup
        from shareloader.modules.news_util import get_user_agent

        baseUrl = 'https://www.quiverquant.com/cramertracker/'

        req = requests.get(baseUrl, headers={'User-Agent': get_user_agent()})
        soup = BeautifulSoup(req.text, "html.parser")

        table = soup.find_all('div', {"class": "holdings-table table-inner"})[0]

        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if not tds:
                continue
            else:
                ticker  = tds[0].text
                direction = tds[1].text
                cob = datetime.strptime(tds[2].text, '%B %d, %Y').date()

                if (date.today() - cob).days > 3:
                    continue

                print(f'Ticker:{ticker}|Direction:{direction}|Date:{cob}')




