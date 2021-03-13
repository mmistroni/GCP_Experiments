from mock import patch, Mock
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from shareloader.modules.news import run_my_pipeline, XyzOptions, \
                                            prepare_for_big_query
from datetime import date
from shareloader.modules.kor_isr_shareloader import find_diff, create_us_and_foreign_dict, \
                            map_ticker_to_html_string, combine_to_html_rows, run_my_pipeline
from functools import reduce
import unittest
from shareloader.modules.news_util import enhance_with_price
import os

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
        with TestPipeline() as p:
            input = (p |  beam.Create(create_us_and_foreign_dict(os.environ['IEXAPI_KEY']))
                       |  beam.Map(print))

    def test_remap(self):
        options = Mock(name='testoptions')
        options.iexapikey = os.environ['IEXAPI_KEY']
        start_list = create_us_and_foreign_dict(options.iexapikey)
        with beam.Pipeline() as p:
            result = (p | 'Start' >> beam.Create(start_list)
                      | 'Getting Prices' >> beam.Map(
                        lambda tpl: (tpl[0], tpl[1], tpl[2], find_diff(tpl[2], date.today())))
                      | 'Filtering Increases' >> beam.Filter(lambda tpl: tpl[3] > 0.0)
                      | 'Map to HTML Table' >> beam.Map(lambda t: map_ticker_to_html_string(t))
                      | 'Combine to one Text' >> beam.CombineGlobally(combine_to_html_rows)
                      | beam.Map(print)
                      )
            print(result)
            

        with TestPipeline() as p:
            options = Mock(name='testoptions')
            options.iexapikey = os.environ['IEXAPI_KEY']
            run_my_pipeline(p, options)


    def test_map_ticker_to_html_string(self):
        tpls = [('AMZN', 'AMZN1', 'AMNZ2', '.5'),
                ('AAPL', 'AAPL1', 'AAPL2', '.9')]
        print(list(map_ticker_to_html_string(tpls)))

    def test_map_ticker_to_html_string(self):
        test_elems = [dict(ticker='AAPL')]
        iexkey = os.environ['IEXAPI_KEY']
        with TestPipeline() as p:
            input = (p | beam.Create(test_elems)
                     | 'EnhancedPrices' >> beam.Map(lambda d: enhance_with_price(d,iexkey=iexkey))
                     | 'Out' >> beam.Map(print))





    def test_combine_to_html_rows(self):
        test_elems = ['<tr><td>AMZN</td><td>AMZN1</td><td>AMNZ2</td><td>.5</td></tr>', '<tr><td>AAPL</td><td>AAPL1</td><td>AAPL2</td><td>.9</td></tr>']
        with TestPipeline() as p:
            input = (p | beam.Create(test_elems)
                     | 'Combining..' >> beam.CombineGlobally(combine_to_html_rows)
                     | 'Out' >> beam.Map(print))





