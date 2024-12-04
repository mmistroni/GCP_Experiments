
import unittest
from shareloader.modules.stock_picks import  map_to_bq_dict, run_my_pipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from unittest.mock import patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
import argparse
from apache_beam.options.pipeline_options import PipelineOptions

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)



class TestEdgarUtils(unittest.TestCase):

    def setUp(self):
        self.notEmptySink = Check(is_not_empty())
        self.notEmptySink = Check(is_not_empty())
        self.printSink = beam.Map(print)
        parser = argparse.ArgumentParser(add_help=False)

    

    def test_generate_initial_feeds(self):
        pass
        #print(generate_initial_feeds(as_of_date=date(2021,3,3)))

    def test_map_to_bq_dict(self):
        test_elemns = [('2021-03-03', 'AMZN', 'is facebook a buy today in the face negative news flow?', 'BUY', 'https://seekingalpha.com/article/4411109-is-fb-stock-a-buy-today-negative-news-flow?utm_source=feed_articles_stock_ideas_editors_picks&utm_medium=referral'),
                       ]
        res = map_to_bq_dict(test_elemns[0])

        assert res['AS_OF_DATE'] == test_elemns[0][0]
        assert res['TICKER'] == test_elemns[0][1]
        assert res['HEADLINE'] == test_elemns[0][2]
        assert res['ACTION'] ==  test_elemns[0][3]
        assert res['LINK'] == test_elemns[0][4]

    def test_pips(self):
        debugSink = beam.Map(print)

        with TestPipeline() as p:
            (p | 'start' >> beam.Create(['foo'])
             | 'out' >> debugSink
             )



    def test_word_count(self):
        
        debugSink = beam.Map(print)
        with TestPipeline() as p:
            (p | 'Start' >> beam.Create(['hello world', 'hello beam'])
                | 'One' >>beam.Map(lambda x: x.upper())
                | 'Three' >> debugSink
            )
            






