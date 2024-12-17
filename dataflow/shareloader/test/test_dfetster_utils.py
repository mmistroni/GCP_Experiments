
import unittest
import argparse
from shareloader.modules.dftester_utils import DfTesterLoader, get_tickers_for_sectors,\
                                                get_tickers_for_industry,get_industries
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import os
from unittest.mock import patch
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import date
from shareloader.modules.obb_utils import ProcessHistorical
from shareloader.modules.finviz_utils import get_extra_watchlist
from shareloader.modules.launcher import run_obb_pipeline, run_premarket_pipeline, run_etoro_pipeline

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)

class AnotherLeftJoinerFn(beam.DoFn):

    def __init__(self):
        super(AnotherLeftJoinerFn, self).__init__()

    def process(self, row, **kwargs):

        right_dict = dict(kwargs['right_list'])
        left_key = row[0]
        left = row[1]
        print('Left is of tpe:{}'.format(type(left)))
        if left_key in right_dict:
            print('Row is:{}'.format(row))
            right = right_dict[left_key]
            left.update(right)
            yield (left_key, left)



class TestDfTesterLoader(unittest.TestCase):

    def setUp(self) -> None:
        self.notEmptySink = Check(is_not_empty())
        self.debugSink = beam.Map(print)
        #self.patcher = patch('shareloader.modules.share_datset_loader.XyzOptions._add_argparse_args')
        #self.mock_foo = self.patcher.start()
        parser = argparse.ArgumentParser(add_help=False)

    def tearDown(self):
        pass
        #self.patcher.stop()

    # https://beam.apache.org/documentation/pipelines/test-your-pipeline/
    def test_run_pipeline(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                       | self.notEmptySink
                     )

    def test_get_tickers_for_sector(self):
        key = os.environ['FMPREPKEY']

        sectors = ['Consumer Cyclical', 'Energy', 'Technology', 'Industrials',
                   'Financial Services', 'Basic Materials', 'Communication Services',
                   'Consumer Defensive', 'Healthcare', 'Real Estate', 'Utilities',
                   'Industrial Goods', 'Financial', 'Services', 'Conglomerates']

        for s in sectors[0:1]:
            ticks = get_tickers_for_sectors(s, key)
            self.assertTrue(len(ticks)> 0)

    def test_get_tickers_for_industry(self):
        key = os.environ['FMPREPKEY']

        industries = ['Consumer Cyclical', 'Energy', 'Technology', 'Industrials',
                   'Financial Services', 'Basic Materials', 'Communication Services',
                   'Consumer Defensive', 'Healthcare', 'Real Estate', 'Utilities',
                   'Industrial Goods', 'Financial', 'Services', 'Conglomerates']

        for s in industries[0:1]:
            ticks = get_tickers_for_sectors(s, key)
            self.assertTrue(len(ticks)> 0)

    def test_all_industries(self):
        key = os.environ['FMPREPKEY']

        data = get_industries(key)

        self.assertTrue(data)
    def test_obb_pipeline(self):
        pat = os.environ['OBB_PAT_KEY']
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                     | self.debugSink
                     )
    def test_launcher(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline(options=PipelineOptions()) as p:
            input = run_obb_pipeline(p, key)
            input | self.debugSink

    def test_premarket_pipeline(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline(options=PipelineOptions()) as p:
            input = run_premarket_pipeline(p, key)
            input2 = run_etoro_pipeline(p)
            res = ( (input, input2) |  "fmaprun" >> beam.Flatten()
                    | 'tosink' >> self.debugSink)

    def test_etoro(self):
        from shareloader.modules.obb_utils import AsyncProcess
        key = os.environ['FMPREPKEY']
        from datetime import date
        cob = date.today()
        with TestPipeline(options=PipelineOptions()) as p:
            ''' sample ppln'''
            cob = date.today()
            test_ppln = get_extra_watchlist()
            input2 =  (test_ppln
                    | 'Maping extra ticker' >> beam.Map(lambda d: d['Ticker'])
                    | 'Filtering extra' >> beam.Filter(
                        lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                    | 'Combine all extratickers' >> beam.CombineGlobally(lambda x: ','.join(x))
                    | 'Etoro' >> beam.ParDo(AsyncProcess({}, cob, price_change=0.0001, selection='EToro'))
                    )

            tech_indicators = (
                    input2 | 'Map To Tick' >> beam.Map(lambda d: d['ticker'])
                    | 'combinea ll ' >> beam.CombineGlobally(lambda x: ','.join(x))
                    | 'run rpocess'  >> beam.ParDo(ProcessHistorical(key, cob))
            )

            mapped = (input2 |'remapping ' >> beam.Map(lambda d: (d['ticker'], d))
                      )

            left_joined = (
                    mapped
                    | 'InnerJoiner: JoinValues' >> beam.ParDo(AnotherLeftJoinerFn(),
                                                right_list=tech_indicators)
                    | 'Display' >> beam.Map(print)
            )














