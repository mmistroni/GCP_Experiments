from mock import patch, Mock
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from functools import reduce
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from itertools import product
from samples.email_pipeline import run
from hamcrest.core.core.allof import all_of
from apache_beam.transforms import util
from shareloader.modules.monthly_sub_pipelines import run_my_pipeline, map_to_dict, write_data, get_date_ranges,\
            get_historical_data_yahoo,generate_performance, enhance_with_ratings,\
            join_performance_with_edgar, remap_ratings
from shareloader.modules.metrics import compute_data_performance, compute_metrics,\
                get_analyst_recommendations, AnotherLeftJoinerFn, merge_dicts, join_lists
from shareloader.modules.mail_utils import combine_data, TEMPLATE
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import date


import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)



class MyTestOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key', default='mykey')
        parser.add_argument('--iexkey', default='none')

class TestBeamFunctions(unittest.TestCase):

    def test_run_my_pipelinec(self):
        with TestPipeline() as p:
            input = p | beam.Create(['AMZN,Consumer Cyclical'])
            res = run_my_pipeline(input)
            final = (res | 'Map' >> beam.Map(lambda x: x[['Ticker', 'Start_Price', 'End_Price', 'Performance']].to_dict())
                        | 'TODICT' >> beam.Map(lambda d: dict((k, v[0]) for k, v in d.items()))
                        | 'PRINT' >> beam.Map(print))

    def test_map_to_dict(self):
        import pandas as pd
        test_dict = {'Ticker': 'AMZN', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125, 'Performance': 0.046234802447688406}
        dicts = [test_dict]
        df = pd.DataFrame(dicts)
        with TestPipeline() as p:
            input = p | beam.Create([df])
            res = map_to_dict(input)
            assert_that(res, equal_to([{'TICKER': 'AMZN', 'START_PRICE': 3000.1201171875, 'END_PRICE': 3138.830078125,
                                        'PERFORMANCE': 0.046234802447688406, 'RUN_DATE' : date.today().strftime('%Y-%m-%d')}]))

    def test_write_data(self):
        test_dict = {'Ticker': 'AMZN', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125, 'Performance': 0.046234802447688406}
        test_writing_set = [test_dict]
        sink = Check(equal_to(test_writing_set))

        with TestPipeline() as p:
            input = p | beam.Create([test_dict])
            write_data(input, sink)

    def test_historical_data(self):
        df = get_historical_data_yahoo('STRM' ,'foo', *get_date_ranges(20))

        print('Columns are:{}'.format(df.columns))
        print(compute_metrics(df))

    @patch('shareloader.modules.monthly_data.get_analyst_recommendations')
    def test_enhance_with_ratings(self, mock_recomm):
        test_token = 'foo'

        returned_val= {'Ticker': 'AAPL', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125,
                       'Performance': 0.8, 'Ratings': 'BUY'}

        mock_recomm.return_value = returned_val
        test_dicts = [{'Ticker': 'AMZN', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125,
                     'Performance': 0.046234802447688406},
                      {'Ticker': 'AAPL', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125,
                       'Performance': 0.8}]

        with TestPipeline() as p:
            input = p | beam.Create(test_dicts)
            res = enhance_with_ratings(input, test_token)
            assert_that(res, equal_to([returned_val]))

    def test_get_analyst_recomm(self):
        t = dict(Ticker='QDEL')
        print(get_analyst_recommendations(t, '<token>'))


    def test_join_performance_with_edgar(self):

        with TestPipeline() as p:
            pcoll1 = p | 'Create coll1' >> beam.Create(
                [('AMZN', {'ticker': 'AMZN', 'price': 20, 'performance': 20}),
                 ('key2', {'ticker': 'key2', 'price': 2, 'performance': 1}),
                 ('key3', {'ticker': 'key3', 'price': 3, 'performance': 3})
                 ])
            pcoll2 = p | 'Create coll 2' >> beam.Create(
                [('AMZN', {'TOTAL_FILLS': 410}),
                 ('key2', {'TOTAL_FILLS': 4})]
            )
            join_performance_with_edgar(pcoll1, pcoll2)

    @patch('shareloader.modules.metrics.get_analyst_recommendations')
    def test_create_joining_dict(self, mock_recomm):
        test_token = 'foo'

        returned_val = {'Ticker': 'AAPL', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125,
                        'Performance': 0.8, 'Ratings': 'BUY'}

        mock_recomm.return_value = returned_val
        test_dicts = [{'Ticker': 'AMZN', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125,
                       'Performance': 0.046234802447688406},
                      {'Ticker': 'AAPL', 'Start_Price': 3000.1201171875, 'End_Price': 3138.830078125,
                       'Performance': 0.8}]

        with TestPipeline() as p:
            input = p | beam.Create(test_dicts)
            res = enhance_with_ratings(input, test_token)
            ratings_remapped = remap_ratings(res)

            ratings_remapped | 'Print out' >> beam.Map(print)

    def test_send_email(self):
        test_data = [{'RUN_DATE': '2020-10-18', 'TICKER': 'PED', 'START_PRICE': 1.090000033378601,
                      'END_PRICE': 1.8300000429153442, 'PERFORMANCE': 0.6788990705284788, 'RATINGS': 'N/A', 'TOTAL_FILLS': 54},
                     {'RUN_DATE': '2020-10-18', 'TICKER': 'ANOTHER', 'START_PRICE': 1.090000033378601,
                      'END_PRICE': 1.8300000429153442, 'PERFORMANCE': 0.6788990705284788, 'RATINGS': 'N/A',
                      'TOTAL_FILLS': 54},
                     {'RUN_DATE': '2020-10-18', 'TICKER': 'ANOTHER2', 'START_PRICE': 1.090000033378601,
                      'END_PRICE': 1.8300000429153442, 'PERFORMANCE': 0.6788990705284788, 'RATINGS': 'N/A',
                      'TOTAL_FILLS': 54}]

        def test_combine_data(elements):
            return reduce(lambda acc, current: acc + current, elements, '')

        with TestPipeline() as p:
            (p | 'Start' >> beam.Create(test_data)
               | 'Map to Template' >> beam.Map(lambda row: TEMPLATE.format(**row))
               | 'Combine' >> beam.CombineGlobally(test_combine_data)
               | 'Print out' >> beam.Map(print))

    def test_combine_globally(self):
        test_data = [{'RUN_DATE': '2020-10-18', 'TICKER': 'PED', 'START_PRICE': 1.090000033378601,
                      'END_PRICE': 1.8300000429153442, 'PERFORMANCE': 0.6788990705284788, 'RATINGS': 'N/A',
                      'TOTAL_FILLS': 54},
                     {'RUN_DATE': '2020-10-18', 'TICKER': 'ANOTHER', 'START_PRICE': 1.090000033378601,
                      'END_PRICE': 1.8300000429153442, 'PERFORMANCE': 0.6788990705284788, 'RATINGS': 'N/A',
                      'TOTAL_FILLS': 54},
                     {'RUN_DATE': '2020-10-18', 'TICKER': 'ANOTHER2', 'START_PRICE': 1.090000033378601,
                      'END_PRICE': 1.8300000429153442, 'PERFORMANCE': 0.6788990705284788, 'RATINGS': 'N/A',
                      'TOTAL_FILLS': 54}]

        res = reduce(lambda acc, current: acc + TEMPLATE.format(**current), test_data, '')
        print(res)
        #print(combine_data(test_data))


