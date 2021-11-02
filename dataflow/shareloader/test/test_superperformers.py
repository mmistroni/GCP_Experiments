
import unittest
from shareloader.modules.superperformers import load_base_data, filter_universe, load_fundamental_data
from shareloader.modules.superperf_metrics import get_all_data, get_descriptive_and_technical, \
                get_financial_ratios

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
import os

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class PercentagesFn(beam.CombineFn):
  def create_accumulator(self):
    return {}

  def add_input(self, accumulator, input):
    # accumulator == {}
    # input == '🥕'
    print('Input is:{}'.format(input))
    accumulator[input['symbol']] = input  # {'🥕': 1}
    print(accumulator)
    return accumulator

  def merge_accumulators(self, accumulators):
    # accumulators == [
    #     {'🥕': 1, '🍅': 2},
    #     {'🥕': 1, '🍅': 1, '🍆': 1},
    #     {'🥕': 1, '🍅': 3},
    # ]
    merged = {}
    for accum in accumulators:
      for item, vals in accum.items():
        merged[item] = vals
    return merged

  def extract_output(self, accumulator):
      # accumulator == {'🥕': 3, '🍅': 6, '🍆': 1}
      print(list(accumulator.values()))
      return list(accumulator.values())




class TestSuperPerformers(unittest.TestCase):



    def all_in_one(self, input):
        from functools import reduce

        dt = [d for d in input]
        print(' dt is:{}'.format(dt))
        return dt

    def test_load_base_data(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            def tickerCombiners(input):
                return ','.join(input)

            input = (p | 'Start' >> beam.Create([('TSCO', 'Something'), ('MCD', 'Something'),
                                                 ('MSFT', 'Something'), ('KL', 'Something')])
                       | 'Extracting only ticker and Industry' >> beam.Map(lambda item: (item[0]))
                     )
            res = load_base_data(input, key)

            res | 'Printing out' >> beam.Map(print)

    def test_getfundamentals(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            def tickerCombiners(input):
                return ','.join(input)

            input = (p | 'Start' >> beam.Create([('TSCO', 'Something'), ('MCD', 'Something'),
                                                 ('MSFT', 'Something')])
                       | 'Extracting only ticker and Industry' >> beam.Map(lambda item: (item[0]))
                     )
            # new strategy. Get tickers,load fundamentals and combine
            descriptive = load_base_data(input, key)

            fundamentals =  load_fundamental_data(input, key) | 'Printing out' >> beam.Map(print)



    def test_filter_universe(self):
        key = os.environ['FMPREPKEY']

        sample_data1 = get_all_data('TSCO', key)

        sink = Check(equal_to([sample_data1]))

        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create([sample_data1]))
        res = filter_universe(input)

        res | sink

    def test_mini_pipeline(self):
        key = os.environ['FMPREPKEY']

        printingSink = beam.Map(print)

        with TestPipeline() as p:
            tickers = (p | 'Starting' >> beam.Create([('TSCO', 'TmpIndustry')]))
            all_data = load_base_data(tickers, key)
            filtered = filter_universe(all_data)
            filtered | printingSink

    def test_getalldata(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print('Key is:{}|'.format(key))
        with TestPipeline() as p:
            tickers = (p | 'Starting' >> beam.Create([('TSCO', 'TmpIndustry'), ('ARR', 'foo'),
                                                      ('ADAP', 'foo'), ('ATGE', 'foo')])
                         | 'Map ' >> beam.Map(lambda i: i[0])
                         | 'Filter out Nones' >> beam.Filter(lambda item: item is not None)
                       )
            all_data = load_base_data(tickers, key)
            all_data  | printingSink


    def test_get_financial_ratios(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print(get_financial_ratios('AAPL', key))
















