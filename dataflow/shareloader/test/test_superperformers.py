
import unittest
from shareloader.modules.superperformers import filter_universe, load_fundamental_data, BenchmarkLoader, \
                                                combine_tickers, benchmark_filter
from shareloader.modules.superperf_metrics import get_all_data, get_descriptive_and_technical, \
                get_financial_ratios, get_fmprep_historical, get_quote_benchmark, \
                get_financial_ratios_benchmark, get_key_metrics_benchmark, get_income_benchmark,\
                get_balancesheet_benchmark


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

    def test_filter_universe(self):
        key = os.environ['FMPREPKEY']

        sample_data1 = get_all_data('TSCO', key)

        sink = Check(equal_to([sample_data1]))

        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create([sample_data1]))
        res = filter_universe(input)

        res | sink

    def test_get_fmprep_historical(self):
        key = os.environ['FMPREPKEY']
        res = get_fmprep_historical('AAPL', key)
        self.assertTrue(res)
        print(res)

    def test_get_descriptive_and_technical(self):
        key = os.environ['FMPREPKEY']
        print(get_descriptive_and_technical('AAPL', key))


    def test_mini_pipeline(self):
        key = os.environ['FMPREPKEY']

        printingSink = beam.Map(print)

        with TestPipeline() as p:
            tickers = (p | 'Starting' >> beam.Create([('TSCO', 'TmpIndustry')]))
            all_data = load_base_data(tickers, key)
            filtered = filter_universe(all_data)
            filtered | printingSink

    def test_get_financial_ratios(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print(get_financial_ratios('AAPL', key))

    def test_get_stock_dividends(self):
        import requests
        from datetime import date, datetime
        key = os.environ['FMPREPKEY']
        divis = requests.get(
            'https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{}?apikey={}'.format(
                'GFI', key)).json()['historical']
        currentDate = date.today()
        hist_date = date(currentDate.year - 20, currentDate.month, currentDate.day)
        all_divis = [(d.get('date'), d.get('adjDividend', 0)) for d in divis if
                     datetime.strptime(d.get('date', date(2000, 1, 1)), '%Y-%m-%d').date() > hist_date]
        from pprint import pprint
        print(len(all_divis))
        pprint(all_divis)

    def test_benchmarkLoader(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print('Key is:{}|'.format(key))
        with TestPipeline() as p:
             (p | 'Starting' >> beam.Create(['AAPL', 'IBM'])
                         | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
                         | 'Running Loader' >> beam.ParDo(BenchmarkLoader(key))
                         | 'Filtering' >> beam.Filter(benchmark_filter)
                         | printingSink
             )





















