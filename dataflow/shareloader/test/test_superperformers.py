
import unittest
from shareloader.modules.superperformers import filter_universe, load_fundamental_data, BenchmarkLoader, \
                                                combine_tickers, benchmark_filter, FundamentalLoader,\
                                                asset_play_filter, defensive_stocks_filter, map_to_bq_dict,\
                                                get_universe_filter
from shareloader.modules.superperf_metrics import get_all_data, get_descriptive_and_technical, \
                get_financial_ratios, get_fmprep_historical, get_quote_benchmark, \
                get_financial_ratios_benchmark, get_key_metrics_benchmark, get_income_benchmark,\
                get_balancesheet_benchmark, compute_cagr, calculate_piotrosky_score


import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
import os
import requests

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


def filter_basic_fields(input_dict):
      keys = ['price', 'yearHigh', 'yearLow', 'priceAvg50', 'priceAvg200', 'marketCap',
              'bookValuePerShare', 'tangibleBookValuePerShare']
      s = [str(input_dict[k]) for k in keys]
      return ','.join(s)


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
             (p | 'Starting' >> beam.Create(['TX'])
                         | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
                         | 'Running Loader' >> beam.ParDo(BenchmarkLoader(key))
                         | 'Filtering' >> beam.Filter(benchmark_filter)
                         | 'Filtering for defensive' >> beam.Filter(defensive_stocks_filter)
                        #| 'Mapper' >> beam.Map(lambda d: map_to_bq_dict(d, 'TESTER'))
                         #| 'Mapping to our functin' >> beam.Map(filter_basic_fields)

              | printingSink
             )

    def test_fundamentalLoader(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print('Key is:{}|'.format(key))
        with TestPipeline() as p:
             (p | 'Starting' >> beam.Create(['AAPL', 'IBM'])
                         | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
                         | 'Running Loader' >> beam.ParDo(FundamentalLoader(key))
                         #| 'Mapper' >> beam.Map(lambda d: map_to_bq_dict(d, 'TESTER'))
                         #| 'Mapping to our functin' >> beam.Map(filter_basic_fields)
                         #| 'Filtering' >> beam.Filter(asset_play_filter)
                         | printingSink
             )


    # Need tow rite test also for fundamental


    def test_get_financial_ratios_benchmark(self):
        import pandas as pd
        key = os.environ['FMPREPKEY']

        f = open('C:\\Users\Marco And Sofia\\GitHubProjects\\GCP_Experiments\\dataflow\\shareloader\\test\\test.csv', 'r')
        mapped = map(lambda i: i.split(',')[0], f.readlines())
        counter = 0
        for ticker in mapped:
            res = get_financial_ratios_benchmark(ticker, key)
            if res:
                print(res)
                counter +=1

            if counter > 50:
                break


    def test_fundamentalLoaderForAssetPLay(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print('Key is:{}|'.format(key))
        with TestPipeline() as p:
             (p | 'Starting' >> beam.Create(['IRT', 'SCU', 'PARA'])
                         | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
                         | 'Running Loader' >> beam.ParDo(FundamentalLoader(key))
                         | 'Filtering on stock universe' >> beam.Filter(get_universe_filter)
                         | 'Filtering' >> beam.Filter(asset_play_filter)
                         | 'Mapping'>> beam.Map(lambda d: dict(avps=d.get('sharesOutstanding', 0) * d.get('bookValuePerShare'),
                                                                    ticker=d['symbol'], marketCap=d['marketCap']))
                         | printingSink
             )

    def test_defensiveAndEnterpriseStocks(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print('Key is:{}|'.format(key))
        with TestPipeline() as p:
             (p | 'Starting' >> beam.Create(['TX'])
                         | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
                         | 'Running Loader' >> beam.ParDo(BenchmarkLoader(key))
                         | 'Filtering' >> beam.Filter(benchmark_filter)
                         | 'Filtering for defensive' >> beam.Filter(defensive_stocks_filter)
                         | 'Printing out' >> beam.Map(print)
                        #| 'Mapper' >> beam.Map(lambda d: map_to_bq_dict(d, 'TESTER'))
                         #| 'Mapping to our functin' >> beam.Map(filter_basic_fields)

              | printingSink
             )

    def test_compute_cagr(self):
        inputs = [1299.8, 1411.3, 1872.9, 3080, 3777]

        from pprint import pprint
        pprint(compute_cagr(inputs))

    def test_piotrosky_scorer(self):
        key = os.environ['FMPREPKEY']
        for ticker in ['MSFT', 'MO', 'NKE', 'NXPI']:
            print(f'{ticker}={calculate_piotrosky_score(key, ticker)}')

    def computeRSI(self, data, time_window):
        diff = data.diff(1).dropna()  # diff in one field(one day)

        # this preservers dimensions off diff values
        up_chg = 0 * diff
        down_chg = 0 * diff

        # up change is equal to the positive difference, otherwise equal to zero
        up_chg[diff > 0] = diff[diff > 0]

        # down change is equal to negative deifference, otherwise equal to zero
        down_chg[diff < 0] = diff[diff < 0]

        # check pandas documentation for ewm
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.ewm.html
        # values are related to exponential decay
        # we set com=time_window-1 so we get decay alpha=1/time_window
        up_chg_avg = up_chg.ewm(com=time_window - 1, min_periods=time_window).mean()
        down_chg_avg = down_chg.ewm(com=time_window - 1, min_periods=time_window).mean()

        rs = abs(up_chg_avg / down_chg_avg)
        rsi = 100 - 100 / (1 + rs)
        return rsi

    def test_compute_rsi(self):
        import pandas as pd
        key = os.environ['FMPREPKEY']
        url = f'https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?from=2022-01-01&to=2022-07-15&apikey={key}'

        historical = requests.get(url).json().get('historical')
        data = pd.DataFrame(data=historical[::-1])
        data['asOfDate'] = pd.to_datetime(data['date'])
        data['RSI'] = self.computeRSI(data['adjClose'], 20)

        print(f'Rsi: {data.tail(1).RSI.values[0]}')



















