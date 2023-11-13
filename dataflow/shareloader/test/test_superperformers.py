import math
import unittest
import numpy as np
from shareloader.modules.superperformers import filter_universe, load_fundamental_data, BenchmarkLoader, \
                                                combine_tickers, benchmark_filter, FundamentalLoader,\
                                                asset_play_filter, defensive_stocks_filter, map_to_bq_dict,\
                                                get_universe_filter, get_defensive_filter_df,\
                                                get_enterprise_filter_df, load_bennchmark_data, \
                                                load_microcap_data, microcap_filter, microcap_sanity_check
from shareloader.modules.superperf_metrics import get_all_data, get_descriptive_and_technical, \
                get_financial_ratios, get_fmprep_historical, get_quote_benchmark, \
                get_financial_ratios_benchmark, get_key_metrics_benchmark, get_income_benchmark,\
                get_balancesheet_benchmark, compute_cagr, calculate_piotrosky_score, \
                get_institutional_holders_quote, filter_historical, get_latest_stock_news,\
                get_mm_trend_template, get_fundamental_parameters

from itertools import chain
from pandas.tseries.offsets import BDay
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
import os
import requests
import pandas as pd
from collections import OrderedDict
from datetime import date, datetime
import logging

def generate_date_headers():
    today = date.today()
    all_dates = [(today - BDay(idx)).month for idx in range(1, 90)]
    sorted_months = sorted(all_dates, key=lambda x: x)
    sorted_set = OrderedDict.fromkeys(sorted_months).keys()
    return [date(today.year, month, 1).strftime('%b %y') for month in sorted_set][1:]




def _fetch_performance(sector, ticker, key):
    endDate = date(2022,8,28)#.today()
    startDate = (endDate - BDay(90)).date()
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={startDate.strftime('%Y-%m-%d')}&to={endDate.strftime('%Y-%m-%d')}&apikey={key}"
    historical = requests.get(url).json().get('historical')
    df = pd.DataFrame(data=historical[::-1])
    df['date'] = pd.to_datetime(df.date)
    df['ticker'] = ticker
    df = df.set_index('date')
    resampled = df.resample('1M').mean()
    resampled[sector] = resampled.close / resampled.close.shift(1) - 1
    records = resampled[[sector]].dropna().T.to_dict('records')

    data = []
    for k, v in records[0].items():
        data.append((k.strftime('%Y-%m-%d'), v))

    return (sector, data)


def extract_test_data_pipeline(p,test_tickers):
    return (p
            | 'Reading Tickers' >> beam.Create(test_tickers)
            | 'Converting to Tuple' >> beam.Map(lambda row: row.split(','))
            | 'Extracting only ticker and Industry' >> beam.Map(lambda item: (item[0]))

            )


class ETFHistoryCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    logging.info('Adding{}'.format(input))
    logging.info('acc is:{}'.format(accumulator))
    accumulator.append(input)
    return accumulator

  def merge_accumulators(self, accumulators):

    return list(chain(*accumulators))

  def extract_output(self, sum_count):
    return sum_count


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)

class EmailSender(beam.DoFn):
    def __init__(self, recipients, key):
        self.recipients = recipients.split(',')
        self.key = key

    def _build_html_message(self, rows):
        html = '<table border="1">'
        header_row = "<tr><th>Sector</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr>"
        row_template = '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'

        headers = rows[0][1]
        dates = [tpl[0] for tpl in headers]
        header_row = header_row.format(*dates)
        html += header_row

        for sector, dates in rows:
            returns = ['%.3f'% val[1] for val in dates]
            sector_data = [sector] + returns
            html += row_template.format(*sector_data)
        html += '</table>'
        return html



    def process(self, element):
        sector_returns = element
        logging.info('Processing returns')
        data = self._build_html_message(element)
        template = \
            "<html><body>{}</body></html>".format(data)
        return [template]


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

    def setUp(self):
        self.notEmptySink = Check(is_not_empty())



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
        res = get_fmprep_historical('AAPL', key, numdays=40,  colname=[])
        self.assertTrue(res)


    def test_get_descriptive_and_technical(self):
        key = os.environ['FMPREPKEY']
        res = get_descriptive_and_technical('AAPL', key)

        expectedKeys = ['marketCap', 'price', 'avgVolume', 'priceAvg50', 'priceAvg200', 'eps', 'pe', 'sharesOutstanding',
            'yearHigh', 'yearLow', 'exchange', 'change', 'open', 'symbol', 'volume', 'previousClose',
            'priceAvg20', 'changeFromOpen', 'allTimeHigh', 'allTimeLow', 'institutionalOwnershipPercentage']

        checker = [k in res.keys() for k in expectedKeys]
        self.assertTrue(all(checker))

    def test_get_financial_ratios(self):
        key = os.environ['FMPREPKEY']
        # dividend yield here
        res = get_financial_ratios('AAPL', key)
        self.assertTrue(res)

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
        self.assertTrue(all_divis)

    def test_get_peterlynch_ratios(self):
        key = os.environ['FMPREPKEY']

        # pe  / we have it in financial ratios
        # dividendYield we have it in fnancial ratios
        # epsGrowth  added.

        # financial ratios for bench has div Yield  + peRatio (pe ratio not in percentage)
        bench = get_financial_ratios_benchmark('AAPL', key)

        # div yield
        standard = get_financial_ratios('AAPL', key)

        ibench = get_income_benchmark('AAPL', key)

        # div yield
        istandard = get_fundamental_parameters('AAPL', key)



        self.assertIsNotNone(bench)
        self.assertIsNotNone(standard)




    def test_benchmarkLoader(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print('Key is:{}|'.format(key))
        with TestPipeline() as p:
             (p | 'Starting' >> beam.Create(['AMZN'])
                         | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
                         | 'Running Loader' >> beam.ParDo(BenchmarkLoader(key))
                         | 'Filtering' >> beam.Filter(benchmark_filter)
                         #| 'Filtering for defensive' >> beam.Filter(defensive_stocks_filter)
                        #| 'Mapper' >> beam.Map(lambda d: map_to_bq_dict(d, 'TESTER'))
                         #| 'Mapping to our functin' >> beam.Map(filter_basic_fields)

              | printingSink
             )

    def test_fundamentalLoader(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print('Key is:{}|'.format(key))
        with TestPipeline() as p:
             (p | 'Starting' >> beam.Create(['AMZN'])
                         | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
                         | 'Running Loader' >> beam.ParDo(FundamentalLoader(key))
                         #| 'Mapper' >> beam.Map(lambda d: map_to_bq_dict(d, 'TESTER'))
                         #| 'Mapping to our functin' >> beam.Map(filter_basic_fields)
                         #| 'Filtering' >> beam.Filter(asset_play_filter)
                         | printingSink
             )

    def test_load_fundamental_data(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print('Key is:{}|'.format(key))
        with TestPipeline() as p:
            ticks = (p | 'Starting' >> beam.Create(['AAPL'])
               | 'tstCombine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
                   )

            res = load_fundamental_data(ticks, key)

            res | self.notEmptySink

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
                         | self.notEmptySink
             )

    def test_defensiveAndEnterpriseStocks(self):
        key = os.environ['FMPREPKEY']
        printingSink = beam.Map(print)

        print('Key is:{}|'.format(key))
        with TestPipeline() as p:
             (p | 'Starting' >> beam.Create(['WOR'])
                         | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
                         | 'Running Loader' >> beam.ParDo(BenchmarkLoader(key))
                         | 'Filtering' >> beam.Filter(benchmark_filter)
                         | 'Filtering for defensive' >> beam.Filter(defensive_stocks_filter)
                         #| 'Printing out' >> beam.Map(print)
                         | 'Mapper' >> beam.Map(lambda d: map_to_bq_dict(d, 'TESTER'))
                         #| 'Mapping to our functin' >> beam.Map(filter_basic_fields)

              | self.notEmptySink
             )

    def test_compute_cagr(self):
        inputs = [1299.8, 1411.3, 1872.9, 3080, 3777]

        from pprint import pprint
        self.assertTrue(compute_cagr(inputs) != 0)

    def test_piotrosky_scorer(self):
        key = os.environ['FMPREPKEY']
        for ticker in ['MSFT', 'MO', 'NKE', 'NXPI']:
            res = calculate_piotrosky_score(key, ticker)
            self.assertTrue(res != 0)


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

        self.assertTrue(data.tail(1).RSI.values[0])

    def test_skew(self):
        key = os.environ['FMPREPKEY']
        base_url = 'https://financialmodelingprep.com/api/v3/quote-short/{}?apikey={}'.format('^VIX', key)
        self.assertTrue(requests.get(base_url).json())


    def test_vix_cftc(self):
        key = os.environ['FMPREPKEY']
        base_url = f'https://financialmodelingprep.com/api/v4/commitment_of_traders_report_analysis/VI?apikey={key}'
        self.assertTrue(requests.get(base_url).json()[0])

    def test_defensive_filter_df(self):
        key = os.environ['FMPREPKEY']
        # Need to find currentRatio, dividendPaid, peRatio, priceToBookRatio
        for ticker in ['TX']:
            print(f'------------{ticker}----------------')
            bmarkData = load_bennchmark_data(ticker, key)
            self.assertIsNotNone(bmarkData['netIncome'])
            self.assertIsNotNone(bmarkData['rsi'])
            bmarkData['stockBuyPrice'] = bmarkData['priceAvg200'] *.8
            bmarkData['stockSellPrice'] = bmarkData['priceAvg200'] * .7
            bmarkData['ACTION'] = 'BUY' if bmarkData['price'] <=  bmarkData['stockBuyPrice'] else ''
            bmark_df = pd.DataFrame(list(bmarkData.items()), columns=['key', 'value'])
            defensive_df = get_defensive_filter_df()
            merged = pd.merge(bmark_df, defensive_df, on='key', how='left')
            self.assertTrue(merged.shape[0] > 0)
            with pd.option_context('display.max_rows', None,
                                   'display.max_columns', 5,
                                   'display.precision', 3,
                                   ):
                print(merged.to_string(index=False))

    def get_universe_filter_df(self):

        filters =  {'marketCap' : 'marketCap  > 300000000',
            'avgVolume' :'avgVolume > 200000' ,
            'price' :  'price > 10',
            'eps_growth_this_year': 'eps_growth_this_year>  0.2',
            'grossProfitMargin'  :'grossProfitMargin > 0' ,
             'priceAvg20' : 'price > priceAvg20',
             'priceAvg50' : 'price > priceAvg50',
             'priceAvg200': 'price > priceAvg200',
             'net_sales_qtr_over_qtr'  : 'net_sales_qtr_over_qtr > 0.2',
             'returnOnEquity'  : 'returnOnEquity> 0',
             'eps_growth_next_year' : 'eps_growth_next_year > 0',
             'eps_growth_qtr_over_qtr' : 'eps_growth_qtr_over_qtr > 0.2'
            }
        return pd.DataFrame(list(filters.items()), columns=['key', 'function'])

    def get_canslim_filter_df(self):
        filters =  {
            'avgVolume' :'avgVolume > 200000' ,
            'eps_growth_this_year': 'eps_growth_this_year>  0.2',
            'eps_growth_next_year': 'eps_growth_next_year>  0.2',
            'eps_growth_qtr_over_qtr': 'eps_growth_qtr_over_qtr > 0.2',
            'net_sales_qtr_over_qtr': 'net_sales_qtr_over_qtr > 0.2',
            'eps_growth_past_5yrs': 'eps_growth_past_5yrs > 0.2',
            'returnOnEquity'  : 'returnOnEquity > 0' ,
            'grossProfitMargin': 'grossProfitMargin > 0',
            'institutionalOwnershipPercentage': 'institutionalOwnershipPercentage > 0.3',
             'price'      : 'price > price20/50/200',
             'priceAvg20' : 'price > priceAvg20',
             'priceAvg50' : 'price > priceAvg50',
             'priceAvg200': 'price > priceAvg200',
             'sharesOutstanding'  : 'sharesOutstanding > 50000000'
            }
        return pd.DataFrame(list(filters.items()), columns=['key', 'function'])

    def test_enterprisee_filter_df(self):
        key = os.environ['FMPREPKEY']
        bmarkData = load_bennchmark_data('NOAH', key)

        #self.assertIsNotNone(bmarkData['netIncome'])
        #self.assertIsNotNone(bmarkData['rsi'])


        bmarkData['stock_buy_price'] = bmarkData['priceAvg200'] * .8
        bmarkData['stock_sell_price'] = bmarkData['priceAvg200'] * .7
        bmarkData['earningYield'] = bmarkData.get('netIncome',0) / bmarkData['marketCap']
        bmark_df = pd.DataFrame(list(bmarkData.items()), columns=['key', 'value'])

        enterprise_df = get_enterprise_filter_df()
        merged = pd.merge(bmark_df, enterprise_df, on='key', how='left')


        self.assertTrue(merged.shape[0] > 0)

        with pd.option_context('display.max_rows', None,
                               'display.max_columns', 5,
                               'display.precision', 3,
                               ):
            print(merged.to_string(index=False))


    def test_stock_universe__filter_df(self):
        from shareloader.modules.superperf_metrics import get_fundamental_parameters, get_fundamental_parameters_qtr, \
            get_financial_ratios, get_analyst_estimates, get_asset_play_parameters, \
            compute_rsi, get_price_change, get_income_benchmark, \
            get_balancesheet_benchmark, compute_cagr, calculate_piotrosky_score

        ## TODO
        key = os.environ['FMPREPKEY']
        ticker = 'TX'
        fundamental_data = get_fundamental_parameters(ticker, key)
        fundamental_qtr = get_fundamental_parameters_qtr(ticker, key)
        fundamental_data.update(fundamental_qtr)
        financial_ratios = get_financial_ratios(ticker, key)
        fundamental_data.update(financial_ratios)

        updated_dict = get_analyst_estimates(ticker, key, fundamental_data)
        descr_and_tech = get_descriptive_and_technical(ticker, key)
        updated_dict.update(descr_and_tech)
        asset_play_dict = get_asset_play_parameters(ticker, key)
        updated_dict.update(asset_play_dict)

        piotrosky_score = calculate_piotrosky_score(key, ticker)
        latest_rsi = compute_rsi(ticker, key)
        updated_dict['piotroskyScore'] = piotrosky_score
        updated_dict['rsi'] = latest_rsi

        priceChangeDict = get_price_change(ticker, key)
        updated_dict.update(priceChangeDict)
        updated_dict['stock_buy_price'] = updated_dict['priceAvg200'] * .8
        updated_dict['stock_sell_price'] = updated_dict['priceAvg200'] * .7
        updated_dict['earningYield'] = updated_dict.get('netIncome',0) / updated_dict['marketCap']
        updated_dict_df = pd.DataFrame(list(updated_dict.items()), columns=['key', 'value'])

        universe_filter_df = self.get_universe_filter_df()
        merged = pd.merge(universe_filter_df, updated_dict_df, on='key', how='left')

        self.assertTrue(merged.shape[0] > 0)



        with pd.option_context('display.max_rows', None,
                               'display.max_columns', 5,
                               'display.precision', 3,
                               ):
            print(merged.to_string(index=False))

    ## Add a test so that we can run all selection criteria against a stock and see why it did not get selected

    def test_microcap_filter_df(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            tickers = extract_test_data_pipeline(p, ['META', 'AAPL', 'FB'])

            microcap = load_microcap_data(tickers, key)

            microcap | self.notEmptySink
            '''
            (tickers
             | 'Combine all at fundamentals microcap' >> beam.CombineGlobally(combine_tickers)
             | 'Getting fundamentals microcap' >> beam.ParDo(FundamentalLoader(key))
             | 'MicroCap Sanity Check' >> beam.Filter(microcap_sanity_check)
             | 'wRITING TO SINK microcap' >> beam.Map(print)
             )
            '''


    def test_filter_historical(self):
        from datetime import date, datetime
        from collections import defaultdict
        testData = [ {  "holder" : "FIL LTD",
                      "shares" : 4336424,
                      "dateReported" : "2022-09-30",
                      "change" : -12387
                    }, {
                      "holder" : "DAVIS SELECTED ADVISERS",
                      "shares" : 830319,
                      "dateReported" : "2022-06-30",
                      "change" : -29778
                    } ]



        result = filter_historical(testData, None)

        expectedResult = defaultdict(list)
        expectedResult[datetime.strptime("2022-09-30", '%Y-%m-%d').date()].append(4336424)
        expectedResult[datetime.strptime("2022-06-30", '%Y-%m-%d').date()].append(830319)

        self.assertEquals(expectedResult, result)

    def test_get_quote_benchmark(self):
        key = os.environ['FMPREPKEY']

        from datetime import date, datetime
        from collections import defaultdict
        testData = [ {  "holder" : "FIL LTD",
                      "shares" : 4336424,
                      "dateReported" : "2022-09-30",
                      "change" : -12387
                    }, {
                      "holder" : "DAVIS SELECTED ADVISERS",
                      "shares" : 830319,
                      "dateReported" : "2022-06-30",
                      "change" : -29778
                    } ]



        result = filter_historical(testData, None)

        expectedResult = defaultdict(list)
        expectedResult[datetime.strptime("2022-09-30", '%Y-%m-%d').date()].append(4336424)
        expectedResult[datetime.strptime("2022-06-30", '%Y-%m-%d').date()].append(830319)

        self.assertEquals(expectedResult, result)

    def test_canslim_filter_df(self):
        from shareloader.modules.superperf_metrics import get_fundamental_parameters, get_fundamental_parameters_qtr, \
            get_financial_ratios, get_analyst_estimates, get_asset_play_parameters, \
            compute_rsi, get_price_change, get_income_benchmark, \
            get_balancesheet_benchmark, compute_cagr, calculate_piotrosky_score

        key = os.environ['FMPREPKEY']

        for ticker in ['COLM']:
            print('--------' * 5)
            fundamental_data = get_fundamental_parameters(ticker, key)
            fundamental_qtr = get_fundamental_parameters_qtr(ticker, key)
            fundamental_data.update(fundamental_qtr)
            financial_ratios = get_financial_ratios(ticker, key)
            fundamental_data.update(financial_ratios)

            updated_dict = get_analyst_estimates(ticker, key, fundamental_data)
            descr_and_tech = get_descriptive_and_technical(ticker, key)
            updated_dict.update(descr_and_tech)
            asset_play_dict = get_asset_play_parameters(ticker, key)
            updated_dict.update(asset_play_dict)

            piotrosky_score = calculate_piotrosky_score(key, ticker)
            latest_rsi = compute_rsi(ticker, key)
            updated_dict['piotroskyScore'] = piotrosky_score
            updated_dict['rsi'] = latest_rsi

            priceChangeDict = get_price_change(ticker, key)
            updated_dict.update(priceChangeDict)
            updated_dict['stock_buy_price'] = updated_dict['priceAvg200'] * .8
            updated_dict['stock_sell_price'] = updated_dict['priceAvg200'] * .7
            updated_dict['earningYield'] = updated_dict.get('netIncome', 0) / updated_dict['marketCap']
            updated_dict_df = pd.DataFrame(list(updated_dict.items()), columns=['key', 'value'])

            canslim_filter_df = self.get_canslim_filter_df()
            merged = pd.merge(canslim_filter_df, updated_dict_df, on='key', how='left')

            self.assertTrue(merged.shape[0] > 0)
            with pd.option_context('display.max_rows', None,
                                   'display.max_columns', 5,
                                   'display.precision', 3,
                                   ):
                print(merged.to_string(index=False))

    def test_institutional_holdings(self):
        from shareloader.modules.superperf_metrics import get_fundamental_parameters, get_fundamental_parameters_qtr, \
            get_financial_ratios, get_analyst_estimates, get_asset_play_parameters, \
            compute_rsi, get_price_change, get_income_benchmark, \
            get_balancesheet_benchmark, compute_cagr, calculate_piotrosky_score, get_institutional_holders_quote, \
            get_institutional_holders_percentage, get_instutional_holders_percentage_yahoo

        key = os.environ['FMPREPKEY']

        """
        res = requests.get(
            'https://financialmodelingprep.com/api/v3/institutional-holder/{}?apikey={}'.format('COLM', key)).json()

        from collections import defaultdict

        ddict = defaultdict(list)

        for item in res:
            holder = item['holder']
            shares = item['shares']
            ddict[holder].append(shares)

        shares = [v[0] for k, v in ddict.items() ]

        print(f'COLM HOLDING are:{sum(shares)}')
        """



        for ticker in ['COLM', 'AMZN', 'MSFT', 'GOOD', 'TREX', 'HALO'] * 10:
            pcnt = get_instutional_holders_percentage_yahoo(ticker)
            print(f'{ticker} has {pcnt}')
            self.assertTrue(pcnt)
        #print(get_institutional_holders_percentage('COLM', 'NASDAQ'))


    def test_combine_tickers(self):
        from shareloader.modules.superperformers import combine_tickers

        class SplitWords(beam.DoFn):
            def __init__(self, key, keyword):
                self.d = key
                self.keyword = keyword

            def process(self, element):
                tmp = element.split(self.d)
                return tmp[0:len(tmp) //2] if 'FIRST' in self.keyword else tmp[len(tmp) //2:]


        with TestPipeline() as p:
            (p | 'START' >> beam.Create(['AMZN', 'GOOGL', 'AAPL', 'FOO', 'BAR'])
               | 'COMBINE' >> beam.CombineGlobally(combine_tickers)
               | 'prdo'   >> beam.ParDo(SplitWords(',', 'FIRST'))
               | 'out' >> self.notEmptySink
             )

    def test_stock_news(self):
        key = os.environ['FMPREPKEY']


        news = get_latest_stock_news('AAPL', key)

        self.assertTrue(news is not None)


    def test_readsp500(self):
        tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        sp500_df = tables[0]
        second_table = tables[1]
        print(sp500_df.shape)

        # rename symbol to escape symbol error
        sp500_df["Symbol"] = sp500_df["Symbol"].map(lambda x: x.replace(".", "-"))


        sp500_df = sp500_df[['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']]
        print(sp500_df.head())

        records = sp500_df.to_dict('records')

        self.assertTrue(len(records) > 0)

    def best_fit_slope(self, y: np.array) -> float:
        '''
        Determine the slope for the linear regression line

        Parameters
        ----------
        y : TYPE
            The time-series to find the linear regression line for

        Returns
        -------
        m : float
            The gradient (slope) of the linear regression line
        '''

        x = np.arange(0, y.shape[0])

        x_bar = np.mean(x)
        y_bar = np.mean(y)

        return np.sum((x - x_bar) * (y - y_bar)) / np.sum((x - x_bar) ** 2)


    def test_get_mm_trendtemplate(self):

        key = os.environ['FMPREPKEY']

        res = get_mm_trend_template('AAPL', key, numdays=4500)

        df = pd.DataFrame(data=res, columns=list(res[0].keys()))

        '''
        The current stock price is above both the 150-day (30-week) and the 200-day (40-week) moving average price lines.
        The 150-day moving average is above the 200-day moving average.
        The 200-day moving average line is trending up for at least 1 month (preferably 4–5 months minimum in most cases).
        The 50-day (10-week) moving average is above both the 150-day and 200-day moving averages.
        The current stock price is trading above the 50-day moving average.
        The current stock price is at least 30 percent above its 52-week low. (Many of the best selections will be 100 percent, 300 percent, or greater above their 52-week low before they emerge from a solid consolidation period and mount a large scale advance.)
        The current stock price is within at least 25 percent of its 52-week high (the closer to a new high the better).
        The relative strength ranking (as reported in Investor’s Business Daily) is no less than 70, and preferably in the 80s or 90s, which will generally be the case with the better selections.
        '''
        #mvg a
        df['200_ma'] = df['close'].rolling(200).mean()
        df['52_week_high'] = df['close'].rolling(52 * 5).max()
        df['52_week_low'] = df['close'].rolling(52 * 5).min()
        df['150_ma'] = df['close'].rolling(150).mean()
        df['50_ma'] = df['close'].rolling(50).mean()
        df['slope'] = df['200_ma'].rolling(40).apply(self.best_fit_slope)
        df['pricegt50avg'] = df['close'] > df['50_ma']
        df['price30pctgt52wklow'] = df['close'] / df['52_week_low'] > 1.3
        df['priceWithin25pc52wkhigh'] = df['close'] / df['52_week_high'] > 0.8
        df['trend_template'] = (
                (df['close'] > df['200_ma'])
                & (df['close'] > df['150_ma'])
                & (df['150_ma'] > df['200_ma'])
                & (df['slope'] > 0)
                & (df['50_ma'] > df['150_ma'])
                & (df['50_ma'] > df['200_ma'])
                & (df['pricegt50avg'] == True)
                & (df['priceWithin25pc52wkhigh'] == True)
                & (df['priceWithin25pc52wkhigh'] == True)
        )

        subset = df[['date', 'close', '200_ma', '150_ma', '50_ma', 'slope', '52_week_low', '52_week_high', 'trend_template'  ]]
        print(subset)

        self.assertTrue(df.shape[0] > 0)

        #subset.to_csv('c:/Users/Marco/mminervini_sample.csv', index=False)
















