import os
import unittest
from shareloader.modules.marketstats_utils import get_all_stocks
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date
from shareloader.modules.marketstats_utils import get_all_stocks, get_prices2, ParsePMI,PutCallRatio, get_vix,\
                        get_all_prices_for_date, get_all_us_stocks, get_all_us_stocks2, MarketBreadthCombineFn
from shareloader.modules.marketstats import run_vix, InnerJoinerFn, run_pmi
from bs4 import  BeautifulSoup
import requests
from io import StringIO
from itertools import chain




class TestShareLoader(unittest.TestCase):
    def test_all_stocks(self):
        key = os.environ['FMPREPKEY']
        print(get_all_stocks(key))

    def test_run_sample(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
                 (p
                  | 'Get List of Tickers' >> beam.Create(get_all_stocks(key))
                  | 'Getting Prices' >> beam.Map(lambda symbol: get_prices2(symbol, key))
                  #| 'Filtering blanks' >> beam.Filter(lambda d: len(d) > 0)
                  | 'Print out' >> beam.Map(print)
                  )

    def test_run_vix(self):
        key = os.environ['FMPREPKEY']
        print(f'{key}|')
        with TestPipeline() as p:
                 run_vix(p, key) | beam.Map(print)

    def test_run_pmi(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
                 (p | 'start' >> beam.Create(['20210101'])
                    | 'pmi' >>   beam.ParDo(ParsePMI())
                    | 'remap' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today(), 'LABEL' : 'PMI', 'VALUE' : d['Actual']})
                    | 'out' >> beam.Map(print)
                )

    def test_pmi_and_vix(self):
        key = os.environ['FMPREPKEY']
        print(f'{key}|')
        with TestPipeline() as p:
            vix_result = run_vix(p, key)
            pmi = run_pmi(p)

            final = (
                    (vix_result, pmi)
                    | 'FlattenCombine all' >> beam.Flatten()
                    | 'Mapping to String' >> beam.Map(lambda data: '{}:{}'.format(data['LABEL'], data['VALUE']))
                    | 'Combine' >> beam.CombineGlobally(lambda x: '<br><br>'.join(x))
                    |' rint out' >> beam.Map(print)

            )





    def test_getallpricesfordate(self):
        import pandas as pd
        key = os.environ['FMPREPKEY']
        asOfDate = date(2021, 9, 30).strftime('%Y-%m-%d')
        print(get_all_prices_for_date(key, asOfDate)[0:20])

    def test_nyse_tickers(self):
        from pandas.tseries.offsets import BDay
        key = os.environ['FMPREPKEY']
        asOfDate = date(2021,9,23)
        prevDate = (asOfDate - BDay(1)).date()
        asOfDateStr = asOfDate.strftime('%Y-%m-%d')
        prevDateStr = prevDate.strftime('%Y-%m-%d')
        dt = get_all_prices_for_date(key, asOfDateStr)
        
        ydt = get_all_prices_for_date(key, prevDateStr)
        
        filtered = [(d['symbol'], d)  for d in dt]
        y_filtered = [(d['symbol'], {'prevClose': d['close']})  for d in ydt]
        all_us_stocks = list(map(lambda t: (t, {}), get_all_us_stocks2(key, "New York Stock Exchange")))

        tmp = [tpl[0] for tpl in all_us_stocks]
        fallus = [tpl for tpl in filtered if tpl[0] in tmp]
        yfallus = [tpl for tpl in y_filtered if tpl[0] in tmp]
        with TestPipeline() as p:
            pcoll1 = p | 'Create coll1' >> beam.Create(all_us_stocks)
            pcoll2 = p | 'Create coll2' >> beam.Create(fallus)
            pcoll3 = p | 'Crete ydaycoll' >> beam.Create(yfallus)

            pcollStocks = pcoll2 | 'Joining y' >> beam.ParDo(InnerJoinerFn(),
                                                             right_list=beam.pvalue.AsIter(pcoll3))

            left_joined = (
                    pcoll1
                    | 'InnerJoiner: JoinValues' >> beam.ParDo(InnerJoinerFn(),
                                                              right_list=beam.pvalue.AsIter(pcollStocks))
                    | 'Map to flat tpl' >> beam.Map(lambda tpl: (tpl[0], tpl[1]['close'], tpl[1]['close'] - tpl[1]['prevClose']))
                    | 'Combine MarketBreadth Statistics' >> beam.CombineGlobally(MarketBreadthCombineFn())
                    | 'mapping' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'),
                                                        'LABEL' : 'NYSE_{}'.format(d[0:d.find(':')]),
                                                       'VALUE' : d[d.rfind(':'):]})

            )
            vix_result = run_vix(p, key)
            pmi = run_pmi(p)

            final = (
                    (left_joined, vix_result, pmi)
                    | 'FlattenCombine all' >> beam.Flatten()
                    | 'Mapping to String' >> beam.Map(lambda data: '{}:{}'.format(data['LABEL'], data['VALUE']))
                    | 'Combine' >> beam.CombineGlobally(lambda x: '<br><br>'.join(x))
                    | ' rint out' >> beam.Map(print)

            )

    def test_another(self):
        import pandas as pd
        print(requests.get(
            'https://financialmodelingprep.com/api/v4/batch-request-end-of-day-prices?date=2021-09-23&apikey=79d4f398184fb636fa32ac1f95ed67e6').text)


if __name__ == '__main__':
    unittest.main()
