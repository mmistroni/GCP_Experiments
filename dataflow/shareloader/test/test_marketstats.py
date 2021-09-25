import os
import unittest
from shareloader.modules.marketstats_utils import get_all_stocks
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date
from shareloader.modules.marketstats_utils import get_all_stocks, get_prices2, ParsePMI,PutCallRatio, get_vix,\
                        get_all_prices_for_date, get_all_us_stocks, get_all_us_stocks2
from shareloader.modules.marketstats import run_vix
from bs4 import  BeautifulSoup
import requests
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


    def test_getallpricesfordate(self):
        import pandas as pd
        key = os.environ['FMPREPKEY']
        asOfDate = date(2021, 9, 23).strftime('%Y-%m-%d')
        bulkRequest = pd.read_csv(
            'https://financialmodelingprep.com/api/v4/batch-request-end-of-day-prices?date={}&apikey={}'.format(
                asOfDate, key),
            header=0)
        print(bulkRequest[0:10].to_dict('records'))

    def test_nyse_tickers(self):
        key = os.environ['FMPREPKEY']
        asOfDate = date(2021,9,23).strftime('%Y-%m-%d')
        dt = get_all_prices_for_date(key, asOfDate)

        filtered = [(d['symbol'], d)  for d in dt]

        all_us_stocks = list(map(lambda t: (t, {}), get_all_us_stocks2(key, "New York Stock Exchange")))

        tmp = [tpl[0] for tpl in all_us_stocks]

        fallus = [tpl for tpl in filtered if tpl[0] in tmp]

        print('TMP IS:{}'.format(len(tmp)))

        print('FALLUS is:{}'.format(len(fallus)))

        with TestPipeline() as p:
            pcoll1 = p | 'Create coll1' >> beam.Create(all_us_stocks)
            pcoll2 = p | 'Create coll2' >> beam.Create(fallus)
            left_joined = (
                    pcoll1
                    | 'InnerJoiner: JoinValues' >> beam.ParDo(InnerJoinerFn(),
                                                              right_list=beam.pvalue.AsIter(pcoll2))
                    | 'Display' >> beam.Map(print)
            )



    def test_left_joins(self):
        with TestPipeline() as p:
            pcoll1 = p | 'Create coll1' >> beam.Create(
                [('AMZN', {'price': 20, 'performance': 20}),
                 ('key2', {'price': 2, 'performance': 1}),
                 ('key3', {'price': 3, 'performance': 3})
                 ])
            pcoll2 = p | 'Create coll2' >> beam.Create([('MCD', {'count': 410}),
                                                        ('key2', {'count': 4})]
                                                       )
            left_joined = (
                    pcoll1
                    | 'InnerJoiner: JoinValues' >> beam.ParDo(InnerJoinerFn(),
                                                              right_list=beam.pvalue.AsIter(pcoll2))
                    | 'Display' >> beam.Map(print)
            )


if __name__ == '__main__':
    unittest.main()
