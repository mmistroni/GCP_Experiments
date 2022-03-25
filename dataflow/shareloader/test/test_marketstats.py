import os
import unittest
from shareloader.modules.marketstats_utils import get_all_stocks
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date
from shareloader.modules.marketstats_utils import get_all_stocks, get_prices2, ParsePMI,PutCallRatio, get_vix,\
                        get_all_prices_for_date, get_all_us_stocks, get_all_us_stocks2, MarketBreadthCombineFn,\
                        ParseManufacturingPMI, get_economic_calendar
from shareloader.modules.marketstats import run_vix, InnerJoinerFn, run_pmi, run_exchange_pipeline,\
                                            run_economic_calendar
from itertools import chain
from bs4 import  BeautifulSoup
import requests
from io import StringIO
from itertools import chain

class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class FlattenDoFn(beam.DoFn):

    def process(self, element):
        try:
            return list(chain(*element))
        except Exception as e:
            print('Failed to get PMI:{}'.format(str(e)))
            return [{'Last' : 'N/A'}]




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

        sink = Check(is_not_empty())

        with TestPipeline() as p:
                 (p | 'start' >> beam.Create(['20210101'])
                    | 'pmi' >>   beam.ParDo(ParsePMI())
                    | 'remap' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today(), 'LABEL' : 'PMI', 'VALUE' : d['Last']})
                    | 'out' >> sink
                )

    def test_run_manuf_pmi(self):
        key = os.environ['FMPREPKEY']
        sink = Check(is_not_empty())

        with TestPipeline() as p:
                 (p | 'start' >> beam.Create(['20210101'])
                    | 'pmi' >>   beam.ParDo(ParseManufacturingPMI())
                    | 'remap' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today(), 'LABEL' : 'MANUFACTURING-PMI', 'VALUE' : d['Last']})
                    | 'out' >> sink
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
        iexapi_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            nyse = run_exchange_pipeline(p, iexapi_key, "New York Stock Exchange")
            nasdaq = run_exchange_pipeline(p, iexapi_key, "Nasdaq Global Select")

            final = (
                    (nyse, nasdaq)
                    | 'FlattenCombine all' >> beam.Flatten()
                    | 'Mapping to String' >> beam.Map(lambda data: '{}:{}'.format(data['LABEL'], data['VALUE']))
                    | 'Combine' >> beam.CombineGlobally(lambda x: '<br><br>'.join(x))
                    | 'print outl' >> beam.Map(print)

            )
    def test_combineGlobally(self):

        class AverageFn(beam.CombineFn):
            def create_accumulator(self):
                return []

            def add_input(self, sum_count, input):
                holder = sum_count
                holder.append(input)
                return holder

            def merge_accumulators(self, accumulators):
                return chain(*accumulators)

            def extract_output(self, sum_count):
                all_data = sum_count
                sorted_els = sorted(all_data, key=lambda t: t[0])
                mapped = list(map(lambda tpl: '{}:{}'.format(tpl[1]['LABEL'], tpl[1]['VALUE']), sorted_els))
                return mapped



        iexapi_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            nyse =  (p | 'Create coll1' >> beam.Create([{'LABEL' :'One', 'VALUE' : 1},
                                                       {'LABEL' :'Two', 'VALUE' : 2},
                                                       {'LABEL' :'Three', 'VALUE' : 3}])
                       |'Map To Key' >> beam.Map(lambda d: (1, d))
                     )

            nasdaq = (p | 'Create coll2' >> beam.Create([{'LABEL' :'Four', 'VALUE' : 4},
                                                       {'LABEL' :'Five', 'VALUE' : 5},
                                                       {'LABEL' :'Six', 'VALUE' : 6}])
                       |'Map To Key1' >> beam.Map(lambda d: (3, d))
                      )

            stats = ( p | 'Create coll3' >> beam.Create([{'LABEL': 'FourtyFive', 'VALUE': 4},
                                                        {'LABEL': 'FiveHundred', 'VALUE': 5},
                                                        {'LABEL': 'SixThousands', 'VALUE': 6}])
                      | 'Map To Key2' >> beam.Map(lambda d: (2, d))
                      )

            final = (
                (nyse, nasdaq, stats)
                | 'FlattenCombine all' >> beam.Flatten()

                | ' do A PARDO:' >> beam.CombineGlobally(AverageFn())
                | 'print outl' >> beam.Map(print)
            )
           # nearly there, Now we need to map each generated collection to a (key, collection) so that we can then
           # sort keys and give it to a ParDo

    def test_economicCalendarData(self):
        iexapi_key = os.environ['FMPREPKEY']
        data = get_economic_calendar(iexapi_key)
        from pprint import pprint
        alldt = [d for d in data if d['impact'] == 'High']
        print(type(alldt[0]['date']))

    def test_economicCalendarPipeline(self):
        iexapi_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            ec = run_economic_calendar(p, iexapi_key)
            ec | 'Printing out' >> beam.Map(print)

if __name__ == '__main__':
    unittest.main()
