import os
import unittest
import apache_beam as beam
from apache_beam.testing.util import assert_that, is_not_empty
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date
from shareloader.modules.marketstats_utils import  ParseNonManufacturingPMI,\
                        get_all_prices_for_date, get_all_us_stocks, get_all_us_stocks2, MarketBreadthCombineFn,\
                        ParseManufacturingPMI, get_economic_calendar, get_equity_putcall_ratio,\
                        get_market_momentum,\
                        get_latest_fed_fund_rates, PMIJoinerFn, NewHighNewLowLoader, get_prices2

from shareloader.modules.marketstats import run_vix, InnerJoinerFn, \
                                            run_economic_calendar, run_exchange_pipeline, run_putcall_ratio,\
                                            run_cftc_spfutures, run_senate_disclosures,\
                                            run_manufacturing_pmi, run_non_manufacturing_pmi, MarketStatsCombineFn,\
                                            run_fed_fund_rates, write_all_to_sink, run_market_momentum, \
                                            run_consumer_sentiment_index

from shareloader.modules.sector_loader import run_my_pipeline

import requests
from itertools import chain
from unittest.mock import patch
import argparse

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

class MissedJoinerFn(beam.DoFn):
    def __init__(self):
        super(MissedJoinerFn, self).__init__()

    def process(self, row, **kwargs):
        right_dict = dict(kwargs['right_list'])
        left_key = row[0]
        left = row[1]
        if left_key not in  right_dict:
            yield (left_key, left)





class TestMarketStats(unittest.TestCase):

    def setUp(self):
        self.patcher = patch('shareloader.modules.sector_loader.XyzOptions._add_argparse_args')
        self.mock_foo = self.patcher.start()
        self.notEmptySink = Check(is_not_empty())
        self.printSink = beam.Map(print)
        parser = argparse.ArgumentParser(add_help=False)

    def tearDown(self):
        self.patcher.stop()


    def test_run_vix(self):
        key = os.environ['FMPREPKEY']
        print(f'{key}|')
        with TestPipeline() as p:
                 run_vix(p, key) | self.notEmptySink

    def test_run_pmi(self):
        key = os.environ['FMPREPKEY']

        sink = Check(is_not_empty())
        printer = beam.Map(print)
        with TestPipeline() as p:
             res = (p | 'start' >> beam.Create(['20210101'])
                | 'pmi' >>   beam.ParDo(ParseNonManufacturingPMI())
                | 'remap' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today(), 'LABEL' : 'PMI', 'VALUE' : d['Last']})

                )
             res | printer
             res | sink




    def test_run_manuf_pmi(self):
        key = os.environ['FMPREPKEY']
        sink = Check(is_not_empty())
        from apache_beam.options.pipeline_options import PipelineOptions

        with TestPipeline(options=PipelineOptions()) as p:
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
            pmi = run_non_manufacturing_pmi(p)

            final = (
                    (vix_result, pmi)
                    | 'FlattenCombine all' >> beam.Flatten()
                    | 'Mapping to String' >> beam.Map(lambda data: '{}:{}'.format(data['LABEL'], data['VALUE']))
                    | 'Combine' >> beam.CombineGlobally(lambda x: '<br><br>'.join(x))
                    | self.notEmptySink

            )

    def test_runcftcspfutures(self):

        key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            run_cftc_spfutures(p, key) | self.printSink

    def test_getallpricesfordate(self):
        import pandas as pd
        key = os.environ['FMPREPKEY']
        asOfDate = date(2021, 9, 30).strftime('%Y-%m-%d')
        self.assertTrue(get_all_prices_for_date(key, asOfDate)[0:20])



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
            pmi = run_non_manufacturing_pmi(p)

            final = (
                    (left_joined, vix_result, pmi)
                    | 'FlattenCombine all' >> beam.Flatten()
                    | 'Mapping to String' >> beam.Map(lambda data: '{}:{}'.format(data['LABEL'], data['VALUE']))
                    | 'Combine' >> beam.CombineGlobally(lambda x: '<br><br>'.join(x))
                    | self.notEmptySink

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
                    | self.notEmptySink

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
                | self.notEmptySink
            )


    def test_economicCalendarData(self):
        iexapi_key = os.environ['FMPREPKEY']
        data = get_economic_calendar(iexapi_key)
        from pprint import pprint
        alldt = [d for d in data if d['impact'] == 'High']
        self.assertTrue(alldt)

    def test_economicCalendarPipeline(self):
        iexapi_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            ec = run_economic_calendar(p, iexapi_key)
            ec | self.notEmptySink

    def test_Nasdap(self):
        iexapi_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_exchange_pipeline(p, iexapi_key, 'NASDAQ Global Select')
            res | self.notEmptySink

    def test_bqueryInnerJoinernyse_tickers(self):
        from pandas.tseries.offsets import BDay
        with TestPipeline() as p:
            pcoll1 = p | 'Create coll1' >> beam.Create([{'TICKER' : 'FDX', 'COUNT' : 100},
                                                        {'TICKER' : 'AMZN', 'COUNT':20},
                                                        {'TICKER': 'MSFT', 'COUNT': 8}
                                                        ])

            pcoll2 = p | 'Create coll2' >> beam.Create([{'TICKER': 'FDX', 'PRICE' : 20, 'PRICEAVG20' : 200, 'DIVIDEND':1},
                                                        {'TICKER' : 'AMZN', 'PRICE': 2000, 'PRICEAVG20': 1500, 'DIVIDEND': 0}
                                                       ])

            coll1Mapped =  pcoll1 | 'Mapping' >> beam.Map(lambda dictionary: (dictionary['TICKER'],
                                                                               dictionary))


            coll2Mapped =  (pcoll2 | 'Mapping 2' >> beam.Map(lambda dictionary: (dictionary['TICKER'],
                                                                                 dictionary))
                            )

            left_joined = (
                    coll1Mapped
                    | 'InnerJoiner: JoinValues' >> beam.ParDo(InnerJoinerFn(),
                                                              right_list=beam.pvalue.AsIter(coll2Mapped))
                    | 'Map to flat tpl' >> beam.Map(lambda tpl: tpl[1])
                    | self.notEmptySink
            )

    def test_putcall_ratio(self):
        with TestPipeline() as p:
            p = run_putcall_ratio(p)
            p | self.notEmptySink

    def test_manufpmifetch(self):
        with TestPipeline() as p:
            run_manufacturing_pmi(p) | self.notEmptySink

    def test_nonmanufpmi(self):
        with TestPipeline() as p:
            run_non_manufacturing_pmi(p) | self.notEmptySink

    def test_get_equity_putcall_ratio(self):
        self.assertTrue(get_equity_putcall_ratio())

    def test_get_market_momentum(self):
        fmp_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            (p
             | 'Get List of Tickers' >> beam.Create([get_market_momentum(fmp_key, '^RUT')])
             | self.printSink
             )



    def test_get_vix(self):
        fmp_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            vix = run_vix(p, fmp_key)
            sd = run_senate_disclosures(p, fmp_key)
            vix_key = vix | 'Add 3' >> beam.Map(lambda d: (3, d))
            sd_key = sd | 'Add sd' >> beam.Map(lambda d: (8, d))

            final = (
                    (vix_key, sd_key,
                     )
                    | 'FlattenCombine all' >> beam.Flatten()
                    | ' do A PARDO combner:' >> beam.CombineGlobally(MarketStatsCombineFn())
                    | ' FlatMapping' >> beam.FlatMap(lambda x: x)
                    | self.notEmptySink
            )

    @patch('shareloader.modules.marketstats.get_senate_disclosures')
    def test_get_senate_disclosures(self, senate_mock):
        fmp_key = os.environ['FMPREPKEY']

        expected = [{'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'label', 'VALUE' : 'value'}]
        senate_mock.return_value = expected
        with TestPipeline(options=PipelineOptions()) as p:
            res = run_senate_disclosures(p, fmp_key)
            res | self.notEmptySink


    def test_compute_etf_historical(self):
        key = os.environ['FMPREPKEY']

        with TestPipeline() as p:
            res = run_my_pipeline(p, key)
            res   | self.notEmptySink


    def test_get_raw_cftc(self):
        key = os.environ['FMPREPKEY']
        base_url = f'https://financialmodelingprep.com/api/v4/commitment_of_traders_report_analysis/VI?apikey={key}'
        data = requests.get(base_url).json()
        print(data)
        res = [dict(date=d['date'],changeInNetPosition=d['changeInNetPosition'],marketSentiment=d['marketSentiment'],
                    marketSituation=d['marketSituation']) for d  in data]
        self.assertTrue(res)

    def test_get_latest_fed_fund_rates(self):
        frates = get_latest_fed_fund_rates()
        self.assertTrue(float(frates) > 0)

    def test_run_fed_funds_rates(self):
        key = os.environ['FMPREPKEY']


        with TestPipeline() as p:
            run_fed_fund_rates(p)  | self.notEmptySink

    def test_write_all_to_sink(self):
        fmp_key = os.environ['FMPREPKEY']
        sink = self.notEmptySink
        with TestPipeline() as p:
            vix = run_vix(p, fmp_key)
            sd = run_senate_disclosures(p, fmp_key)
            write_all_to_sink([vix, sd], sink)

    def test_get_all_us_stocks(self):
        fmp_key = os.environ['FMPREPKEY']
        self.assertTrue(get_all_us_stocks(fmp_key))

    def xtest_premarket_loader(self):
        from shareloader.modules.premarket_loader import extract_data_pipeline, extract_trend_pipeline
        fmp_key = os.environ['FMPREPKEY']
        sink = self.notEmptySink
        with TestPipeline() as p:
            result = extract_trend_pipeline(p, fmp_key)
            result | sink

    def test_missed_joinis(self):
        debugSink = beam.Map(print)
        with TestPipeline() as p:
            pcoll1 = p | 'Create coll1' >> beam.Create([{'TICKER': 'FDX'},
                                                        {'TICKER': 'AMZN'},
                                                        {'TICKER': 'MSFT'}
                                                        ])

            pcoll2 = p | 'Create coll2' >> beam.Create(
                [{'TICKER': 'FDX', 'PRICE': 20, 'PRICEAVG20': 200, 'DIVIDEND': 1},
                 {'TICKER': 'AMZN', 'PRICE': 2000, 'PRICEAVG20': 1500, 'DIVIDEND': 0}
                 ])

            coll1Mapped = pcoll1 | 'Mapping' >> beam.Map(lambda dictionary: (dictionary['TICKER'],
                                                                             dictionary))

            coll2Mapped = (pcoll2 | 'Mapping 2' >> beam.Map(lambda dictionary: (dictionary['TICKER'],
                                                                                dictionary))
                           )

            left_joined = (
                    coll1Mapped
                    | 'InnerJoiner: JoinValues' >> beam.ParDo(MissedJoinerFn(),
                                                              right_list=beam.pvalue.AsIter(coll2Mapped))
                    | 'Map to flat tpl' >> beam.Map(lambda tpl: tpl[1])
                    | debugSink
            )

    def test_pmi_joinis(self):
        debugSink = beam.Map(print)
        with TestPipeline() as p:
            pmi = run_non_manufacturing_pmi(p)
            bigQuerPmi = p | 'Create coll1' >> beam.Create([{'LABEL': 'NON-MANUFACTURING-PMI', 'AS_OF_DATE' : '2023-07-01',
                                                         'VALUE' : 55.2}
                                                        ])

            coll1Mapped = pmi | 'Mapping' >> beam.Map(lambda dictionary: (dictionary['LABEL'],
                                                                             dictionary))

            coll2Mapped = (bigQuerPmi | 'Mapping 2' >> beam.Map(lambda dictionary: (dictionary['LABEL'],
                                                                                dictionary))
                           )

            left_joined = (
                    coll1Mapped
                    | 'InnerJoiner: JoinValues' >> beam.ParDo(PMIJoinerFn(),
                                                              right_list=beam.pvalue.AsIter(coll2Mapped))
                    | 'Map to flat tpl' >> beam.Map(lambda tpl: tpl[1])
                    | debugSink
            )

    def test_consumer_sentiment(self):
        debugSink = beam.Map(print)
        with TestPipeline() as p:
            pmi = run_consumer_sentiment_index(p)
            pmi | debugSink

    def test_newhigh_newlow(self):
        fmp_key = os.environ['FMPREPKEY']
        nyse = get_all_us_stocks2(fmp_key, "New York Stock Exchange")
        full_ticks = 'APPF,CIDM,FXCO,GMM,HZNP,IMAC,INTR,ERYP'
        debugSink = beam.Map(print)

        with TestPipeline() as p:
            res = ( p
                    | 'Start' >> beam.Create([full_ticks])
                    | 'Get all List' >> beam.ParDo(NewHighNewLowLoader(fmp_key))
                    |  debugSink
            )

    def test_prices2(self):
        fmp_key = os.environ['FMPREPKEY']
        full_ticks = 'APPF,CIDM'

        for tick in full_ticks.split(','):
            res = get_prices2(tick, fmp_key)
            print('foo')




if __name__ == '__main__':
    unittest.main()
