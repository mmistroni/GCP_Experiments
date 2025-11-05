import os
import unittest
import scrapy
import apache_beam as beam
from apache_beam.testing.util import assert_that, is_not_empty
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date, datetime
from shareloader.modules.marketstats_utils import  ParseNonManufacturingPMI,\
                        get_all_prices_for_date, get_all_us_stocks, get_all_us_stocks2, MarketBreadthCombineFn,\
                        ParseManufacturingPMI, get_economic_calendar, get_equity_putcall_ratio,\
                        get_market_momentum,\
                        get_latest_fed_fund_rates, PMIJoinerFn, NewHighNewLowLoader, get_prices2,\
                        get_mcclellan, get_cftc_spfutures, parse_consumer_sentiment_index,\
                        get_shiller_indexes, AdvanceDecline, AdvanceDeclineSma, get_obb_vix, AsyncFetcher,\
                        OBBMarketMomemtun, BenzingaNews, AsyncSectorRotation, get_sector_rotation_indicator, \
                        AsyncEconomicCalendar, generate_cotc_data, get_cot_futures, SentimentCalculator

from shareloader.modules.marketstats import run_vix, InnerJoinerFn, \
                                            run_economic_calendar, run_putcall_ratio,\
                                            run_cftc_spfutures, \
                                            run_manufacturing_pmi, run_non_manufacturing_pmi, MarketStatsCombineFn,\
                                            run_fed_fund_rates, write_all_to_sink, run_market_momentum, \
                                            run_consumer_sentiment_index, run_newhigh_new_low,  run_junk_bond_demand, \
                                            run_cramer_pipeline, run_advance_decline, run_sp500multiples, \
                                            run_advance_decline_sma, run_economic_calendar, run_cftc_spfutures
from shareloader.modules.obb_processes import AsyncCFTCTester


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


class AppendN(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, accumulator, input):
        val = f"AS_OF_DATE:{input['AS_OF_DATE']}|{input['LABEL']} {input['VALUE']}"
        accumulator.append(f'{val}')
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = []
        for acc in accumulators:
            merged.extend(acc)
        return merged

    def extract_output(self, accumulator):
        return '\n'.join(accumulator)


class TestMarketStats(unittest.TestCase):

    def setUp(self):
        self.notEmptySink = Check(is_not_empty())
        self.printSink = beam.Map(print)

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
        
        #ydt = get_all_prices_for_date(key, prevDateStr)
        
        filtered = [(d['symbol'], d)  for d in dt]
        #y_filtered = [(d['symbol'], {'prevClose': d['close']})  for d in ydt]
        all_us_stocks = list(map(lambda t: (t, {}), get_all_us_stocks2(key, "New York Stock Exchange")))

        tmp = [tpl[0] for tpl in all_us_stocks]
        fallus = [tpl for tpl in filtered if tpl[0] in tmp]
        with TestPipeline() as p:
            pcoll1 = p | 'Create coll1' >> beam.Create(all_us_stocks)
            pcoll2 = p | 'Create coll2' >> beam.Create(fallus)
            
            
            left_joined = (
                    pcoll1
                    | 'InnerJoiner: JoinValues' >> beam.ParDo(InnerJoinerFn(),
                                                              right_list=beam.pvalue.AsIter(pcoll2))
                    | 'Map to flat tpl' >> beam.Map(lambda tpl: (tpl[0], tpl[1]['close'], tpl[1]['close']))
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
        from shareloader.modules.marketstats import run_senate_disclosures
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
        full_ticks = 'ADSE,ALNY,AMZN,AOSL,BRFH,COKE,DAKT,DXJS,DYCQR,EUDA,FORLU,FTE,HLAL,HSPO,INDH,INDY,ISRG,JVSA,MSFL,MSFT,NPAB,PAL,PFX,PNQI,RDVT,SLNHP,TTMI,VITL,VSEC,WENAW,WTFCM'
        debugSink = beam.Map(print)

        with TestPipeline() as p:
            res = ( p
                    | 'Start' >> beam.Create([full_ticks])
                    | 'Get all List' >> beam.ParDo(NewHighNewLowLoader(fmp_key))
                    |  debugSink
            )
    def test_highlowpipeline(self):
        fmp_key = os.environ['FMPREPKEY']
        debugSink = beam.Map(print)

        with TestPipeline() as p:
            res = run_newhigh_new_low(p, fmp_key)
            res | debugSink


    def test_junkbonddemand(self):
        key = os.environ['FREDKEY']
        debugSink = beam.Map(print)

        with TestPipeline() as p:
            res = run_junk_bond_demand(p, key)
            res | debugSink


    def test_prices2(self):
        fmp_key = os.environ['FMPREPKEY']
        full_ticks = 'APPF,CIDM'

        for tick in full_ticks.split(','):
            res = get_prices2(tick, fmp_key)
            print('foo')

    def test_get_cftc_spfutures(selfself):
        fmp_key = os.environ['FMPREPKEY']
        base_url = f'https://financialmodelingprep.com/api/v4/commitment_of_traders_report_analysis/VX?apikey={fmp_key}'
        all_data = requests.get(base_url).json()
        print(all_data)

    def test_get_cftc_spfutures_pipeline(selfself):
        fmp_key = os.environ['FMPREPKEY']
        base_url = f'https://financialmodelingprep.com/api/v4/commitment_of_traders_report_analysis/VX?apikey={fmp_key}'
        all_data = requests.get(base_url).json()
        print(all_data)




    def test_McClellan(self):

        with TestPipeline() as p:
            res = ( p
                    | 'Start' >> beam.Create(['$NYSI'])
                    | 'Get mmcl' >> beam.Map(get_mcclellan)
                    |  'nysi sink' >>self.printSink
            )

            res2 = (p
                   | 'Start2' >> beam.Create(['$NYMO'])
                   | 'Get mmcl2' >> beam.Map(get_mcclellan)
                   | 'nymo sink' >> self.printSink
                   )

        import pandas as pd
        #res1 = get_mcclellan('$NYSI')
        #print(res1)
        #res2 = get_mcclellan('$NYMO')
        #print(res2)

    def test_parse_consumer_sentiment_index(self):

        res = parse_consumer_sentiment_index()
        print(res)
        assert res is not None


    def test_cramer_pipeline(self):
        fmp_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_cramer_pipeline(p, fmp_key, 7)
            debugSink = beam.Map(print)

            res | debugSink


    def test_shillers(self):
        debugSink = beam.Map(print)

        with TestPipeline() as p:
            (p | 'shiller starter' >> beam.Create(['20240101'])
             | 'getting shillers' >> beam.FlatMap(lambda d: get_shiller_indexes())
             | 'combining' >> beam.CombineGlobally(AppendN())
             | 'todbg' >> debugSink
             )

        # QuiverQuants top funds https://www.quiverquant.com/sec13f/

    def test_sp500(self):
        debugSink = beam.Map(print)

        with TestPipeline() as p:
            res = run_sp500multiples(p)

            (res | 'combinin g sp' >> beam.CombineGlobally(AppendN())
             | 'todbg sp' >> debugSink
             )


    def test_advance_decline(self):
        debugSink = beam.Map(print)

        with TestPipeline() as p:
            res = run_advance_decline(p, 'NASDAQ')
            res |  debugSink

    def test_pips(self):
        debugSink = beam.Map(print)

        with TestPipeline() as p:
            (p | 'start' >> beam.Create(['foo'])
               |  'out' >> debugSink
             )

    def test_newadvancedecline(self):
        debugSink = beam.Map(print)
        with TestPipeline() as p:
            res = ( p
                    | 'Start' >> beam.Create(['NASDAQ'])
                    | 'Get all List' >> beam.ParDo(AdvanceDecline())
                    |  debugSink
            )

    def test_advance_declinesma50(self):
        debugSink = beam.Map(print)

        with TestPipeline() as p:
            res = run_advance_decline_sma(p, 'NASDAQ', 50)
            res |  debugSink

    def test_get_obb_vix(self):
        fmp_key = os.environ['FMPREPKEY']
        debugSink = beam.Map(print)
        with TestPipeline() as p:
            res = ( p
                    | 'Start' >> beam.Create(['^VIX'])
                    | 'Get all List' >> beam.ParDo(AsyncFetcher(fmp_key))
                    | 'remap vix' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'VIX', 'VALUE' : str(d['close'])})
                    |  debugSink
            )

    def test_get_obb_market_momentum(self):
        debugSink = beam.Map(print)
        fmp_key = os.environ['FMPREPKEY']
        
        with TestPipeline() as p:
            res = ( p
                    | 'Start' >> beam.Create(['^GSPC'])
                    | 'Get all List' >> beam.ParDo(OBBMarketMomemtun(fmp_key))
                    | f'remap mm_tst' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'MARKET_MOMENTUM', 'VALUE' : str(d)})
                    |  debugSink
            )

    def test_run_market_momentum(self):
        debugSink = beam.Map(print)
        fmp_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_market_momentum(p, fmp_key)
            res | debugSink

    def test_run_market_momentum_composite(self):
        debugSink = beam.Map(print)
        fmp_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_market_momentum(p, fmp_key, ticker='^NYA')
            res | debugSink


    def test_new_sector_rotation(self):
        debugSink = beam.Map(print)
        fmp_key = os.environ['FMPREPKEY']
        
        with TestPipeline() as p:
            res = ( p
                    | 'Start' >> beam.Create(['^GSPC'])
                    | 'Get all List' >> beam.ParDo(AsyncSectorRotation(fmp_key))
                    | f'remap mm_tst' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'SECTOR ROTATION(GROWTH/VALUE)', 'VALUE' : str(d)})
                    |  debugSink
            )

    def test_economic_calendar(self):
        debugSink = beam.Map(print)
        fmp_key = os.environ['FMPREPKEY']
        
        with TestPipeline() as p:
            res = ( p
                    | 'Start' >> beam.Create(['^GSPC'])
                    | 'Get all List' >> beam.ParDo(AsyncEconomicCalendar(fmp_key))
                    | 'reMapping' >> beam.Map(lambda d: {'AS_OF_DATE' : d['date'],
                                                          'LABEL' : d['event'],
                                                         'VALUE' : f"Previous:{d.get('previous', '')},Estimate:{d.get('consensus', '')},Actual:{d.get('actual') or ''}"
                                                         }
                                              )
                    |  debugSink
            )
    
        print('------------------')
        from pprint import pprint
        pprint(get_economic_calendar(fmp_key))

    def test_run_econ_calendar_ppln(self):
        debugSink = beam.Map(print)
        fmp_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_economic_calendar(p, fmp_key)
            res | debugSink

    def test_run_cotc(self):
        debugSink = beam.Map(print)
        fmp_key = os.environ['FMPREPKEY']
        with TestPipeline() as p:
            res = run_cftc_spfutures(p, fmp_key)
            res | debugSink

    def test_commitmentoftrades(self):
        key = os.environ['FMPREPKEY']
        base_url = f'https://financialmodelingprep.com/api/v4/commitment_of_traders_report/?apikey={key}'
        all_data = requests.get(base_url).json()
        from pprint import pprint

        vx = [d for d in all_data if d['symbol'] == 'VX']

        from pprint import pprint
        pprint(vx[0:10])
        print('----------')
        pprint(vx[-5:])
        print('------')
        print(vx[0])

    def test_run_cotc2(self):
        key = os.environ['FMPREPKEY']
        base_url = f'https://financialmodelingprep.com/api/v4/commitment_of_traders_report_analysis/VX?apikey={key}'
        all_data = requests.get(base_url).json()

        from pprint import pprint

        vx = [d for d in all_data if d['symbol'] == 'VX']

        from pprint import pprint
        pprint(vx[0:10])
        print('----------')
        pprint(vx[-5:])
        print('------')
        print(vx[0])

    def test_generate_cotc_data(self):
        sample = {'symbol' : 'TST', 'as_of_date_in_form_yymmdd' : '250114',
                'short_name' : 'tst', 'market_and_exchange_names' : 'none',
              'cftc_contract_market_code':'x', 'noncomm_positions_long_all' : 26107,
                  'noncomm_positions_short_all' : 29018,
              'comm_positions_long_all' : 33114, 'comm_positions_short_all' : 28483}

        print(generate_cotc_data(sample))


    def test_get_historical_prices(self):
        from datetime import datetime

        key = os.environ['FMPREPKEY']



    def test_get_cotc_futures_obb(self):
        from datetime import datetime
        import pandas as pd
        from apache_beam import pvalue
        pd.set_option('display.max_columns', None)

        def get_historical_prices(ticker, start_date, key):
            hist_url = f'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={ticker}&apikey={key}&from=2004-01-01'

            data =  requests.get(hist_url).json()
            return [d for d in data if datetime.strptime(d['date'], '%Y-%m-%d').date() >=start_date]

        key = os.environ['FMPREPKEY']

        with TestPipeline() as p:
            pcoll_cftc = (
                    p
                    | 'Tester' >> beam.Create([('^VIX', '1170E1')])
                    | 'CFTC' >> beam.ParDo(AsyncCFTCTester(credentials={'fmp_api_key': key}))
            )

        # 1. Capture the data into the external list using a DoFn
        vix_prices = get_historical_prices('^VIX', date(2004, 1, 1), key)


        pcoll_yahoo = (p | beam.Create(vix_prices))

        import apache_beam as beam
        import pandas as pd

        # Assuming pcoll_yahoo contains all historical daily price data
        # We'll use a single-element trigger to materialize all price data for resampling

        class ResampleToWeekly(beam.DoFn):
            def process(self, dummy_element, all_prices_list):
                # 1. Load all historical prices into a DataFrame (Side Input)
                df = pd.DataFrame(all_prices_list)
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date').sort_index()

                # 2. Resample to Weekly (using the last price of the week)
                # We need a custom mapping to align exactly with the COT Tuesday date.
                # If your COT date is the Tuesday, we must align the price to that day.

                # Standard weekly resampling (e.g., using Friday close):
                # df_weekly = df['close'].resample('W-FRI').last().dropna().reset_index()

                # Better: Resample to end-of-day TUESDAY ('W-TUE') and use that closing price:
                df_weekly = df['close'].resample('W-TUE').last().dropna().reset_index()

                # 3. Yield the resampled weekly records back to Beam
                for _, row in df_weekly.iterrows():
                    yield {
                        'date': row['date'].strftime('%Y-%m-%d'),  # Standardized string key
                        'close': row['close']
                    }

        # --- Apply Resampling ---
        # 1. Convert historical price PCollection into a single list view
        price_list_view = beam.pvalue.AsList(pcoll_yahoo)
        trigger = p | 'CreateTrigger' >> beam.Create([None])

        pcoll_yahoo_weekly = (
                trigger
                | 'ResamplePrices' >> beam.ParDo(ResampleToWeekly(), all_prices_list=price_list_view)
                | 'KeyYahooWeekly' >> beam.Map(lambda x: (x['date'], x))
        )






        #1 getting basics
        LOOKBACK_WINDOW_SIZE = 156
        # Assume pcoll_cftc and pcoll_yahoo are available

        # 1. Key and Standardize CFTC Data
        keyed_cftc = (
                pcoll_cftc
                | 'KeyCFTC' >> beam.Map(
            # Convert date object to YYYY-MM-DD string for joining
            lambda x: (x['date'].strftime('%Y-%m-%d'), x)
        )
        )

        # 2. Key Yahoo Finance Data
        keyed_yahoo = (
                pcoll_yahoo_weekly
                | 'KeyYahoo' >> beam.Map(
            # Yahoo date is already the correct string format
            lambda x: (x['date'], x)
        )
        )

        #2. join and calculate net positions
        class CalculateNetPosition(beam.DoFn):
            def process(self, element):
                date, joined_data = element

                cftc_list = joined_data.get('COT', [])
                yahoo_list = joined_data.get('Price', [])

                if cftc_list and yahoo_list:
                    cot = cftc_list[0]
                    price = yahoo_list[0]

                    # 🚨 Pydantic Reminder: Validation check should happen here 🚨

                    # Calculate daily Net Position
                    net_position = cot['noncomm_positions_long_all'] - cot['noncomm_positions_short_all']

                    # Yield the record keyed by the market code (for the rolling GroupByKey)
                    yield (cot['cftc_contract_market_code'], {
                        'date': date,
                        'net_position': net_position,
                        'price': price['close']
                    })

        # --- Join ---
        joined_pcoll = (
                {'COT': keyed_cftc, 'Price': keyed_yahoo}
                | 'JoinDataOnDate' >> beam.CoGroupByKey()
        )

        # --- Calculate Net Position ---
        net_position_pcoll = (
                joined_pcoll
                | 'CalculateNetPos' >> beam.ParDo(CalculateNetPosition())
        )


        #3 Rolling COT index
        class CalculateRollingIndexAndSignal(beam.DoFn):
            def __init__(self, window_size):
                self.window_size = window_size

            def process(self, element):
                market, historical_data = element

                # 1. Create a Pandas DataFrame on the market's history
                df = pd.DataFrame(historical_data)

                # 2. Sort by date (CRITICAL for rolling calculations)
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values(by='date').reset_index(drop=True)

                # 3. Calculate Rolling Min/Max
                min_pos = df['net_position'].rolling(window=self.window_size, min_periods=1).min()
                max_pos = df['net_position'].rolling(window=self.window_size, min_periods=1).max()

                # 4. Calculate COT Index (Vectorized)
                # Use .fillna(50) to set the initial value before the window is full
                # Use .where() to handle division by zero (min == max)
                df['cot_index'] = (
                        100 * (df['net_position'] - min_pos) / (max_pos - min_pos)
                ).where(min_pos != max_pos, 50.0)  # Set to 50 if min == max

                # 5. Signal Generation (Vectorized)
                df['signal'] = 'Neutral'
                df.loc[df['cot_index'] < 10.0, 'signal'] = 'Oversold (BUY)'
                df.loc[df['cot_index'] > 90.0, 'signal'] = 'Overbought (SELL)'

                # 6. Yield final results back to Beam
                yield from df[['date', 'cot_index', 'signal', 'price']].to_dict('records')

        # --- Calculate Rolling Index ---
        final_cot_data = (
                net_position_pcoll
                | 'GroupAllHistoryByMarket' >> beam.GroupByKey()
                | 'CalculateRollingIndex' >> beam.ParDo(CalculateRollingIndexAndSignal(LOOKBACK_WINDOW_SIZE))
        )

        final_cot_data | 'output' >> beam.Map(print)


        from pprint import pprint
        results_list = []
        '''
        cot_futures = cftc_records | 'CaptureList' >> beam.ParDo(ListCapture(results_list))



        # Example usage (you would use your actual data here)
        calc = SentimentCalculator()
        analysis_df = calc.calculate_sentiment(cot_futures, vix_prices_df)

        print(analysis_df)

        current_dt = datetime.now().strftime('')


        if analysis_df is not None:
            SentimentCalculator.plot_cot_vix_relationship(analysis_df, file_name='c:/Temp/cot_vix_plot.png')

        # >>> obb.regulators.cftc.cot(id='1170E1', provider='cftc').columns
        '''

if __name__ == '__main__':
    unittest.main()
