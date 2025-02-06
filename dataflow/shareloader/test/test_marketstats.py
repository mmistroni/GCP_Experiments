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
                        get_shiller_indexes, AdvanceDecline, AdvanceDeclineSma

from shareloader.modules.marketstats import run_vix, InnerJoinerFn, \
                                            run_economic_calendar, run_putcall_ratio,\
                                            run_cftc_spfutures, \
                                            run_manufacturing_pmi, run_non_manufacturing_pmi, MarketStatsCombineFn,\
                                            run_fed_fund_rates, write_all_to_sink, run_market_momentum, \
                                            run_consumer_sentiment_index, run_newhigh_new_low,  run_junk_bond_demand, \
                                            run_cramer_pipeline, run_advance_decline, run_sp500multiples, \
                                            run_advance_decline_sma


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


    def test_scrapy(self):
        from scrapy.http import FormRequest
        from scrapy.crawler import CrawlerProcess
        class MySpider(scrapy.Spider):
            name = 'my_spider'
            start_urls = ['https://efdsearch.senate.gov/']

            def parse(self, response):
                # Extract the hidden token
                token = response.xpath('//input[@name="csrfmiddlewaretoken"]/@value').get()

                # Construct the form data
                formdata = {
                    'csrfmiddlewaretoken': token,
                    'reportTypes' : '11',
                    'submitted_start_date' : '2024-08-15',
                    'submitted_end_date': '2024-08-21'

                    # Add other form fields here
                }

                # Submit the form
                yield FormRequest(
                    url=response.url,
                    method='POST',
                    formdata=formdata,
                    callback=self.parse_response
                )

            def parse_response(self, response):
                # Process the response here
                print(f'Got:{response}')
                pass

        process = CrawlerProcess()
        process.crawl(MySpider)
        process.start()

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




if __name__ == '__main__':
    unittest.main()
