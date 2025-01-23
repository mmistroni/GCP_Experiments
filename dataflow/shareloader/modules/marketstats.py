from __future__ import absolute_import

import logging
from pandas.tseries.offsets import BDay
from itertools import chain
from apache_beam.io.gcp.internal.clients import bigquery

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime, date
from .marketstats_utils import MarketBreadthCombineFn, \
                            get_vix, ParseNonManufacturingPMI, get_all_us_stocks2,\
                            get_all_prices_for_date, InnerJoinerFn, create_bigquery_ppln,\
                            ParseManufacturingPMI,get_economic_calendar, get_equity_putcall_ratio,\
                            get_cftc_spfutures, create_bigquery_ppln_cftc, get_market_momentum, \
                            get_senate_disclosures, create_bigquery_manufpmi_bq, create_bigquery_nonmanuf_pmi_bq,\
                            get_sector_rotation_indicator, get_latest_fed_fund_rates,\
                            get_latest_manufacturing_pmi_from_bq, PMIJoinerFn, ParseConsumerSentimentIndex,\
                            get_latest_non_manufacturing_pmi_from_bq, create_bigquery_pipeline,\
                            get_mcclellan, get_all_us_stocks, get_junkbonddemand, \
                            get_cramer_picks, NewHighNewLowLoader, get_shiller_indexes
from shareloader.modules.finviz_utils import  get_advance_decline
from shareloader.modules.obb_utils import AsyncProcessSP500Multiples

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization, To
from functools import reduce
import time
import argparse

class MarketStatsCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, sum_count, input_data):
        holder = sum_count
        logging.info('Adding:{}'.format(input_data))
        holder.append(input_data)
        return holder

    def merge_accumulators(self, accumulators):
        return chain(*accumulators)

    def extract_output(self, sum_count):
        all_data = sum_count
        sorted_els = sorted(all_data, key=lambda t: t[0])
        mapped = list(map (lambda tpl: tpl[1], sorted_els))
        
        stringified = list(map(lambda x: '|{}|{}|{}|'.format(x['AS_OF_DATE'],
                                                           x['LABEL'],
                                                           x['VALUE']), mapped))
                
        
        logging.info('MAPPED IS :{}'.format(mapped))
        return stringified


class MarketStatsSinkCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, sum_count, input_data):
        holder = sum_count
        holder.append(input_data)
        return holder

    def merge_accumulators(self, accumulators):
        return chain(*accumulators)

    def extract_output(self, all_data):
        return [i for i in all_data]

class EmailSender(beam.DoFn):
    def __init__(self, recipients, key):
        self.recipients = recipients.split(',')
        self.key = key


    def _build_personalization(self, recipients):
        to_dict = []
        for email in recipients:
          to_dict.append(
              { "email" : email,
                "name"  : email
                })

        pers_dict = dict(to=to_dict)
        return [pers_dict]



    def process(self, element):
        logging.info('Attepmting to send emamil to:{}, using key:{}'.format(self.recipients, self.key))
        template = "<html><body>{}</body></html>"
        content = template.format(element)
        logging.info('Sending \n {}'.format(content))

        to_emails = []
        for recipient in self.recipients:
            to_emails.append((recipient, recipient.split('@')[0]))

        message = Mail(
            from_email=Email('gcp_cloud_mm@outlook.com'),
            to_emails=to_emails,
            subject='Market Stats',
            html_content=content)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        logging.info(response.status_code, response.body, response.headers)


def run_non_manufacturing_pmi(p):
    nmPmiDate = date(date.today().year, date.today().month, 1)
    return (p | 'startstart' >> beam.Create(['20210101'])
                    | 'pmi' >>   beam.ParDo(ParseNonManufacturingPMI())
                    | 'remap  pmi' >> beam.Map(lambda d: {'AS_OF_DATE' : nmPmiDate.strftime('%Y-%m-%d'), 'LABEL' : 'NON-MANUFACTURING-PMI', 'VALUE' : d['Last']})
            )

def run_consumer_sentiment_index(p):
    csPmiDate = date(date.today().year, date.today().month, 1)
    return (p | 'csstartstart' >> beam.Create(['20210101'])
                    | 'cs' >>   beam.ParDo(ParseConsumerSentimentIndex())
                    | 'remap  csi' >> beam.Map(lambda d: {'AS_OF_DATE' : csPmiDate.strftime('%Y-%m-%d'), 'LABEL' : 'CONSUMER_SENTIMENT_INDEX', 'VALUE' : d['Last']})
            )


def run_cramer_pipeline(p, fmpKey, numdays=5):
    return (p | 'cramer starter' >> beam.Create(['20240101'])
              | 'getting picks'  >> beam.FlatMap(lambda d: get_cramer_picks(fmpKey, numdays))
              )

def run_putcall_ratio(p):
    return (p | 'start putcall ratio' >> beam.Create(['20210101'])
            | 'putcall' >> beam.Map(lambda d: get_equity_putcall_ratio())
            | 'remap pcratio' >> beam.Map(
                lambda d: {'AS_OF_DATE': date.today().strftime('%Y-%m-%d'), 'LABEL': 'EQUITY_PUTCALL_RATIO', 'VALUE': str(d)})
            )


def run_manufacturing_pmi(p):
    manufPmiDate = date(date.today().year, date.today().month, 1)

    return (p | 'startstartnpmi' >> beam.Create(['20210101'])
                    | 'manifpmi' >>   beam.ParDo(ParseManufacturingPMI())
                    | 'manufremap  pmi' >> beam.Map(lambda d: {'AS_OF_DATE' : manufPmiDate.strftime('%Y-%m-%d'), 'LABEL' : 'MANUFACTURING-PMI', 'VALUE' : d['Last']})
            )

def run_economic_calendar(p, key):
    return (p | 'startcal' >> beam.Create(['20210101'])
                    | 'econcalendar' >>   beam.FlatMap(lambda d: get_economic_calendar(key))
                    | 'reMapping' >> beam.Map(lambda d: {'AS_OF_DATE' : d['date'],
                                                         'LABEL' : d['event'],
                                                         'VALUE' : f"Previous:{d['previous']},Estimate:{d['estimate']},Actual:{d.get('actual') or ''}"
                                                         }
                                              )

            )


def run_vix(p, key):
    return (p | 'start run_vix' >> beam.Create(['20210101'])
                    | 'vix' >>   beam.Map(lambda d:  get_vix(key))
                    | 'remap vix' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'VIX', 'VALUE' : str(d)})
            )

def run_junk_bond_demand(p, fredkey):
    return (p | 'start run_junk' >> beam.Create(['20210101'])
                    | 'junk' >>   beam.Map(lambda d:  get_junkbonddemand(fredkey))
                    | 'remap junk' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'JUNK_BOND_DEMAND', 'VALUE' : str(d)})
            )

def run_senate_disclosures(p, key):
    return (p | 'start run_sd' >> beam.Create(['20210101'])
              | 'run sendisclos' >> beam.FlatMap(lambda d : get_senate_disclosures(key))
            )

def run_fed_fund_rates(p):
    return (p | 'start run_ffr' >> beam.Create(['20210101'])
              | 'run ffrates' >> beam.Map(lambda d : get_latest_fed_fund_rates())
              | 'remap fr' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'FED_FUND_RATES', 'VALUE' : str(d)})
            )


def run_market_momentum(p, key, ticker='^GSPC'):
    return (p | f'start run_mm_{ticker}' >> beam.Create(['20210101'])
                    | f'mm_{ticker}' >>   beam.Map(lambda d:  get_market_momentum(key, ticker))
                    | f'remap mm_{ticker}' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'MARKET_MOMENTUM', 'VALUE' : str(d)})
            )

def run_growth_vs_value(p, key):
    return (p | 'start run_gv' >> beam.Create(['20210101'])
                    | 'gv' >>   beam.Map(lambda d: get_sector_rotation_indicator (key))
                    | 'remap mmgv' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'SECTOR ROTATION(GROWTH/VALUE)', 'VALUE' : str(d)})
            )

def run_cftc_spfutures(p, key):
    return (p | 'start_cftc' >> beam.Create(['20210101'])
                    | 'sptufutres' >>   beam.Map(lambda d:  get_cftc_spfutures(key))
                    | 'remap cftcspfutures' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'CFTC-SPFUTURES', 'VALUE' : str(d)})
            )

def run_mcclellan_pipeline(p, ticker):
    return (p
     | f'Start_{ticker}' >> beam.Create([ticker])
     | f'Get mmcl_{ticker}' >> beam.Map(get_mcclellan)
     )




def run_prev_dates_statistics(p) :
    # Need to amend the query to order by asofdate sc
    nysebqp = ( create_bigquery_ppln(p, 'NEW YORK STOCK EXCHANGE_MARKET BREADTH')
               #| 'map to tpl2' >> beam.Map(lambda d: ( d['AS_OF_DATE'], d['LABEL'], d['VALUE'] ))
    )
    
    return nysebqp


def run_prev_dates_statistics_cftc(p):
    # Need to amend the query to order by asofdate sc
    cftc_ppln = (create_bigquery_ppln_cftc(p)
               )

    return cftc_ppln

def run_prev_dates_statistics_manuf_pmi(p):
    # Need to amend the query to order by asofdate sc
    pmi_ppln = (create_bigquery_manufpmi_bq(p)
               )

    return pmi_ppln

def run_prev_dates_statistics_non_manuf_pmi(p):
    # Need to amend the query to order by asofdate sc
    npmi_ppln = (create_bigquery_nonmanuf_pmi_bq(p)
               )

    return npmi_ppln




def run_prev_dates_statistics_cftc(p):
    # Need to amend the query to order by asofdate sc
    cftc_ppln = (create_bigquery_ppln_cftc(p)
               )

    return cftc_ppln



def write_all_to_sink(results_to_write, sink):
    to_tpl = tuple(results_to_write)
    return (
            to_tpl
            | 'FlattenCombine all sink' >> beam.Flatten()
            | 'write all to sink' >> sink

    )

def run_newhigh_new_low(p, fmpKey):
    full_ticks = ','.join(get_all_us_stocks(fmpKey))

    return  (p
           | 'Start' >> beam.Create([full_ticks])
           | 'Get all List' >> beam.ParDo(NewHighNewLowLoader(fmpKey))
           | 'Mapping' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'),
                                              'LABEL' : 'NEW_HIGH_NEW_LOW',
                                              'VALUE' : f"{d['VALUE']}"
                                              })
           )

def run_advance_decline(p, exchange):
    return  (p
           | f'Start Advance Decline {exchange}' >> beam.Create([exchange])
           | f'calling  ad-{exchange}' >> beam.Map(lambda exc :get_advance_decline(exc))
           | f'remap {exchange}' >> beam.Map(
                lambda d: {'AS_OF_DATE': date.today().strftime('%Y-%m-%d'), 'LABEL': f'{exchange} ADVANCE/DECLINE',
                           'VALUE': str(d['VALUE'])})
           )



def run_shillers(p):
    return (p | 'shiller starter' >> beam.Create(['20240101'])
             | 'getting shillers' >> beam.FlatMap(lambda d: get_shiller_indexes())
             )

def run_sp500multiples(p):
    return (p | 'Start sp500' >> beam.Create(['shiller_pe_month', 'pe_month', 'earnings_growth_year'])
                     | 'Run Loader sp500' >> beam.ParDo(AsyncProcessSP500Multiples({}))
                     )

def parse_known_args(argv):
    """Parses args for the workflow."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--key')
    parser.add_argument('--fredkey')
    parser.add_argument('--sendgridkey')
    parser.add_argument('--recipients', default='mmistroni@gmail.com')
    return parser.parse_known_args(argv)

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    # Check  this https://medium.datadriveninvestor.com/markets-is-a-correction-coming-aa609fba3e34

    known_args, pipeline_args = parse_known_args(argv)
    pipeline_optionss = PipelineOptions(pipeline_args)
    pipeline_optionss.view_as(SetupOptions).save_main_session = save_main_session
    logging.info(f'fmp key:{known_args.key}')

    debugSink =  beam.Map(logging.info)


    with beam.Pipeline(options=pipeline_optionss) as p:

        iexapi_key = known_args.key
        fred_key = known_args.fredkey
        current_dt = datetime.now().strftime('%Y%m%d-%H%M')
        
        destination = 'gs://mm_dataflow_bucket/outputs/shareloader/{}_run_{}.csv'

        logging.info('====== Destination is :{}'.format(destination))
        logging.info('SendgridKey=={}'.format(known_args.sendgridkey))
        
        bq_sink = beam.io.WriteToBigQuery(
            bigquery.TableReference(
                projectId="datascience-projects",
                datasetId='gcp_shareloader',
                tableId='market_stats'),
            schema='AS_OF_DATE:STRING,LABEL:STRING,VALUE:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        cramer_sink = beam.io.WriteToBigQuery(
            bigquery.TableReference(
                projectId="datascience-projects",
                datasetId='gcp_shareloader',
                tableId='cramer_predictions'),
            schema='DATE:STRING,TICKER:STRING,RECOMMENDATION:STRING,PRICE:FLOAT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        senate_disclosures_sink = beam.io.WriteToBigQuery(
            bigquery.TableReference(
                projectId="datascience-projects",
                datasetId='gcp_shareloader',
                tableId='senate_disclosures'),
            schema='AS_OF_DATE:DATE,TICKER:STRING,DISCLOSURE:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        run_weekday = date.today().weekday()

        logging.info('Run pmi')
        
        non_manuf_pmi_res = run_non_manufacturing_pmi(p)

        manuf_pmi_res = run_manufacturing_pmi(p)

        consumer_res = run_consumer_sentiment_index(p)

        if run_weekday == 5:
            logging.info(f'Weekday for rundate is {run_weekday}')
            cftc = run_cftc_spfutures(p, iexapi_key)
            cftc | 'cftc to sink' >> bq_sink

        vix_res = run_vix(p, iexapi_key)

        mmomentum_res = run_market_momentum(p, iexapi_key)

        nasdaq_res = run_market_momentum(p, iexapi_key, 'QQQ')

        russell_res = run_market_momentum(p, iexapi_key, 'IWM')

        growth_vs_val_res = run_growth_vs_value(p, iexapi_key)

        senate_disc = run_senate_disclosures(p, iexapi_key)

        nysi_res = run_mcclellan_pipeline(p, '$NYSI')
        nymo_res = run_mcclellan_pipeline(p, '$NYMO')

        equity_pcratio = run_putcall_ratio(p)

        fed_funds = run_fed_fund_rates(p)

        econ_calendar = run_economic_calendar(p, iexapi_key)

        high_low = run_newhigh_new_low(p, iexapi_key)

        junk_bond = run_junk_bond_demand(p, fred_key)

        staticStart = (p | 'Create static start' >> beam.Create(
            [dict(AS_OF_DATE='------- ', LABEL='<b> THIS WEEK ECONOMIC CALENDAR</b>', VALUE='--------')])
                   )

        static1 = (p |'Create static1' >>  beam.Create([dict(AS_OF_DATE='------- ', LABEL='<b> TODAYS PERFORMANCE</b>', VALUE='--------')])
                 )
        
        static = (p | 'Create static 2' >> beam.Create([dict(AS_OF_DATE='------- ', LABEL='<b> LAST 5 DAYS PERFORMANCE</b>', VALUE='--------')])
                 )
        statistics = run_prev_dates_statistics(p)

        cftc_historical = run_prev_dates_statistics_cftc(p)

        pmi_hist = run_prev_dates_statistics_manuf_pmi(p)

        shillers = run_shillers(p)

        sp500_multi = run_sp500multiples(p)


        adv_decline_nyse = run_advance_decline(p , 'NYSE')
        adv_decline_nasd = run_advance_decline(p, 'NASDAQ')

        #non_pmi_hist = run_prev_dates_statistics_non_manuf_pmi(p)




        staticStart_key = staticStart | 'Add -2' >> beam.Map(lambda d: (-2, d))
        econCalendarKey = econ_calendar | 'Add -1' >> beam.Map(lambda d: (-1, d))
        static1_key = static1 | 'Add 0' >> beam.Map(lambda d: (0, d))
        pmi_key = non_manuf_pmi_res | 'Add 1' >> beam.Map(lambda d: (1, d))
        manuf_pmi_key = manuf_pmi_res | 'Add 2' >> beam.Map(lambda d: (2, d))

        vix_key = vix_res | 'Add 3' >> beam.Map(lambda d: (3, d))
        adv_decline_key_nys = adv_decline_nyse | 'add adv decl nyse' >> beam.Map(lambda d: (6, d))
        adv_decline_key_nas = adv_decline_nasd | 'add adv decl nas' >> beam.Map(lambda d: (7, d))
        highlow_key = high_low | 'add highlow' >> beam.Map(lambda d: (8, d))

        epcratio_key = equity_pcratio | 'Add 6' >> beam.Map(lambda d: (9, d))
        mm_key = mmomentum_res | 'Add mm' >> beam.Map(lambda d: (10, d))
        qqq_key = nasdaq_res | 'Add QQQ' >> beam.Map(lambda d: (11, d))
        rut_key = russell_res | 'Add rut' >> beam.Map(lambda d: (12, d))
        growth_vs_val_key = growth_vs_val_res | 'Add 14' >> beam.Map(lambda d: (13, d))

        nysi_key = nysi_res | 'Add nysi' >> beam.Map(lambda d: (15, d))
        nymo_key = nymo_res | 'Add nymo' >> beam.Map(lambda d: (16, d))

        junk_bond_key = junk_bond | 'add junnkbond' >> beam.Map(lambda d: (17, d))



        fed_funds_key = fed_funds | 'Add ff' >> beam.Map(lambda d: (22, d))
        cons_res_key = consumer_res | 'Add cres' >> beam.Map(lambda d: (23, d))
        shillers_key = shillers | 'add shillers' >> beam.Map(lambda d: (24, d))
        sp500_key = sp500_multi | 'add sp500 multi' >> beam.Map(lambda d: (26, d))

        sd_key = senate_disc | 'Add sd' >> beam.Map(lambda d: (34, d))

        static_key = static | 'Add 10' >> beam.Map(lambda d: (45, d))
        stats_key = statistics | 'Add 11' >> beam.Map(lambda d: (46, d))
        cftc_key = cftc_historical | 'Add 12' >> beam.Map(lambda d: (57, d))
        # we need a global combiner to write to sink
        pmi_hist_key = pmi_hist | 'Add 20' >> beam.Map(lambda d: (60, d))

        #non_manuf_pmi_hist_key = non_pmi_hist | 'Add 40' >> beSam.Map(lambda d: (40, d))

        final = (
                (staticStart_key, econCalendarKey, static1_key, pmi_key,
                    manuf_pmi_key,  epcratio_key, mm_key, qqq_key, rut_key,
                        nysi_key, nymo_key, junk_bond_key,adv_decline_key_nas,
                        adv_decline_key_nys,
                        shillers_key, highlow_key,
                        sp500_key,
                        cftc_key,  vix_key, sd_key, growth_vs_val_key,
                        fed_funds_key, cons_res_key,
                        static_key, stats_key,
                        pmi_hist_key,
                        #non_manuf_pmi_hist_key,
                        )
                | 'FlattenCombine all' >> beam.Flatten()
                | ' do A PARDO combner:' >> beam.CombineGlobally(MarketStatsCombineFn())
                | ' FlatMapping' >> beam.FlatMap(lambda x: x)
                | 'Combine' >> beam.CombineGlobally(lambda x: '<br><br>'.join(x))
                | 'SendEmail' >> beam.ParDo(EmailSender(known_args.recipients, known_args.sendgridkey))

        )

        final_sink_results = [
                 vix_res, mmomentum_res, growth_vs_val_res,
                 adv_decline_key_nys,
                 adv_decline_key_nas,
                 equity_pcratio, fed_funds, junk_bond
                ]

        # Writing everything to sink
        logging.info('Writing all to sink')

        destination = 'gs://mm_dataflow_bucket/outputs/marketstats_{}'.format(
            date.today().strftime('%Y-%m-%d %H:%M'))

        write_all_to_sink(final_sink_results, bq_sink)

        logging.info('----- Attepmting some Inner Joins....')

        #consumer_sentiment_res = run_consumer_sentiment_index(p)

        #consumer_sentiment_res | debugSink

        bq_pmi_res = get_latest_manufacturing_pmi_from_bq(p)

        coll1Mapped = manuf_pmi_res | 'Mapping PMI from Web ' >> beam.Map(lambda dictionary: (dictionary['LABEL'],
                                                                      dictionary))

        coll2Mapped = (bq_pmi_res | 'Mapping PMI from BQ' >> beam.Map(lambda dictionary: (dictionary['LABEL'],
                                                                                dictionary))
                       )
        left_joined = (
                coll1Mapped
                | 'PMI InnerJoiner: JoinValues' >> beam.ParDo(PMIJoinerFn(),
                                                          right_list=beam.pvalue.AsIter(coll2Mapped))
                | 'PMI Map to flat tpl' >> beam.Map(lambda tpl: tpl[1])

        )

        left_joined | 'PMI TO Debug Sink' >> debugSink
        left_joined | 'PMI TO BQ Sink' >> bq_sink

        bq_nmfpmi_res = get_latest_non_manufacturing_pmi_from_bq(p)

        nonMfPmiSourced = non_manuf_pmi_res | 'Mapping NMPMI from Web ' >> beam.Map(lambda dictionary: (dictionary['LABEL'],
                                                                                              dictionary))

        nonMfPmiMapped = (bq_nmfpmi_res | 'Mapping NMPMI from BQ' >> beam.Map(lambda dictionary: (dictionary['LABEL'],
                                                                                          dictionary))
                       )
        nm_left_joined = (
                nonMfPmiSourced
                | 'NMPMI InnerJoiner: JoinValues' >> beam.ParDo(PMIJoinerFn(),
                                                              right_list=beam.pvalue.AsIter(nonMfPmiMapped))
                | 'NMPMI Map to flat tpl' >> beam.Map(lambda tpl: tpl[1])

        )

        nm_left_joined | 'NPMI to sink' >> debugSink


        # Consumer sentiment index
        consumerSentimentmiSourced = consumer_res | 'Mapping consumer res from Web ' >> beam.Map(
            lambda dictionary: (dictionary['LABEL'],
                                dictionary))
        bq_consres_res = create_bigquery_pipeline(p, 'CONSUMER_SENTIMENT_INDEX')

        bqConsResMapped = (bq_consres_res | 'Mapping ConsRes from BQ' >> beam.Map(lambda dictionary: (dictionary['LABEL'],
                                                                                                  dictionary))
                          )
        cres_left_joined = (
                consumerSentimentmiSourced
                | 'ConsRes InnerJoiner: JoinValues' >> beam.ParDo(PMIJoinerFn(),
                                                                right_list=beam.pvalue.AsIter(bqConsResMapped))
                | 'ConsRes Map to flat tpl' >> beam.Map(lambda tpl: tpl[1])

        )

        cres_left_joined | 'CRES to sink' >> debugSink
        cres_left_joined | 'CRES to BQsink' >> bq_sink

        if run_weekday >= 5:
            cramer_result = run_cramer_pipeline(p, iexapi_key)

            debug_sink = beam.Map(logging.info)

            cramer_result | debug_sink
            cramer_result | cramer_sink

        logging.info('Writing senate disclosures to sink')

        (senate_disc | 'Remapping SD ' >> beam.Map(lambda d: dict(AS_OF_DATE=datetime.strptime(d['AS_OF_DATE'], '%Y-%m-%d').date(),
                                                    TICKER=d.get('VALUE', '').split('|')[0],
                                                    DISCLOSURE=d.get('VALUE', '').split('|')[1] if len(d.get('VALUE', '').split('|')) > 0 else
                                                    d.get('VALUE', '').split('|')[0]))
                      | 'To Senate Sink' >> senate_disclosures_sink)


