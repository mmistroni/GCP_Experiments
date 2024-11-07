from __future__ import absolute_import

import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime, date
from shareloader.modules.marketstats_utils import MarketBreadthCombineFn, \
                            get_vix, ParseNonManufacturingPMI, get_all_us_stocks2,\
                            get_all_prices_for_date, InnerJoinerFn, create_bigquery_ppln,\
                            ParseManufacturingPMI,get_economic_calendar, get_equity_putcall_ratio,\
                            get_cftc_spfutures, create_bigquery_ppln_cftc, get_market_momentum, \
                            get_senate_disclosures, create_bigquery_manufpmi_bq, create_bigquery_nonmanuf_pmi_bq,\
                            get_sector_rotation_indicator, get_latest_fed_fund_rates,\
                            get_latest_manufacturing_pmi_from_bq, PMIJoinerFn, ParseConsumerSentimentIndex,\
                            get_latest_non_manufacturing_pmi_from_bq, create_bigquery_pipeline,\
                            get_mcclellan, get_all_us_stocks, get_junkbonddemand, \
                            get_cramer_picks, NewHighNewLowLoader



class AbcOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--key')
        parser.add_argument('--fredkey')
        parser.add_argument('--sendgridkey')
        parser.add_argument('--recipients', default='mmistroni@gmail.com')



def run_vix(p, key):
    return (p | 'start run_vix' >> beam.Create(['20210101'])
                    | 'vix' >>   beam.Map(lambda d:  get_vix(key))
                    | 'remap vix' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'VIX', 'VALUE' : str(d)})
            )


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""


    pipeline_options = AbcOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    debugSink = beam.Map(logging.info)

    with beam.Pipeline(options=pipeline_options) as p:
        fmp_key = pipeline_options.key
        logging.info(pipeline_options.get_all_options())
        destination = 'gs://mm_dataflow_bucket/outputs/shareloader/{}_run_{}.csv'

        logging.info('====== Destination is :{}'.format(destination))
        logging.info('SendgridKey=={}'.format(pipeline_options.sendgridkey))

        p = run_vix(p, fmp_key)

        p | 'Writing to sink' >> debugSink



