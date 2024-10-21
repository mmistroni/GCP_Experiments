from __future__ import absolute_import

import logging
from itertools import chain

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime, date
from .marketstats_utils import MarketBreadthCombineFn, get_vix

import requests



class AbcOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--key')
        parser.add_argument('--fredkey')
        parser.add_argument('--sendgridkey')
        parser.add_argument('--recipients', default='mmistroni@gmail.com')


def get_vix(key):
    try:
        base_url = 'https://financialmodelingprep.com/api/v3/quote-short/{}?apikey={}'.format('^VIX', key)
        logging.info('Url is:{}'.format(base_url))
        return requests.get(base_url).json()[0]['price']
    except Exception as e:
        logging.info(f'Exception in getting vix:{str(e)}')
        return 0.0



def run_vix(p, key):
    return (p | 'start run_vix' >> beam.Create(['20210101'])
                    | 'vix' >>   beam.Map(lambda d:  get_vix(key))
                    | 'remap vix' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'VIX', 'VALUE' : str(d)})
            )
def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    # Check  this https://medium.datadriveninvestor.com/markets-is-a-correction-coming-aa609fba3e34


    pipeline_options = AbcOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    debugSink =  beam.Map(logging.info)


    with beam.Pipeline(options=pipeline_options) as p:

        fmp_key = pipeline_options.key
        fred_key = pipeline_options.fredkey
        logging.info(pipeline_options.get_all_options())
        current_dt = datetime.now().strftime('%Y%m%d-%H%M')
        
        destination = 'gs://mm_dataflow_bucket/outputs/shareloader/{}_run_{}.csv'

        logging.info('====== Destination is :{}'.format(destination))
        logging.info('SendgridKey=={}'.format(pipeline_options.sendgridkey))

        debugSink = beam.Map(logging.info)

        p = run_vix(p, fmp_key )

        p | 'Writing to sink' >> debugSink

        

