from __future__ import absolute_import

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from collections import OrderedDict
from datetime import datetime, date
from .sectors_utils import SectorsEmailSender, ETFHistoryCombineFn, fetch_performance
from .marketstats_utils import get_senate_disclosures
import requests

sectorsETF = OrderedDict ({
            'Technology' : 'XLK',
            'Health Care': 'XLV',
            'Financials' : 'XLF',
            'Real Estate': 'SCHH',
            'Energy'     : 'XLE',
            'Materials'  : 'XLB',
            'Consumer Discretionary' : 'XLY',
            'Industrials': 'VIS',
            'Utilities': 'VPU',
            'Consumer Staples' : 'XLP',
            'Telecommunications':'XLC',
            'S&P 500' : '^GSPC'
        })


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key')
        parser.add_argument('--sendgridkey')


def run_my_pipeline(p, fmprepkey):
    return (p | 'Starting' >> beam.Create([tpl for tpl in sectorsETF.items()])
     | 'Fetch data' >> beam.Map(lambda tpl: fetch_performance(tpl[0], tpl[1], fmprepkey))
     | 'Combine' >> beam.CombineGlobally(ETFHistoryCombineFn())
     )

def run_senate_disclosures(p, key):
    return (p | 'start run_sd' >> beam.Create(['20210101'])
              | 'run sendisclos' >> beam.Map(lambda d : get_senate_disclosures(key))
              | ' log out' >> beam.Map(logging.info)
            )






def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    logging.info(pipeline_options.get_all_options())

    with beam.Pipeline(options=pipeline_options) as p:
        result = run_my_pipeline(p, pipeline_options.fmprepkey)

        result2 = run_senate_disclosures(p, pipeline_options.key)

        result2 | 'Mapping to String11' >> beam.Map(logging.info)

        result | 'Mapping to String' >> beam.Map(logging.info)


        result | 'Generate Msg' >> beam.ParDo(SectorsEmailSender(pipeline_options.recipients,
                                                                 pipeline_options.sendgridkey))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()