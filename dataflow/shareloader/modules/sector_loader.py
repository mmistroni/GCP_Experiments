from __future__ import absolute_import

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from collections import OrderedDict
from datetime import datetime, date
from .sectors_utils import SectorsEmailSender, ETFHistoryCombineFn, get_sector_rankings, \
        get_finviz_performance
from .marketstats_utils import get_senate_disclosures
import argparse

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



#https://www.tradingview.com/chart/AAPL/pmHMR643-Investors-Holy-Grail-The-Business-Economic-Cycle/?utm_source=Weekly&utm_medium=email&utm_campaign=TradingView+Weekly+188+%28EN%29
def run_sector_loader_pipeline(p, fmprepkey):
    return (p | 'Starting st' >> beam.Create(['Start'])
     | 'Fetch data' >> beam.Map(lambda tpl: get_sector_rankings(fmprepkey))
     #| 'Combine' >> beam.CombineGlobally(ETFHistoryCombineFn())
     )

def run_sector_loader_finviz(p):
    return (p | 'Starting fvz' >> beam.Create(['Start'])
     | 'Fetch data' >> beam.Map(lambda tpl: get_finviz_performance())
     )




def run_senate_disclosures(p, key):
    return (p | 'start run_sd' >> beam.Create(['20210101'])
              | 'run sendisclos' >> beam.Map(lambda d : get_senate_disclosures(key))
              | ' log out' >> beam.Map(logging.info)
            )

def parse_known_args(argv):
    """Parses args for the workflow."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--recipients')
    parser.add_argument('--key')
    parser.add_argument('--sendgridkey')
    return parser.parse_known_args(argv)


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    known_args, pipeline_args = parse_known_args(argv)
    pipeline_optionss = PipelineOptions(pipeline_args)
    pipeline_optionss.view_as(SetupOptions).save_main_session = save_main_session
    
    debugSink = beam.Map(logging.info)

    with beam.Pipeline(options=pipeline_optionss) as p:
        result = run_sector_loader_pipeline(p, known_args.key)
        finviz_result = run_sector_loader_finviz(p)
        finviz_result | 'debug sink' >> debugSink
        result | 'Mapping to String' >> beam.Map(logging.info)
        finviz_result | 'Generate Msg' >> beam.ParDo(SectorsEmailSender(known_args.recipients,
                                                                 known_args.sendgridkey))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()