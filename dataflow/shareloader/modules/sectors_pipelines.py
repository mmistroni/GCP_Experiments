from __future__ import absolute_import

import logging
import apache_beam as beam
from collections import OrderedDict
from .sectors_utils import SectorsEmailSender, ETFHistoryCombineFn, get_sector_rankings, \
        get_finviz_performance
from .marketstats_utils import get_senate_disclosures

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
     | 'Fetch data 1' >> beam.Map(lambda tpl: get_sector_rankings(fmprepkey))
     #| 'Combine' >> beam.CombineGlobally(ETFHistoryCombineFn())
     )

def run_sector_loader_finviz(p):
    return (p | 'Starting fvz' >> beam.Create(['Start'])
     | 'Fetch data 2' >> beam.Map(lambda tpl: get_finviz_performance())
     )

def run_senate_disclosures(p, key):
    return (p | 'start run_sd' >> beam.Create(['20210101'])
              | 'run sendisclos' >> beam.Map(lambda d : get_senate_disclosures(key))
            )

def run_pipelines(p, fmpkey, recipients=''):
    #result = run_sector_loader_pipeline(p, fmpkey)
    return  run_sector_loader_finviz(p)

def run_sector_pipelines(p, known_args):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    debugSink = beam.Map(logging.info)
    finviz_result  = run_pipelines(p, known_args.key)
    finviz_result |'tosink' >> debugSink
    finviz_result | 'Generate Msg' >> beam.ParDo(SectorsEmailSender(known_args.recipients,
                                                                    known_args.sendgridkey))
