import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from collections import OrderedDict
from datetime import datetime, date
from .sectors_utils import SectorsEmailSender, ETFHistoryCombineFn, get_sector_rankings, \
        get_finviz_performance
from .marketstats_utils import get_senate_disclosures
import argparse
from .sectors_pipelines import run_sector_pipelines


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

    with beam.Pipeline(options=pipeline_optionss) as p:
        run_sector_pipelines(p, known_args)