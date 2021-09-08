from __future__ import absolute_import

import argparse
import logging
import re

from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import re, requests
from datetime import datetime, date
from collections import OrderedDict
import requests
import pandas_datareader.data as dr
import logging
import apache_beam as beam
from datetime import date
import apache_beam.io.gcp.gcsfilesystem as gcs
from apache_beam.options.pipeline_options import PipelineOptions
from .superperf_metrics import get_all_data


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')


def get_universe_filter(input_dict):
    return (input_dict.get('marketCap', 0) > 300000000) and (input_dict.get('avgVolume', 0) > 200000) \
        and (input_dict.get('price', 0) > 10) and (input_dict.get('eps_growth_this_year', 0) > 0.2) \
            and (input_dict.get('eps_growth_next_year', 0) > 0) and (input_dict.get('eps_growth_qtr_over_qtr', 0) > 0.2)\
            and (input_dict.get('net_sales_qtr_over_qtr', 0) > 0.2) and (input_dict.get('returnOnEquity', 0) > 0) \
            and (input_dict.get('grossProfitMargin', 0) > 0) \
            and  (input_dict.get('price', 0) > input_dict.get('priceAvg20', 0))\
            and (input_dict.get('price', 0) > input_dict.get('priceAvg50', 0)) \
            and (input_dict.get('price', 0) > input_dict.get('priceAvg200', 0))

def write_to_bucket(lines, sink):
    return (
            lines | sink
    )

def load_all(source,fmpkey):
    return (source
              | beam.Map(lambda tpl: get_all_data(tpl[0], fmpkey))
            )
def filter_universe(data):
    return (data
             | 'Filtering' >> beam.Filter(get_universe_filter)
            )

def extract_data_pipeline(p, input_file):
    return (p
            | 'Reading Tickers' >> beam.io.textio.ReadFromText(input_file)
            | 'Converting to Tuple' >> beam.Map(lambda row: row.split(','))
            | 'Extracting only ticker and Industry' >> beam.Map(lambda item:(item[0], item[2]))
            | 'Sample. Filtering only few stocks' >> beam.Filter(lambda tpl: tpl[0] in ['AAPL', 'AMZN', 'MCD'])
            )

def find_canslim(p):
    pass

def find_leaf(p):
    pass

def find_stocks_under10m(p):
    pass

def find_stocks_alltime_high(p):
    pass

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    input_file = 'gs://mm_dataflow_bucket/inputs/shares_dataset.csv-00000-of-00001'
    destination = 'gs://mm_dataflow_bucket/outputs/superperformers_universe_{}'.format(date.today().strftime('%Y-%m-%d'))
    sink = beam.Map(lambda x: logging.info(x))#beam.io.WriteToText(destination, num_shards=1)
    pipeline_options = XyzOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        tickers = extract_data_pipeline(p, input_file)
        all_data = load_all(tickers, pipeline_options.key)
        write_to_bucket(all_data, sink)

        #universe = filter_universe(all_data)

