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
from apache_beam.io.gcp.internal.clients import bigquery


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
            )

def canslim_filter(input_dict):
    return (input_dict['avgVolume'] > 200000) and (input_dict['eps_growth_this_year'] > 0.2) and (input_dict['eps_growth_next_year'] > 0.2) \
    and (input_dict['eps_growth_qtr_over_qtr'] > 0.2) and (input_dict['net_sales_qtr_over_qtr'] > 0.2) \
    and (input_dict['eps_growth_past_5yrs'] > 0.2) and (input_dict['returnOnEquity'] > 0) \
    and (input_dict['grossProfitMargin'] > 0) and (input_dict['institutionalHoldingsPercentage'] > 0.3) \
    and (input_dict['price'] > input_dict['priceAvg20']) and (input_dict['price'] > input_dict['priceAvg50']) \
    and (input_dict['price'] > input_dict['priceAvg200']) and (input_dict['sharesOutstanding'] > 50000000)

def stocks_under_10m_filter(input_dict):
    return (input_dict['marketCap'] < 10000000000) and (input_dict['avgVolume'] > 100000) \
                            and (input_dict['eps_growth_this_year'] > 0) and (input_dict['eps_growth_next_year'] > 0.25) \
                            and (input_dict['eps_growth_qtr_over_qtr'] > 0.2) and  (input_dict['net_sales_qtr_over_qtr'] > 0.25) \
                            and (input_dict['returnOnEquity'] > 0.15) and (input_dict['price'] > input_dict['priceAvg200'])

def new_high_filter(input_dict):
    return (input_dict['eps_growth_this_year'] > 0) and (input_dict['eps_growth_next_year'] > 0) \
                    and (input_dict['eps_growth_qtr_over_qtr'] > 0) and  (input_dict['net_sales_qtr_over_qtr'] > 0) \
                    and (input_dict['returnOnEquity'] > 0) and (input_dict['price'] > input_dict['priceAvg20']) \
                    and (input_dict['price'] > input_dict['priceAvg50']) \
                    and (input_dict['price'] > input_dict['priceAvg200']) and (input_dict['change'] > 0) \
                    and (input_dict['changeFromOpen'] > 0) and (input_dict['price'] >= input_dict['allTimeHigh'])

def find_leaf(p):
    pass

def find_stocks_under10m(p):
    pass

def find_stocks_alltime_high(p):
    pass

def write_to_bigquery(p, bq_sink, status):
    return (p | 'Mapping Tuple' >> beam.Map(lambda d: (datetime.today().strftime('%Y-%m-%d'), d['ticker'], status))
              | 'Mapping to BQ Dict' >> beam.Map(lambda tpl: dict(AS_OF_DATE=tpl[0], TICKER=tpl[1], STATUS=tpl[2]))
              | bq_sink 
              )


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    input_file = 'gs://mm_dataflow_bucket/inputs/shares_dataset.csv-00000-of-00001'
    destination = 'gs://mm_dataflow_bucket/outputs/superperformers_universe_{}'.format(date.today().strftime('%Y-%m-%d'))
    sink = beam.io.WriteToText(destination, num_shards=1)
    bq_sink = beam.io.WriteToBigQuery(
             bigquery.TableReference(
                projectId="datascience-projects",
                datasetId='gcp_shareloader',
                tableId='mm_stock_picks'),
            schema='AS_OF_DATE:STRING,TICKER:STRING,STATUS:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    pipeline_options = XyzOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        tickers = extract_data_pipeline(p, input_file)
        all_data = load_all(tickers, pipeline_options.fmprepkey)
        filtered = filter_universe(all_data)
        write_to_bucket(filtered, sink)
        write_to_bigquery(filtered, bq_sink, 'UNIVERSE')

        #universe = filter_universe(all_data)

