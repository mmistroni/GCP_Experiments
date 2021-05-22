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


def get_prices(ticker):
  try:
    logging.info('Fetching {}'.format(ticker))
    df = dr.get_data_yahoo(ticker, date(2016,1,1,), date(2021,4,1))[['Close']]
    df.reset_index(inplace=True)
    df['asofdate'] = df.Date.apply(lambda dt: dt.strftime('%Y%m%d'))
    df['ticker'] = ticker
    good = df.drop(columns=['Date'])
    return list(good.to_records(index=False))
  except Exception as e:
    print('could not fetch {}'.format(ticker))


def write_to_bucket(lines):
    bucket_destination = 'gs://mm_dataflow_bucket/outputs/shares_dataset_{}.csv'.format(datetime.now().strftime('%Y%m%d%H%M'))
    return (
            lines
            | 'Map to  String_' >> beam.Map(lambda lst: ','.join([str(i) for i in lst]))
            # cob, ticker, shares, increase, trans price, volume
            | 'WRITE TO BUCKET' >> beam.io.WriteToText(bucket_destination, header='close,ticker,asofdate',
                                                       num_shards=1)


    )

def run_my_pipeline(p, options=None):
    lines = (p
             | 'Getting Prices' >> beam.FlatMap(lambda symbol: get_prices(symbol))
             )
    return lines

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    with beam.Pipeline(options=PipelineOptions()) as p:
        input = p  | 'Get List of Tickers' >> beam.Create(['AAPL'])
        data = run_my_pipeline(input)
        write_to_bucket(data)

