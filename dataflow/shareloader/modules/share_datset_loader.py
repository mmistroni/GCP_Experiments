from __future__ import absolute_import

import argparse
import logging
import re

from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions, DebugOptions
import re, requests
from datetime import datetime, date
from collections import OrderedDict
import requests
import pandas_datareader.data as dr
import logging
import apache_beam as beam
import apache_beam.io.gcp.gcsfilesystem as gcs
from apache_beam.options.pipeline_options import PipelineOptions
import re

class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')
		
class GetAllTickers(beam.DoFn):
    def __init__(self, fmprepkey):
        self.fmprepkey = fmprepkey
        logging.info('Initialized')

    def _is_valid(self, d):
        stock_name = d['name']
        exchange = d['exchangeShortName']
        symbol = d['symbol']


        return (stock_name is not None and stock_name.find('ETF') < 0 \
                    and stock_name.find('ETNF') < 0  and stock_name.find('Fund') <0) \
                    and stock_name.find('ProShares') < 0 \
                    and (bool(exchange)) \
                    and   (exchange.lower().find('nasdaq') >= 0 or  exchange.lower() == 'nyse') \
                    and symbol.find('.') < 0

    def get_all_tradables(self):
        all_symbols = requests.get('https://financialmodelingprep.com/api/v3/stock/list?apikey={}'.format(self.fmprepkey)).json()
        return   [(d['symbol'], re.sub('[^\w\s]', '', d['name']), d['exchange']) for d in all_symbols if self._is_valid(d)]

    def process(self, item):
        tradables = self.get_all_tradables()
        return tradables


def write_to_bucket(lines, sink):
	return (
			lines | sink
	)

def get_industry(ticker, key):
    try:
        profile = requests.get('https://financialmodelingprep.com/api/v3/profile/{}?apikey={}'.format(ticker.upper(), key)).json()
        ind = profile[0]['industry']
        return re.sub('[^\w\s]', '', ind)  if ind else ''
    except Exception as e:
        print('Exceptoin:{}'.format(str(e)))
        return ''

class DeleteOriginal(beam.DoFn):
    def __init__(self, gfs):
        self.gfs = gfs

    def process(self, file_path):
        logging.info('Deleting:{}'.format(file_path))
        self.gfs.delete([file_path])
        return ['foobar']


def run_my_pipeline(p, key):
	return (p
			 | 'Getting All Tickers' >> beam.ParDo(GetAllTickers(key))
             | 'Mapping to Industry' >> beam.Map(lambda tpl: (tpl[0], tpl[1], get_industry(tpl[0], key)))
             | 'Adding asOfDate'     >> beam.Map(lambda tpl: (tpl[0], tpl[1], tpl[2], date.today().strftime('%Y%m%d')))
             | 'Filtering out None and blankos' >> beam.Filter(lambda t : all(t))
             | 'Mapping to String'  >> beam.Map(lambda tpl: ','.join(tpl))
			 )


def run_delete_pipeline(p, file_pattern, gfs):
	logging.info('About to delete files with pattern:{}'.format(file_pattern))
	lines = (p
			 | 'Creating File' >> beam.Create([file_pattern])
			 | 'And Now Deleting...' >> beam.Map(logging.info)
			 )
	return lines



def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    destination = 'gs://mm_dataflow_bucket/inputs/shares_dataset.csv'
    sink = beam.io.WriteToText(destination,num_shards=1)
    pipeline_options = XyzOptions()

    timeout_secs = 16200
    experiment_value = f"max_workflow_runtime_walltime_seconds={timeout_secs}"

    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(DebugOptions).add_experiment(experiment_value)

    gfs = gcs.GCSFileSystem(pipeline_options)
    pattern = 'gs://mm_dataflow_bucket/inputs/shares_dataset*'
    with beam.Pipeline(options=pipeline_options) as p:
        result = run_delete_pipeline(p, pattern, gfs)
        tickers = run_my_pipeline(result, pipeline_options.fmprepkey)
        write_to_bucket(tickers, sink)