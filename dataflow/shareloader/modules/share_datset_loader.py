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
import apache_beam.io.gcp.gcsfilesystem as gcs
from apache_beam.options.pipeline_options import PipelineOptions


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')
        
class DeleteOriginal(beam.DoFn):
  def __init__(self, gfs):
    self.gfs = gfs

  def process(self, file_metadata):
    if file_metadata.size_in_bytes == 0:
      self.gfs.delete([file_metadata.path])

class GetAllTickers(beam.DoFn):
  def __init__(self, fmprepkey):
      self.fmprepkey = fmprepkey
  
  def get_all_tradables(self):
    try:
        all_symbols = requests.get('https://financialmodelingprep.com/api/v3/available-traded/list?apikey={}'.format(self.fmpkey)).json()
        return [(d['symbol'], d['name']) for d in all_symbols]
    except Exception as e:
        logging.info('Exception:{}'.format(str(e)))
        return []
    

  def process(self, item):
    return self.get_all_tradables()

def write_to_bucket(lines, bucket_destination):
    return (
            lines
            | 'Map to  String_' >> beam.Map(lambda lst: ','.join([str(i) for i in lst]))
            # cob, ticker, shares, increase, trans price, volume
            | 'WRITE TO BUCKET' >> beam.io.WriteToText(bucket_destination, header='symbol,name, industry',
                                                       num_shards=1)


    )


def get_industry(ticker, key):
  try:
    profile = requests.get('https://financialmodelingprep.com/api/v3/profile/{}?apikey={}'.format(ticker.upper(), getfmpkeys())).json()
    print(profile)
    return profile[0]['industry']
  except Exception as e:
    print('Exceptoin:{}'.format(str(e)))
    return 'NA'


def run_my_pipeline(p, key):
    lines = (p
             | 'Getting All Tickers' >> beam.ParDo(GetAllTickers(key))
             | 'Mapping to Industry' >> beam.Map(lambda tpl: (tpl[0], tpl[1], get_industry(tpl[0], key)))
             )
    return lines

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    destination = 'gs://mm_dataflow_bucket/inputs/shares_dataset.csv'
    pipeline_options = XyzOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        input = p  | 'Deleting Original' >> beam.ParDo(DeleteOriginal(destination))
            
        data = run_my_pipeline(input, pipeline_options.fmprepkey)
        write_to_bucket(data, destination)

