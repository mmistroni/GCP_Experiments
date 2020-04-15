import apache_beam as beam
import argparse
import logging
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from itertools import groupby
from .edgar_utils import ReadRemote, ParseForm13F, cusip_to_ticker, \
            find_current_year, EmailSender
from apache_beam.io import WriteToText
from apache_beam.io.textio import ReadAllFromText
import urllib
from collections import defaultdict
from datetime import date, datetime
from itertools import groupby
from apache_beam.io.gcp.internal.clients import bigquery
import requests
import os

bucket_destination = 'gs://mm_dataflow_bucket/outputs/daily/edgar_{}.csv'
form_type = '13F-HR'

EDGAR_URL = 'https://www.sec.gov/Archives/edgar/daily-index/{year}/{quarter}/master.{current}.idx'

def find_current_quarter(current_date):
    quarter_dictionary = {
        "QTR1": [1, 2, 3],
        "QTR2": [4, 5, 6],
        "QTR3": [7, 8, 9],
        "QTR4": [10, 11, 12]
    }
    current_month = current_date.month
    print('Fetching quarter for month:{}'.format(current_month))
    return [key for key, v in quarter_dictionary.items() if current_month in v][0]

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()

  known_args, pipeline_args = parser.parse_known_args(argv)

  p4 = beam.Pipeline(options=PipelineOptions())
  current_date = date(2020,4,14)

  current_quarter = find_current_quarter(current_date)
  current_year = find_current_year(current_date)

  master_idx_url = EDGAR_URL.format(quarter=current_quarter, year=current_year,
                                    current=current_date.strftime('%Y%m%d'))
  logging.info('Extracting data from:{}'.format(master_idx_url))
  destination =   bucket_destination.format(current_date.strftime('%Y%m%d'))
  logging.info('Writing to:{}'.format(destination))

  lines = (
       p4
       | 'Sampling Data' >> beam.Create([master_idx_url])
       | 'readFromText' >> beam.ParDo(ReadRemote())
       | 'map to Str'   >> beam.Map(lambda line:str(line))
       | 'Filter only form 13HF' >> beam.Filter(lambda row: len(row.split('|')) > 4 and form_type in row.split('|')[2])
       | 'Generating Proper file path' >> beam.Map(lambda row: '{}/{}'.format('https://www.sec.gov/Archives', row.split('|')[4]))
       | 'replacing eol' >> beam.Map(lambda p: p[0:p.find('\\n')])
       | 'parsing edgar filing' >> beam.ParDo(ParseForm13F())
       | 'Combining similar' >> beam.combiners.Count.PerElement()
       | 'Groupring' >> beam.MapTuple(lambda word, count: (word, count))
       | 'Adding Cusip' >> beam.MapTuple(lambda word, count: [word, cusip_to_ticker(word), str(count)])
       | 'Writing to CSV' >> beam.Map(lambda lst: ','.join(lst))
       | 'WRITE TO BUCKET' >> beam.io.WriteToText(destination, num_shards=1)

  )
  p4.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
