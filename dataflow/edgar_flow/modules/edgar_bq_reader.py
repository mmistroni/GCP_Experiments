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
            find_current_year, EdgarCombineFn
from apache_beam.io import WriteToText
from apache_beam.io.textio import ReadAllFromText
import urllib
from collections import defaultdict
from datetime import date, datetime
from itertools import groupby
from pandas.tseries.offsets import BDay
import requests
import os
from apache_beam.io.gcp.internal.clients import bigquery


def get_edgar_table_schema():
  edgar_table_schema = 'COB:STRING, CUSIP:STRING, COUNT:INTEGER, TICKER:STRING'
  return edgar_table_schema

def get_edgar_table_spec():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='form_13hf_data')

class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key')

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()

  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = XyzOptions()
  pipeline_options.view_as(SetupOptions).save_main_session = True

  p4 = beam.Pipeline(options=pipeline_options)
  current_date = date.today() - BDay(1)

  lines = (
       p4
       | 'ReadTable' >> beam.io.Read(beam.io.BigQuerySource(get_edgar_table_spec()))
       | 'Filter only form 13HF' >> beam.Filter(lambda row: row['cob_date'] == '2020-02-12')
       |'sampling lines' >> beam.transforms.combiners.Sample.FixedSizeGlobally(10)
       | 'Print out' >> beam.Map(print)

  )
  p4.run()
  return


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
