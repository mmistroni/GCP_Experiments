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
from edgar_utils import ReadRemote, ReadAllFromText, ReadFromText, ParseForm13F, cusip_to_ticker
from apache_beam.io import WriteToText
from apache_beam.io.textio import ReadAllFromText
import urllib
from collections import defaultdict
from datetime import date, datetime
from itertools import groupby
from apache_beam.io.gcp.internal.clients import bigquery
import requests

p4 = beam.Pipeline()
test_bucket = 'gs://mm_dataflow_bucket/'
form_type = '13F-HR'
filename = '{}_{}'.format(form_type, datetime.now().strftime('%Y$m%d-%H%M'))

### BIG QUERY CONFIGS
## BIG QUERY SCHEMA


class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument('--year',
                                         dest='input',
                                         default='2019',
                                         help='Input year to process')
      parser.add_value_provider_argument('--output',
                                         dest='output',
                                         default='gs://mm_dataflow_bucket/outputs',
                                         help='Input year to process')

      parser.add_value_provider_argument(
          '--runner',
          default='DataFlowRunner',
          help='Runner to use')
      parser.add_value_provider_argument(
          '--template_location',
          default='gs: //mm_dataflow_bucket/templates/edgar_df_template',
          help='Template Location')
      parser.add_value_provider_argument(
          '--staging_location',
          default='gs: //mm_dataflow_bucket/staging',
          help='Staging Location')


def get_edgar_table_schema():
  edgar_table_schema = 'COB:STRING, CUSIP:STRING, COUNT:INTEGER, TICKER:STRING'
  return edgar_table_schema

def get_edgar_table_spec():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='form_13hf_data')

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()

  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)

  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  myoptions = pipeline_options.view_as(MyOptions)

  p4 = beam.Pipeline()

  lines = (
       p4
       #| 'generate master url' >>beam.Create(['https://www.sec.gov/Archives/edgar/full-index/2019/QTR1/master.idx'])
       | 'Sampling Data' >> beam.Create(['https://www.sec.gov/Archives/edgar/full-index/2019/QTR1/master.idx',
                      #'https://www.sec.gov/Archives/edgar/full-index/2019/QTR2/master.idx'
                      ])
       | 'readFromText' >> beam.ParDo(ReadRemote())
       | 'map to Str'   >> beam.Map(lambda line:str(line))
       | 'Filter only form 13HF' >> beam.Filter(lambda row: len(row.split('|')) > 4 and form_type in row.split('|')[2])
       | 'Generating Proper file path' >> beam.Map(lambda row: '{}/{}'.format('https://www.sec.gov/Archives', row.split('|')[4]))
       | 'replacing eol' >> beam.Map(lambda p: p[0:p.find('\\n')])
       | 'sampling lines' >> beam.transforms.combiners.Sample.FixedSizeGlobally(10)
       | 'flat Mapping' >> beam.Map(lambda elements: elements[0])
       | 'parsing edgar filing' >> beam.ParDo(ParseForm13F())
       | 'Combining similar' >> beam.combiners.Count.PerElement()
       | 'Groupring' >> beam.MapTuple(lambda word, count: (word, count))
       #| 'sampling again' >> beam.transforms.combiners.Sample.FixedSizeGlobally(20)
       | 'Adding Cusip' >> beam.MapTuple(lambda word, count: (word, cusip_to_ticker(word), count))
       #| 'Filtering' >> beam.Filter(lambda tpl: tpl[1] > 300)
       | 'Creating BigQuery Data' >> beam.MapTuple(lambda word, ticker, count: dict(COB=date.today().strftime('%Y-%m-%d'), CUSIP=word, TICKER=ticker,COUNT=count))
       | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                                              get_edgar_table_spec(),
                                              schema=get_edgar_table_schema(),
                                              write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

  )
  p4.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

