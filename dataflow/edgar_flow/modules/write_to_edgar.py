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
from apache_beam.options.value_provider import StaticValueProvider
from itertools import groupby
from .edgar_utils import ReadRemote, ParseForm13F, cusip_to_ticker, get_company_stats,\
                                        EdgarEmailSender
from apache_beam.io import WriteToText
from apache_beam.io.textio import ReadAllFromText
import urllib
from collections import defaultdict
from datetime import date, datetime
from itertools import groupby
from apache_beam.io.gcp.internal.clients import bigquery
import requests
import os

test_bucket = 'gs://mm_dataflow_bucket/'
form_type = '13F-HR'
filename = '{}_{}'.format(form_type, datetime.now().strftime('%Y$m%d-%H%M'))

GC_PROJECT = 'datascience-projects'
STAGING_BUCKET = 'gs://mm_dataflow_bucket/staging'
TEMP_BUCKET = 'gs://mm_dataflow_bucket/temp'
TEMPLATE_BUCKET = 'gs://mm_dataflow_bucket/templates'


### BIG QUERY CONFIGS
## BIG QUERY SCHEMA

def get_edgar_table_schema():
  edgar_table_schema = 'EDGAR_YEAR:STRING,COB:STRING,CUSIP:STRING,COUNT:INTEGER,TICKER:STRING,INDUSTRY:STRING,BETA:STRING,DCF:STRING'
  return edgar_table_schema

def get_edgar_table_spec():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='form_13hf_data_enhanced')


def map_to_year(path, ppln_year):
    return path.format(ppln_year)


class EdgarOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--year', type=str)
        parser.add_argument('--fmprepkey')
        parser.add_argument('--sendgridkey')


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  dirpath = os.getcwd()
  logging.info("current directory is : " + dirpath)


  known_args, pipeline_args = parser.parse_known_args(argv)

  po = PipelineOptions()


  pipeline_options = po.view_as(EdgarOptions)

  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  p4 = beam.Pipeline(options=po)
  destination = 'gs://mm_dataflow_bucket/outputs/edgar_quarterly_run_withindustry-{}.csv'.format(datetime.now().strftime('%Y-%m-%d-%H%M'))

  lines = (
       p4
       #| 'generate master url' >>beam.Create(['https://www.sec.gov/Archives/edgar/full-index/2019/QTR1/master.idx'])
       | 'Sampling Data' >> beam.Create([
                                         'https://www.sec.gov/Archives/edgar/full-index/{}/QTR1/master.idx',
                                         #'https://www.sec.gov/Archives/edgar/full-index/{}/QTR2/master.idx',
                                         #'https://www.sec.gov/Archives/edgar/full-index/{}/QTR3/master.idx',
                                         #'https://www.sec.gov/Archives/edgar/full-index/{}/QTR4/master.idx'
                      ])
       | 'Map to year'  >> beam.Map(lambda epath: map_to_year(epath, pipeline_options.year.get()))
       | 'readFromText' >> beam.ParDo(ReadRemote())
       | 'map to Str'   >> beam.Map(lambda line:str(line))
       | 'Filter only form 13HF' >> beam.Filter(lambda row: len(row.split('|')) > 4 and form_type in row.split('|')[2])
       | 'Generating Proper #file path' >> beam.Map(lambda row: '{}/{}'.format('https://www.sec.gov/Archives', row.split('|')[4]))
       | 'replacing eol' >> beam.Map(lambda p: p[0:p.find('\\n')])
       | 'parsing edgar filing' >> beam.ParDo(ParseForm13F())
       | 'Combining similar' >> beam.combiners.Count.PerElement()
       | 'Groupring' >> beam.MapTuple(lambda word, count: (word, count))
       | 'Adding Cusip' >> beam.MapTuple(lambda word, count: (word, cusip_to_ticker(word), count))
       |'Fetching Statistics and Mapping to BQ' >> beam.Map(lambda tpl: get_company_stats(tpl, pipeline_options.fmprepkey ))

  )
  write_to_bucket = (
          lines
          | 'Map to String' >> beam.Map(
                    lambda tpl: '{},{},{},{}'.format(tpl['COB'],
                                                     tpl['CUSIP'], tpl['TICKER'],
                                                     tpl['COUNT'],
                                                     tpl['INDUSTRY'],
                                                     tpl['BETA']
                                                     ))
          | 'WRITE TO BUCKET' >> beam.io.WriteToText(destination, header='COB,CUSIP,TICKER,COUNT,INDUSTRY,BETAs',
                                                num_shards=1)
        )
  write_to_bigquery = (
          lines
          | 'Map to Another Dict' >> beam.Map(lambda tpl: dict(EDGAR_year=pipeline_options.year.get(),
                                                            COB=tpl['COB'],
                                                             CUSIP=tpl['CUSIP'],
                                                             TICKER=tpl['TICKER'],
                                                             COUNT=tpl['COUNT'],
                                                             INDUSTRY=tpl['INDUSTRY'],
                                                             BETA=tpl['BETA'],
                                                             DCF=tpl['DCF']),
                                                             )
          | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                      get_edgar_table_spec(),
                      schema=get_edgar_table_schema(),
                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
  )
  send_notification = (
          write_to_bucket
          | 'SendEmail' >> beam.ParDo(EdgarEmailSender('mmistroni@gmail.com', pipeline_options.sendgridkey, destination ))

  )

  p4.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
