import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from modules.edgar_utils import ReadRemote, ParseForm13F, cusip_to_ticker
from datetime import date, datetime
from apache_beam.io.gcp.internal.clients import bigquery
from modules.edgar_daily import combine_data, filter_form_13hf
import requests
import os
from modules.price_utils import get_current_price

test_bucket = 'gs://mm_dataflow_bucket/'
form_type = '13F-HR'
filename = '{}_{}'.format(form_type, datetime.now().strftime('%Y$m%d-%H%M'))

EDGAR_URL = 'https://www.sec.gov/Archives/edgar/full-index/{year}/{quarter}/master.idx'

### BIG QUERY CONFIGS
## BIG QUERY SCHEMA

# CRON EXPRESSION

def find_current_quarter(current_date):
    quarter_dictionary = {
        "Q1": [1, 2, 3],
        "Q2": [4, 5, 6],
        "Q3": [7, 8, 9],
        "Q4": [10, 11, 12]
    }
    current_month = current_date.month
    prev_month = current_month - 1 if current_month > 1 else current_month % 12
    print('Fetching quarter for month:{}'.format(prev_month))
    return [key for key, v in quarter_dictionary.items() if prev_month in v][0]

def find_current_year(current_date):
    quarter_dictionary = {
        "Q1": [1, 2, 3],
        "Q2": [4, 5, 6],
        "Q3": [7, 8, 9],
        "Q4": [10, 11, 12]
    }
    current_month = current_date.month
    edgar_year = current_date.year if current_month > 1 else current_date.year -1
    logging.info('Year to use is{}'.format(edgar_year))
    return edgar_year

def write_to_bigquery(lines):
    big_query = (
            lines
            |  'Add Current Price '  >> beam.Map(lambda tpl: (tpl[0], tpl[1], tpl[2], tpl[3],
                                                              tpl[4], 0.0))

            | 'Map to BQ Compatible Dict' >> beam.Map(lambda tpl: dict(COB=date.today().strftime('%Y-%m-%d'),
                                                                       PERIODOFREPORT=tpl[1],
                                                                       CUSIP=tpl[2],
                                                                       TICKER=tpl[3],
                                                                       COUNT=tpl[4],
                                                                       PRICE=tpl[5]))
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId="datascience-projects",
            datasetId='gcp_edgar',
            tableId='form_13hf_daily_quarterly'),
        schema='COB:STRING,PERIODOFREPORT:STRING,CUSIP:STRING,COUNT:INTEGER,TICKER:STRING,PRICE:FLOAT',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  dirpath = os.getcwd()
  print("current directory is : " + dirpath)

  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(options=PipelineOptions()) as p4:
      lines = (
           p4
           #| 'generate master url' >>beam.Create(['https://www.sec.gov/Archives/edgar/full-index/2019/QTR1/master.idx'])
           | 'Sampling Data' >> beam.Create([#'https://www.sec.gov/Archives/edgar/full-index/2019/QTR1/master.idx',
                                             #'https://www.sec.gov/Archives/edgar/full-index/2019/QTR2/master.idx',
                                             'https://www.sec.gov/Archives/edgar/full-index/2020/QTR3/master.idx',
                                             #'https://www.sec.gov/Archives/edgar/full-index/2020/QTR4/master.idx'
                          ])
           | 'readFromText' >> beam.ParDo(ReadRemote())
           | 'map to Str'   >> beam.Map(lambda line:str(line))
      )
      enhanced_data = filter_form_13hf(lines)
      logging.info('Next step')
      form113 = combine_data(enhanced_data)
      write_to_bigquery(form113)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
