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

test_bucket = 'gs://mm_dataflow_bucket/'
form_type = '13F-HR'
filename = '{}_{}'.format(form_type, datetime.now().strftime('%Y$m%d-%H%M'))

RUNNER ='DataflowRunner'
GC_PROJECT = 'datascience-projects'
STAGING_BUCKET = 'gs://mm_dataflow_bucket/staging'
TEMP_BUCKET = 'gs://mm_dataflow_bucket/temp'
TEMPLATE_BUCKET = 'gs://mm_dataflow_bucket/templates'


### BIG QUERY CONFIGS
## BIG QUERY SCHEMA

from apache_beam.io.gcp.internal.clients import bigquery
table_schema = 'source:STRING, quote:STRING'
table_spec = bigquery.TableReference(
    projectId=GC_PROJECT,
    datasetId='gcp_edgar',
    tableId='test_edgar_data')


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)

    #pipeline_args.append('--project {}'.format(GC_PROJECT))
    #pipeline_args.append('--runner {}'.format(RUNNER))
    #pipeline_args.append('--staging_location {}'.format(STAGING_BUCKET))
    #pipeline_args.append('--temp_location {}'.format(TEMP_BUCKET))
    #pipeline_args.append('--template_location {}/test_dataflow_template'.format(TEMPLATE_BUCKET))

    pipeline_options = PipelineOptions(pipeline_args)
    print('== pipeline options are:{}'.format(pipeline_options))

    p2 = beam.Pipeline(pipeline_options)
    print('')
    test_buckt = 'gs://mm_dataflow_bucket/'
    lines = (
          p2
          | beam.Create([
                              {'source': 'Mahatma Gandhi', 'quote': 'My life is my message.'},
                              {'source': 'Yoda', 'quote': "Do, or do not. There is no 'try'."},
                          ])
          | beam.io.WriteToBigQuery(
                      table_spec,
                      schema=table_schema,
                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        )
    p2.run().wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

