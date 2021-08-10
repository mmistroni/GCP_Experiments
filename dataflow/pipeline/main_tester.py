import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date, datetime
from apache_beam.options.value_provider import RuntimeValueProvider
from datetime import date
import requests
import urllib
import requests

EDGAR_QUARTERLY_URL = 'gs://mm_dataflow_bucket/inputs/all_data_utilities_df.csv'

def run(argv=None, save_main_session=True):

    # Look for form N-23C3B

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    logging.info('starting pipeline..')
    with beam.Pipeline(options=pipeline_options) as p:
          (p
         | 'ReadTextFile' >> beam.io.textio.ReadFromText(EDGAR_QUARTERLY_URL)
         | 'Log it out' >> beam.Map(logging.info))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
