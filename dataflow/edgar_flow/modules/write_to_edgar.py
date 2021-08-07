import apache_beam as beam
import argparse
import logging
import re
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from modules.edgar_utils import ReadRemote, ParseForm13F, cusip_to_ticker, get_company_stats,\
                                        EdgarEmailSender
from datetime import date, datetime

import os
from modules.beam_functions import map_to_year, get_edgar_index_files, \
            map_from_edgar_row, map_to_bucket_string, \
            map_to_bq_dict, get_edgar_table_schema, get_edgar_table_spec

test_bucket = 'gs://mm_dataflow_bucket/'
form_type = '13F-HR'
filename = '{}_{}'.format(form_type, datetime.now().strftime('%Y$m%d-%H%M'))

GC_PROJECT = 'datascience-projects'
STAGING_BUCKET = 'gs://mm_dataflow_bucket/staging'
TEMP_BUCKET = 'gs://mm_dataflow_bucket/temp'
TEMPLATE_BUCKET = 'gs://mm_dataflow_bucket/templates'




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


  # Break apart first pipeline into smaller pipelines, perhaps 3 items per pipeline so that we can test it

  lines = (
       p4
       | 'Sampling Data' >> beam.Create(get_edgar_index_files())
       | 'Map to year'  >> beam.Map(lambda epath: map_to_year(epath, pipeline_options.year.get()))
       | 'readFromText' >> beam.ParDo(ReadRemote())
       | 'map to Str'   >> beam.Map(lambda line:str(line))
       | 'Filter only form 13HF' >> beam.Filter(lambda row: len(row.split('|')) > 4 and form_type in row.split('|')[2])
       | 'Generating Proper #file path' >> beam.Map(lambda r: map_from_edgar_row(r))
       | 'replacing eol' >> beam.Map(lambda p_tpl: (p_tpl[0], p_tpl[1][0:p_tpl[1].find('\\n')]))
       | 'parsing edgar filing' >> beam.ParDo(ParseForm13F())
       | 'Combining similar' >> beam.combiners.Count.PerElement()
       | 'Groupring' >> beam.MapTuple(lambda tpl, count: (tpl[0], tpl[1], count))
       | 'Adding Cusip' >> beam.MapTuple(lambda cob, word, count: (cob, word, cusip_to_ticker(word), count))
       |'Fetching Statistics and Mapping to BQ' >> beam.Map(lambda tpl: get_company_stats(tpl, pipeline_options.fmprepkey ))

  )

  write_to_bigquery = (
          lines
          | 'Map to Another Dict' >> beam.Map(lambda tpl: map_to_bq_dict(tpl, pipeline_options.year.get()))
          | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                              get_edgar_table_spec(),
                              schema=get_edgar_table_schema(),
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
  )

  p4.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
