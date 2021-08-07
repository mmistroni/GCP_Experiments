from __future__ import absolute_import

import argparse
import logging
import re
import urllib
import json
import pandas as pd
from pandas.tseries.offsets import BDay
import pandas_datareader.data as dr
from datetime import datetime, date
import apache_beam as beam
from .mail_utils import send_mail
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from .monthly_sub_pipelines import run_my_pipeline, create_bigquery_ppln, \
    map_to_dict, enhance_with_ratings, write_data, remap_ratings, \
        join_performance_with_edgar
from .bq_utils import get_table_spec,get_table_schema


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients')
        parser.add_argument('--key')
        parser.add_argument('--sgridkey')

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()

    cloud_key = pipeline_options.key
    logging.info('===cloud key:{}'.format(cloud_key))
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:
        source = p  | 'Read Source File' >> ReadFromText('gs://datascience-bucket-mm/all_sectors.csv')
        sink = beam.io.WriteToBigQuery(
            get_table_spec(),
            schema=get_table_schema(),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        performance = run_my_pipeline(source)
        edgar_fills = create_bigquery_ppln(p)
        dicted_data = map_to_dict(performance)
        with_ratings = enhance_with_ratings(dicted_data, cloud_key)
        write_data(with_ratings, sink)

        ratings_remapped = remap_ratings(with_ratings)
        all_data = join_performance_with_edgar(ratings_remapped, edgar_fills)
        send_mail(all_data, pipeline_options)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()