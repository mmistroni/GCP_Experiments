import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date, datetime
from itertools import groupby
from pandas.tseries.offsets import BDay
import requests
import os
from apache_beam.io.gcp.internal.clients import bigquery

def get_table_schema():
  edgar_table_schema = 'COB:STRING,Ticker:STRING,Start_Price:FLOAT,End_Price:FLOAT,Performance:FLOAT'
  return edgar_table_schema

def get_table_spec():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='monthly_perf')

def map_to_bq_dict(original_dict):
    return dict(     RUN_DATE=date.today().strftime('%Y-%m-%d'),
                     TICKER=original_dict['Ticker'],
                     START_DATE=original_dict['Start_Date'],
                     END_DATE=original_dict['End_Date'],
                     PERFORMANCE=original_dict['PERFORMANCE'])
