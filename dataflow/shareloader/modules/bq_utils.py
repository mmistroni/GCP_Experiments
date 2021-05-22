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
  mshares_table_schema = 'RUN_DATE:STRING,TICKER:STRING,START_PRICE:FLOAT,END_PRICE:FLOAT,PERFORMANCE:FLOAT,RATINGS:STRING,MARKETCAP:FLOAT,BETA:FLOAT,PERATIO:FLOAT'
  return mshares_table_schema

def get_table_spec():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_shareloader',
      tableId='monthly_perf_latest')

def map_to_bq_dict(original_dict):
    return dict(     RUN_DATE=date.today().strftime('%Y-%m-%d'),
                     TICKER=original_dict['Ticker'],
                     START_PRICE=original_dict['Start_Price'],
                     END_PRICE=original_dict['End_Price'],
                     PERFORMANCE=original_dict['Performance'],
                     RATINGS=original_dict['Ratings'],
                     MARKETCAP=original_dict['marketCap'],
                     BETA=original_dict['beta'],
                     PERATIO=original_dict['peRatio'])

def get_news_table_schema():
  edgar_table_schema = 'RUN_DATE:STRING,TICKER:STRING,HEADLINE:STRING,SCORE:FLOAT'
  return edgar_table_schema

def get_news_table_spec():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_shareloader',
      tableId='daily_news')





