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
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from .bq_utils import get_table_schema, get_table_spec, map_to_bq_dict
import re, requests
from datetime import datetime, date
from collections import OrderedDict
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
from .metrics import compute_data_performance, compute_metrics
from datetime import date


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key')

def get_date_ranges(prev_bus_days):
    print('Checking result sfor {}'.format(prev_bus_days))
    end_date = date.today()
    start_date = end_date - BDay(
        prev_bus_days)  # go back 3 months to find crossovers. Start to plan to move to Dataflow
    return start_date, end_date

def get_historical_data_yahoo(symbol, sector, start_dt, end_dt):
    try:
        end_date = date.today()
        data = dr.get_data_yahoo(symbol, start_dt, end_dt)[['Adj Close']]
        df = data.rename(columns={'Adj Close': symbol})
        df['COB'] = date.today().strftime('%Y-%m-%d')
        #df['sector'] = sector
        return df
    except Exception as e:
        return pd.DataFrame(columns=[symbol])

def map_to_dict(df_pipeline):
    dicted = (df_pipeline
                | 'Map' >> beam.Map(lambda x: x[['Ticker', 'Start_Price', 'End_Price', 'Performance']].to_dict())
                | 'TODICT' >> beam.Map(lambda d: dict((k, v[0]) for k, v in d.items()))
                | 'Enhance' >> beam.Map(map_to_bq_dict)
              )
    return dicted

def generate_performance(pandas_data):
    pfm = (pandas_data
           | 'Calculate Pef' >> beam.Map(lambda df: compute_metrics(df)))
    return pfm

def write_data(data, sink):
    data | sink


def run_my_pipeline(source):
    tickers_and_sectors = (source
            | 'Map to Tpl' >> beam.Map(lambda ln: ln.split(','))
            | 'Get Prices' >> beam.Map(
                lambda tpl : (get_historical_data_yahoo(tpl[0],
                                                       tpl[1],
                                                       *get_date_ranges(20))))
           )
    return generate_performance(tickers_and_sectors)


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:
        source = p  | 'Read Source File' >> ReadFromText('gs://datascience-bucket-mm/all_sectors.csv')
        sink = beam.io.WriteToBigQuery(
            get_table_spec(),
            schema=get_table_schema(),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        performance = run_my_pipeline(source)
        dicted_data = map_to_dict(performance)
        write_data(dicted_data, sink)



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()