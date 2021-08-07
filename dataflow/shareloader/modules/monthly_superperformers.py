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
from .bq_utils import get_table_spec,get_table_schema
from .superperf_metrics import  get_historical_data_yahoo, get_all_stock_metrics, create_metrics,\
    get_fmprep_metrics

class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients')
        parser.add_argument('--key')
        parser.add_argument('--sgridkey')
        parser.add_argument('--fmprepkey')

def merge_with_fmprep(perf_dict, end_dt) :
    try:
        ticker = perf_dict['ticker']
        print('Getting fmrpep for:{}'.format(ticker))
        fmprepdata = get_fmprep_metrics(ticker, end_dt)
        existing = perf_dict.copy()
        existing.update(fmprepdata)
        return pd.DataFrame([existing])
    except Exception as e:
        print('Could not ifnd FMPREP data for :{}'.format(perf_dict['ticker']))
        return None

def groupByIndustry(perf_df, tickersdf):
    performer_with_industry  = pd.merge(perf_df, tickersdf, on='ticker')
    with_ind_grp = performer_with_industry.groupby('industry', as_index=False).agg({'ticker' : 'count'})
    print('Filtering where we have more than 5 items in industry')
    good_ones =  with_ind_grp[with_ind_grp['ticker'] > 1][['industry']]
    print('Good ones shape:{}. Perf with ind shape:{}'.format(good_ones.shape, performer_with_industry.shape))
    if good_ones.shape[0] > 0:
        return pd.merge(performer_with_industry, good_ones, on='industry')




def build_data(ticker, end_date):
    try:
        start_date = end_date - BDay(300)
        test_data = get_historical_data_yahoo(ticker, start_date, end_date)

        df_metrics = get_all_stock_metrics(test_data)
        perf_metrics = create_metrics(ticker, df_metrics)
        return perf_metrics
    except Exception as e:
        print('error for {}:{}'.format(ticker, str(e)))


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

        #edgar_fills = create_bigquery_ppln(p)

        #ratings_remapped = remap_ratings(with_ratings)
        #all_data = join_performance_with_edgar(ratings_remapped, edgar_fills)
        #ssend_mail(all_data, pipeline_options)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()