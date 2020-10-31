import apache_beam as beam
import logging
from .bq_utils import get_table_schema, get_table_spec, map_to_bq_dict
from .metrics import compute_data_performance, compute_metrics, get_analyst_recommendations,\
                                            AnotherLeftJoinerFn, Display, output_fields, merge_dicts,\
                                            join_lists
from datetime import date
import argparse
import logging
import re
import urllib
import json
import pandas as pd
from pandas.tseries.offsets import BDay
import pandas_datareader.data as dr
from datetime import datetime, date
from .metrics import get_analyst_recommendations, get_historical_data_yahoo, get_date_ranges


def enhance_with_ratings(all_dicts, cloud_token):
    return (all_dicts
             |  'Filtering only the ones with Perf > 0.6' >> beam.Filter(lambda d: d['Performance'] > 0.6)
             |  'Filtering also where price > 10$' >> beam.Filter(lambda d: d['Start_Price'] > 10.0)
             | 'Enhance with Ratings' >> beam.Map(lambda d: get_analyst_recommendations(d, cloud_token))
             | 'TO BQ DICT' >> beam.Map(map_to_bq_dict)
            )

def map_to_dict(df_pipeline):
    dicted = (df_pipeline
                | 'Map' >> beam.Map(lambda x: x[['Ticker', 'Start_Price', 'End_Price', 'Performance']].to_dict())
                | 'TODICT' >> beam.Map(lambda d: dict((k, v[0]) for k, v in d.items()))
              )
    return dicted

def create_bigquery_ppln(p):
    cutoff_date = (date.today() - BDay(20)).date()
    logging.info('Cutoff is:{}'.format(cutoff_date))
    edgar_sql = """SELECT E.TICKER,  SUM(COUNT) AS TOTAL_FILLS 
FROM `datascience-projects.gcp_edgar.form_13hf_daily` E 
WHERE E.COB != '20201009' AND PARSE_DATE("%F", E.COB) < PARSE_DATE("%F", '{cutoff}')
GROUP BY TICKER ORDER BY TOTAL_FILLS DESC 
  """.format(run_date=date.today().strftime('%Y-%m-%d'), cutoff=cutoff_date.strftime('%Y-%m-%d') )
    logging.info('executing SQL :{}'.format(edgar_sql))
    return (p | beam.io.Read(beam.io.BigQuerySource(query=edgar_sql, use_standard_sql=True))
                  |'Extractign only what we need..' >> beam.Map(lambda elem: (elem['TICKER'],
                                           {'TOTAL_FILLS' : elem['TOTAL_FILLS']}))
                  | 'Removing NA' >> beam.Filter(lambda tpl: tpl[0] != 'N/A')
                )

def write_data(data, sink):
    return  (
        data
           | sink
    )

def create_joining_dict(input_dict):
    return (input_dict['TICKER'],
            input_dict)

def remap_ratings(input_ratings):
    return input_ratings | 'Remapping' >> beam.Map(create_joining_dict)

def join_performance_with_edgar(performance_p, edgar_p):
    logging.info('Joining Performances with Edgar...')
    return (
            {'perflist': performance_p, 'edgarlist': edgar_p}
            | 'co groupting' >> beam.CoGroupByKey()
            | 'flatmapping' >> beam.FlatMap(join_lists)
            | 'mergedicts' >> beam.Map(merge_dicts)
    )

def generate_performance(pandas_data):
    pfm = (pandas_data
           | 'Calculate Pef' >> beam.Map(lambda df: compute_metrics(df)))
    return pfm


def run_my_pipeline(source):
    tickers_and_sectors = (source
            | 'Map to Tpl' >> beam.Map(lambda ln: ln.split(','))
            | 'Get Prices' >> beam.Map(
                lambda tpl : (get_historical_data_yahoo(tpl[0],
                                                       tpl[1],
                                                       *get_date_ranges(20))))
           )
    return generate_performance(tickers_and_sectors)
