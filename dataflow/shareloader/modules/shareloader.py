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
from apache_beam.io import WriteToText
from apache_beam.io.textio import ReadAllFromText
import urllib
from collections import defaultdict
from datetime import date, datetime
from itertools import groupby
import requests
import os
import pandas_datareader.data as dr
from datetime import date
from .metrics import compute_metrics
from pandas.tseries.offsets import BDay
import logging
logger = logging.getLogger().setLevel(logging.INFO)


class XyzOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--business_days', default=60)


def get_tickers():
    return['AAPL', 'AMZN', 'MSFT']

def get_latest_price_yahoo(symbol, business_days):
  try:#
    logger.info('Fetching  prices for last :{}'.format(business_days))
    as_of_date = datee.today()
    start_date = as_of_date - BDay(60)
    logger.info('--latest price for{}'.format(symbol))
    res = dr.get_data_yahoo(symbol, start_date, as_of_date)[['Adj Close', 'Volume']]
    res['Symbol'] = symbol
    return res
  except Exception as e :
    logger.info('Exception in loading latest prices:{}'.format(str(e)))
    return pd.DataFrame(columns=[symbol])


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  dirpath = os.getcwd()
  print("current directory is : " + dirpath)

  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = XyzOptions()
  pipeline_options.view_as(SetupOptions).save_main_session  = save_main_session
  print('Options are:{}'.format(pipeline_options.get_all_options()))
  print('Busdays:{}'.format(pipeline_options.business_days))

  p4 = beam.Pipeline(options=pipeline_options)

  lines = (
       p4
       #| 'generate master url' >>beam.Create(['https://www.sec.gov/Archives/edgar/full-index/2019/QTR1/master.idx'])
       | 'Sampling Data' >> beam.Create(get_tickers())
       | 'Getting Latest Prices' >> beam.Map(lambda symbol: get_latest_price_yahoo(symbol, pipeline_options.business_days))
       | 'Printing Out Results' >> beam.Map(print)

  )
  p4.run().wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
