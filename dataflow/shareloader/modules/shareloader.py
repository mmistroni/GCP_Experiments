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

def get_tickers():
    return['AAPL', 'AMZN', 'MSFT']

def get_latest_price_yahoo(symbol):
  try:#
    as_of_date = date(2020,2,28)
    print('--latest price for{}'.format(symbol))
    res = dr.get_data_yahoo(symbol, as_of_date, as_of_date)[['Close']]
    df['Symbol'] = symbol
    return df
  except Exception as e :
    return pd.DataFrame(columns=[symbol])


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  dirpath = os.getcwd()
  print("current directory is : " + dirpath)

  known_args, pipeline_args = parser.parse_known_args(argv)


  p4 = beam.Pipeline(options=PipelineOptions())

  lines = (
       p4
       #| 'generate master url' >>beam.Create(['https://www.sec.gov/Archives/edgar/full-index/2019/QTR1/master.idx'])
       | 'Sampling Data' >> beam.Create(get_tickers())
       | 'Getting Latest Prices' >> beam.Map(lambda symbol: get_latest_price_yahoo(symbol))
       | 'Printing Out Results' >> beam.Map(print)

  )
  p4.run().wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
