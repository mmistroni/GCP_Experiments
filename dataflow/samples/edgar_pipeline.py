from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode
from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import re, requests
from datetime import datetime, date
from collections import OrderedDict
import requests


quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
full_dir = "https://www.sec.gov/Archives/edgar/full-index/{year}/{QUARTER}/"


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--abc', default='')
        parser.add_argument('--xyz', default='end')


def get_edgar_urls(years: list):
    print('fetching master.idx for year {}'.format(years))
    idx_directories = [full_dir.format(year=year, QUARTER=qtr) for year in years for qtr in quarters]
    return ['{}'.format(edgar_dir) for edgar_dir in idx_directories]

def retrieve_tickers():
    all_stocks = requests.get('https://financialmodelingprep.com/api/v3/company/stock/list').json()['symbolsList']
    return map(lambda d: d['symbol'], all_stocks)


def get_tickers():
    logging.info('Retreiving tickers ')
    tickers =  list(retrieve_tickers())
    logging.info('We have retrieved {}'.format(len(tickers)))
    return tickers

def get_prices(ticker):
    logging.info('Retreiving prices for {}'.format(ticker))
    full_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/Daily/{}?timeseries=1'.format(ticker)
    result = requests.get(full_url).json()
    try:
        historical_data = result['historical'][0]
        return [historical_data['date'], ticker, str(historical_data['adjClose']),
            str(historical_data['change']), str(historical_data['volume'])]
    except Exception as e :
        logging.info('Exception retrieving ticker for {}:{}'.format(ticker, str(e)))
        return [date.today().strftime('%Y-%m-%d'), '{}-{}'.format(ticker, 'Exception'), '0.0',
                '0.0', '0.0']


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)

    logging.info(pipeline_options.get_all_options())

    logging.info("=== readign from textfile:{}".format(pipeline_options.abc))

    destination = 'gs://mm_dataflow_bucket/outputs/shareloader/pipeline_test_{}.csv'.format(datetime.now().strftime('%Y%m%d-%H%M'))

    logging.info('====== Destination is :{}'.format(destination))

    lines = (p
             | 'Get List of Tickers' >> beam.Create(get_tickers())
             | 'Getting Prices' >> beam.Map(lambda symbol: get_prices(symbol))
             | 'Writing to CSV' >> beam.Map(lambda lst: ','.join(lst))
             | 'WRITE TO BUCKET' >> beam.io.WriteToText(destination, header='date,symbol,adj_close,change,volume',
                                                        num_shards=1)
             )
    result = p.run()

    return


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()