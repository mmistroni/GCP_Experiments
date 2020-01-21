import json
import pandas as pd
from pandas.tseries.offsets import BDay
import pandas_datareader.data as dr
import numpy as np
from datetime import datetime, date
import requests

logger = logging.getLogger(__name__)


class StockTickerClient(object):
    '''
      Nasdaq client to get quartery predictions
    '''

    @staticmethod
    def get_latest_price_yahoo(symbol, cob_date):
        try:  #
            logger.info('--latest price for{}'.format(symbol))
            start_date = cob_date - BDay(1)
            dfy = dr.get_data_yahoo(symbol, start_date, start_date)[['Adj Close']]
            dft = dr.get_data_yahoo(symbol, cob_date, cob_date)[['Adj Close']]
            dfy['symbol'] = symbol
            dft['symbol'] = symbol
            merged = pd.merge(dft, dfy, on='symbol', suffixes=('_t', '_y'), )
            merged['diff'] = merged['Adj Close_t'] - merged['Adj Close_y']
            return merged.iloc[0].to_dict()
        except Exception as e:
            print('Unable to find data for {}'.format(symbol))
            return pd.DataFrame.from_dict(
                {'symbol': [symbol], 'Adj Close_t': [0], 'Adj Close_y': [0], 'diff': [0]}).to_dict()


def get_latest_stock_values(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    logger.info('REquest Path is:{}', request.path)

    input_path = request.path.split('/')
    if len(input_path) > 1:
        ticker = input_path[1]
        logger.info('Getting ticker for:{}'.format(ticker))
        return StockTickerClient.get_latest_price_yahoo(ticker, date.today())

    else:
        return 'Invalid Request:{}'.format(request.path)
