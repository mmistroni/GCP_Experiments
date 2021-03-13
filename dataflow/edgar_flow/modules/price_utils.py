import pandas_datareader.data as dr
from datetime import date
import logging
def get_current_price(symbol, start_dt=date.today()):
  try:
    logging.info('Gettign prices for:{}'.format(start_dt))
    data = dr.get_data_yahoo(symbol, start_dt, start_dt)[['Adj Close']]
    ret = data['Adj Close'].values[0]
    return ret
  except Exception as e:
    return 0
