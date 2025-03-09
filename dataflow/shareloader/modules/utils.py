import json
import pandas as pd
from pandas.tseries.offsets import BDay
import pandas_datareader.data as dr
import numpy as np
from datetime import datetime, date
import requests
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
import apache_beam as beam


def get_all_shares_dataframe():
  all_shares = requests.get('https://k1k1xtrm88.execute-api.us-west-2.amazonaws.com/test/query-shares').json()
  ds = [d for d in all_shares if d['QTY'] > 1]
  return pd.DataFrame.from_dict(ds)


def get_similar_companies(apiKey, industry, exchange):
    # URL = https://financialmodelingprep.com/api/v3/stock-screener?marketCapMoreThan=1000000000&betaMoreThan=1&volumeMoreThan=10000&sector=Technology&exchange=NASDAQ&dividendMoreThan=0&limit=100&apikey=79d4f398184fb636fa32ac1f95ed67e6
    baseUrl = f'https://financialmodelingprep.com/api/v3/stock-screener?industry={industry}&exchange={exchange}&apikey={apiKey}'
    return requests.get(baseUrl).json()


def get_peers(apiKey, ticker):
    # URL = https://financialmodelingprep.com/api/v3/stock-screener?marketCapMoreThan=1000000000&betaMoreThan=1&volumeMoreThan=10000&sector=Technology&exchange=NASDAQ&dividendMoreThan=0&limit=100&apikey=79d4f398184fb636fa32ac1f95ed67e6
    baseUrl = f'https://financialmodelingprep.com/api/v4/stock_peers?symbol={ticker}&apikey={apiKey}'
    return requests.get(baseUrl).json()[0].get('peersList', [])


def get_latest_price_yahoo(symbol, cob_date):
    try:  #
        print('--latest price for{}'.format(symbol))
        start_date = cob_date - BDay(1)
        dfy = dr.get_data_yahoo(symbol, start_date, start_date)[['Adj Close']]
        dft = dr.get_data_yahoo(symbol, cob_date, cob_date)[['Adj Close']]
        dfy['symbol'] = symbol
        dft['symbol'] = symbol
        print(dfy.shape)
        print(dft.shape)

        merged = pd.merge(dft, dfy, on='symbol', suffixes=('_t', '_y'), )
        merged['diff'] = merged['Adj Close_t'] - merged['Adj Close_y']
        print('Merged shap eis:{}'.format(merged.shape))
        return merged.iloc[0].to_dict()

    except Exception as e:
        print('Unable to find data for {}'.format(symbol))
        return pd.DataFrame.from_dict(
            {'symbol': [symbol], 'Adj Close_t': [0], 'Adj Close_y': [0], 'diff': [0]}).to_dict()

def get_latest_price_yahoo_2(symbol, cob_date):
    try:  #
        logging.info('--latest price for{}'.format(symbol))
        start_date = cob_date - BDay(1)
        res = dr.get_data_yahoo(symbol, start_date, cob_date)['Adj Close']
        logging.info('We got:{}'.format(res))
        return res.pct_change().values[-1]
    except Exception as e:
        print('Unable to find data for {}'.format(symbol))
        return 0


def get_prices(symbols):
    prices_dfs = (get_latest_price_yahoo(symbol, date.today()) for symbol in symbols)
    all_data = pd.concat(prices_dfs)
    return all_data


def create_email_template(input_elements):
    total_ptf_value = sum(map(lambda elm_list: elm_list[6], input_elements))
    one, two, three = 'one', 'two', 'three'
    base_template = '<tr><td>{ticker}</td><td>{qty}</td><td>{}</td></tr>'.format(one, two, three)
    mapped_str = map(lambda lst: base_template.format(

    ))

def get_isr_and_kor(token):
  isr_stocks = dict((d['name'], d['symbol']) for d in requests.get('https://cloud.iexapis.com/stable/ref-data/exchange/TAE/symbols?token={token}'.format(token=token)).json())
  kor_stocks = dict((d['name'], d['symbol']) for d in requests.get('https://cloud.iexapis.com/stable/ref-data/exchange/KRX/symbols?token={token}'.format(token=token)).json())
  isr_stocks.update(kor_stocks)
  return isr_stocks

def get_out_of_hour_info(token, ticker):
  logging.info('Getting out of quote info for {}'.format(ticker))
  try:
    tpl = ('AAPL', 1, 20.0)
    ticker, qty, original_price = tpl[0] , int(tpl[1]), float(tpl[2])
    stat_url = 'https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={token}'.format(symbol=ticker,
                                                                                                token='')
    historical_data = requests.get(stat_url).json()[0]
    return historical_data['price'], historical_data['change'],
  except Exception as e:
      logging.info('exception in retrieving quote for :{}:{}'.format(ticker, str(e)))
      return 0,0


def get_usr_adrs(token):
  nas_stocks = [d for d in requests.get('https://cloud.iexapis.com/stable/ref-data/exchange/NAS/symbols?token={token}'.format(token=token)).json()]
  nys_stocks = [d for d in requests.get('https://cloud.iexapis.com/stable/ref-data/exchange/NYS/symbols?token={token}'.format(token=token)).json()]
  all_us = nys_stocks + nas_stocks
  return dict((c['name'].split('-')[0].strip(), c['symbol']) for c in all_us if c['type'] == 'ad')# and c['name'].split('-')[0].strip() in kor_stocks)






class EmailSender(beam.DoFn):
    def __init__(self, recipients, key):
        self.recipients = recipients.split(',')
        self.key = key

    def _build_personalization(self, recipients):
        personalizations = []
        for recipient in recipients:
            logging.info('Adding personalization for {}'.format(recipient))
            person1 = Personalization()
            person1.add_to(Email(recipient))
            personalizations.append(person1)
        return personalizations


    def process(self, element):
        logging.info('Attepmting to send emamil to:{}'.format(self.recipients))
        template = "<html><body><table><th>Cusip</th><th>Ticker</th><th>Counts</th>{}</table></body></html>"
        content = template.format(element)
        print('Sending \n {}'.format(content))
        message = Mail(
            from_email='from_email@example.com',
            #to_emails=self.recipients,
            subject='Sending with Twilio SendGrid is Fun',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        print(response.status_code, response.body, response.headers)





