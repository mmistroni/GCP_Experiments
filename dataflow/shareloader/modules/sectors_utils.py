import requests
import apache_beam as beam
import logging
from itertools import chain
from bs4 import BeautifulSoup
import requests
from itertools import chain
from io import StringIO
from datetime import date, timedelta, datetime
from pandas.tseries.offsets import BDay
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
from .marketstats_utils import get_senate_disclosures

def fetch_performance(sector, ticker, key):
    endDate = date.today()
    startDate = (endDate - BDay(90)).date()
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={startDate.strftime('%Y-%m-%d')}&to={endDate.strftime('%Y-%m-%d')}&apikey={key}"
    historical = requests.get(url).json().get('historical')
    df = pd.DataFrame(data=historical[::-1])
    df['date'] = pd.to_datetime(df.date)
    df['ticker'] = ticker
    df = df.set_index('date')
    resampled = df.resample('1M').mean()
    resampled[sector] = resampled.close / resampled.close.shift(1) - 1
    records = resampled[[sector]].dropna().T.to_dict('records')

    data = []
    for k, v in records[0].items():
        data.append((k.strftime('%Y-%m-%d'), v))

    return (sector, data)




class ETFHistoryCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    logging.info('Adding{}'.format(input))
    logging.info('acc is:{}'.format(accumulator))
    accumulator.append(input)
    return accumulator

  def merge_accumulators(self, accumulators):

    return list(chain(*accumulators))

  def extract_output(self, sum_count):
    return sum_count


class SectorsEmailSender(beam.DoFn):
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

  def _build_html_message(self, rows):
      html = '<table border="1">'
      header_row = "<tr><th>Sector</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr>"
      row_template = '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'

      headers = rows[0][1]
      dates = [tpl[0] for tpl in headers]
      header_row = header_row.format(*dates)
      html += header_row

      for sector, dates in rows:
          returns = ['%.3f' % val[1] for val in dates]
          sector_data = [sector] + returns
          html += row_template.format(*sector_data)
      html += '</table>'
      return html

  def process(self, element):
      sector_returns = element
      logging.info(f'Processing returns:\n{sector_returns}')
      data = self._build_html_message(element)
      content = \
          "<html><body>{}</body></html>".format(data)

      message = Mail(
          from_email='gcp_portfolio@mmistroni.com',
          subject='Sectors Return for last 3 Months',
          html_content=content)

      personalizations = self._build_personalization(self.recipients)
      for pers in personalizations:
          message.add_personalization(pers)

      sg = SendGridAPIClient(self.key)

      response = sg.send(message)
      logging.info('Mail Sent:{}'.format(response.status_code))
      logging.info('Body:{}'.format(response.body))
