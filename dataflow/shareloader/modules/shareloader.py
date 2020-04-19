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
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization



ROW_TEMPLATE =  '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'


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
        msg, ptf_diff = element
        logging.info('Attepmting to send emamil to:{} with diff {}'.format(self.recipients, ptf_diff))
        template = \
            "<html><body><table><th>Ticker</th><th>Quantity</th><th>Latest Price</th><th>Change</th><th>Volume</th><th>Diff</th><th>Positions</th>{}</table></body></html>"
        content = template.format(msg)
        logging.info('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_portfolio@mmistroni.com',
            subject='Portfolio change:{}'.format(ptf_diff),
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        logging.info('Mail Sent:{}'.format(response.status_code))
        logging.info('Body:{}'.format(response.body))


class PortfolioCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return ('', 0.0)

  def add_input(self, accumulator, input):
    print('Adding{}'.format(input))
    print('acc is:{}'.format(accumulator))
    (row_acc, current_diff) = accumulator
    return row_acc + ROW_TEMPLATE.format(*input), current_diff + input[5]

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)
    return ''.join(sums), sum(counts)

  def extract_output(self, sum_count):
    (sum, count) = sum_count
    return sum_count

class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key')

def get_prices(tpl):
    logging.info('Input tpl is:{}'.format(tpl))
    ticker, qty, original_price = tpl.split(',')
    logging.info('Retreiving prices for {}'.format(ticker))
    full_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/Daily/{}?timeseries=1'.format(ticker)
    result = requests.get(full_url).json()
    historical_data = result['historical'][0]
    pandl = historical_data['change'] * int(qty)
    current_pos = int(qty) * historical_data['adjClose']
    return [ticker, qty,
        historical_data['adjClose'],
        historical_data['change'],
        historical_data['volume'],
        pandl, current_pos]

def combine_portfolio(elements):
    # Calculating variance, we need it for subject
    row_template = '<tr><td>{}</td><td>{}</td><td>{}</td>td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'
    combined = map(lambda el: row_template.format(*el), elements)
    joined = ''.join(list(combined))
    return (joined, 100.0)

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)

    logging.info(pipeline_options.get_all_options())
    input_file = 'gs://mm_dataflow_bucket/inputs/shares.txt'
    logging.info("=== readign from textfile:{}".format(input_file))
    destination = 'gs://mm_dataflow_bucket/outputs/shareloader/pipeline_{}.csv'.format(datetime.now().strftime('%Y%m%d-%H%M'))

    logging.info('====== Destination is :{}'.format(destination))

    lines = (p
             | 'Get List of Tickers' >> ReadFromText(input_file)
             | 'Getting Prices' >> beam.Map(lambda symbol: get_prices(symbol))
             | 'Combine' >> beam.CombineGlobally(PortfolioCombineFn())
             | 'SendEmail' >> beam.ParDo(EmailSender(pipeline_options.recipients, pipeline_options.key))
             )
    p.run()

    return


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()