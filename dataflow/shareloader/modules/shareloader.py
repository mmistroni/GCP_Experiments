from __future__ import absolute_import

import argparse
import logging
import re

from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import re, requests
from datetime import datetime, date
from collections import OrderedDict
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization


ROW_TEMPLATE =  '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'

class PortfolioCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return ('', 0.0)

  def add_input(self, accumulator, input):
    logging.info('Adding{}'.format(input))
    logging.info('acc is:{}'.format(accumulator))
    (row_acc, current_diff) = accumulator
    return row_acc + ROW_TEMPLATE.format(*input), current_diff + input[5]

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)
    return ''.join(sums), sum(counts)

  def extract_output(self, sum_count):
    (sum, count) = sum_count
    return sum_count

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
            "<html><body><table><th>Ticker</th><th>Quantity</th><th>Latest Price</th><th>Change</th><th>Volume</th><th>Diff</th><th>Positions</th><th>Total Gain</th><th>Action</th>{}</table></body></html>"
        content = template.format(msg)
        logging.info('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_cloud_mm@outlook.com',
            subject='Portfolio change:{}'.format(ptf_diff),
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        logging.info('Mail Sent:{}'.format(response.status_code))
        logging.info('Body:{}'.format(response.body))



class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key')
        parser.add_argument('--fmprepkey')


def get_prices(tpl, fmprepkey):
    try:
        ticker, qty, original_price = tpl[0] , int(tpl[1]), float(tpl[2])
        stat_url = 'https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={token}'.format(symbol=ticker,
                                                                                                   token=fmprepkey)
        historical_data = requests.get(stat_url).json()[0]
        pandl = historical_data.get('change', 0) * int(qty)
        current_pos = int(qty) * historical_data.get('price', 0)
        total_gain = int(qty) * (historical_data.get('price', 0) - float(original_price))
        wk52high = historical_data.get('yearHigh',0)
        return [ticker, qty,
             historical_data.get('price', 0),
             historical_data.get('change', 0),
             historical_data.get('volume', 0),
             pandl, current_pos, total_gain, 'Above 52wk High' if historical_data.get('price', 0) > wk52high else '' ]
    except Exception as e :
        logging.info('Excepiton for {}:{}'.format(tpl[0], str(e)))
        return None

def combine_portfolio(elements):
    # Calculating variance, we need it for subject
    row_template = '<tr><td>{}</td><td>{}</td><td>{}</td>td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'
    logging.info('Combining:{}'.format(elements))
    combined = map(lambda el: row_template.format(*el), elements)
    joined = ''.join(list(combined))
    return (joined, 100.0)

def run_my_pipeline(p, options):
    lines = (p
             | 'Getting Prices' >> beam.Map(lambda symbol: get_prices(symbol, options.fmprepkey))
             | 'Filtering empties' >> beam.Filter(lambda row: row is not None)
             | 'Combine' >> beam.CombineGlobally(PortfolioCombineFn())
             | 'SendEmail' >> beam.ParDo(EmailSender(options.recipients, options.key))
             )
    return lines


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    input_file = 'gs://mm_dataflow_bucket/inputs/shares.txt'
    destination = 'gs://mm_dataflow_bucket/outputs/shareloader/pipeline_{}.csv'.format(datetime.now().strftime('%Y%m%d-%H%M'))
    logging.info(pipeline_options.get_all_options())
    logging.info("=== readign from textfile:{}".format(input_file))
    logging.info('====== Destination is :{}'.format(destination))

    with beam.Pipeline(options=pipeline_options) as p:
        input = (p | 'Start' >> beam.io.textio.ReadFromText(input_file)
                   |' Split each line' >> beam.Map(lambda f: tuple(f.split(",")))
                   | 'Deduplicate elements_{}' >> beam.Distinct()
                   | 'Filter only elements wtih length 3' >> beam.Filter(lambda l: len(l) == 3)
                 )

        run_my_pipeline(input, pipeline_options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()