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
from .utils import get_isr_and_kor, get_usr_adrs, get_latest_price_yahoo_2
from functools import reduce


ROW_TEMPLATE =  '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'

class ADREmailSender(beam.DoFn):
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
        if element:
            msg = element
            logging.info('Attepmting to send emamil to:{} with diff {}'.format(self.recipients,msg))
            template = \
                "<html><body><table><th>Symbol</th><th>Name</th><th>Latest Price</th><th>Change</th><th>Open</th><th>PreviousClose</th>" \
                "{}</table></body></html>"
            content = template.format(msg)
            logging.info('Sending \n {}'.format(content))
            message = Mail(
                from_email='gcp_portfolio@mmistroni.com',
                subject='ADR Stocks spiked up!',
                html_content=content)

            personalizations = self._build_personalization(self.recipients)
            for pers in personalizations:
                message.add_personalization(pers)

            sg = SendGridAPIClient(self.key)

            response = sg.send(message)
            logging.info('Mail Sent:{}'.format(response.status_code))
            logging.info('Body:{}'.format(response.body))
        else:
            logging.info('Not Sending email...nothing to do')


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key')
        parser.add_argument('--fmprepkey')


def extract_data_pipeline(p, input_file):
    logging.info('r?eadign from:{}'.format(input_file))
    return (p
            | 'Reading Tickers' >> beam.io.textio.ReadFromText(input_file)
            | 'Converting to Tuple' >> beam.Map(lambda row: row.split(','))
            | 'Extracting only ticker ' >> beam.Map(lambda item: (item[0]))
            )

def map_ticker_to_html_string(tpl):
    res =   ROW_TEMPLATE.format(tpl[0], tpl[1], tpl[2], tpl[3], tpl[4], tpl[5])
    logging.info('Mapped is:{}'.format(res))
    return res

def combine_to_html_rows(elements):
    logging.info('Combining')
    combined =  reduce(lambda acc, current: acc + current, elements, '')
    logging.info('Combined string is:{}'.format(combined))
    return combined

def find_diff(ticker, fmprepkey):
    quote_url = 'https://financialmodelingprep.com/api/v3/quote/{}?apikey={}'.format(ticker, fmprepkey)
    logging.info('Get latest price for:{}'.format(quote_url))

    res = requests.get(quote_url).json()
    if res:
        item = res[0]
        key_fields = ['symbol','name',  'price', 'changesPercentage', 'open', 'previousClose']
        return dict((k, v) for k, v in item.items() if k in key_fields)


def run_my_pipeline(p, fmprepkey, filter_fun):
    return (p
             | 'Getting Prices' >> beam.Map(lambda ticker: find_diff(ticker, fmprepkey))
             | 'Filtering out nones' >> beam.Filter(lambda d : d is not None)
             | 'Filtering Increases' >> beam.Filter(lambda mydict: filter_fun(mydict))
             )

def email_pipeline(p, options):
    logging.info('Sending emails')
    return (p
            | 'Map to HTML Table' >> beam.Map(map_ticker_to_html_string)
            | 'Combine to one Text' >> beam.CombineGlobally(combine_to_html_rows)
            | 'SendEmail' >> beam.ParDo(ADREmailSender(options.recipients, options.key))
            )


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    logging.info(pipeline_options.get_all_options())
    input_file = 'gs://mm_dataflow_bucket/inputs/fmprep_adrs.csv'
    stock_filter = lambda d: d['changesPercentage'] > 15

    with beam.Pipeline(options=pipeline_options) as p:
        tickers = extract_data_pipeline(p, input_file)
        result =  run_my_pipeline(tickers, pipeline_options.fmprepkey, stock_filter)
        email_pipeline(result, pipeline_options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()