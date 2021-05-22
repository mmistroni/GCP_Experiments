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
                "<html><body><table><th>Foreign Ticker</th><th>ADR Ticker</th><th>Latest Price</th><th>Change</th>{}</table></body></html>"
            content = template.format(msg)
            logging.info('Sending \n {}'.format(content))
            message = Mail(
                from_email='gcp_portfolio@mmistroni.com',
                subject='KOR-ISR Stocks spiked up!',
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
        parser.add_argument('--iexapikey')


def create_us_and_foreign_dict(token):
    adrs = get_usr_adrs(token)
    intern_stocks = get_isr_and_kor(token)
    intern_symbols = [(k, v) for k, v in intern_stocks.items() if k in adrs.keys()]
    adr_symbols = dict((k, v) for k, v in adrs.items() if k in intern_stocks)
    intern_and_adr = dict(
        map(lambda tpl: (tpl[0], tpl[1].replace('-IT', '.TA').replace('-KP', '.KS')), intern_symbols))
    intern_and_adr
    # adr_symbols

    us_and_foreign = map(lambda tpl: (tpl[0], tpl[1], intern_and_adr.get(tpl[0])), adr_symbols.items())
    res = list(us_and_foreign)
    logging.info('US and foreign:{}'.format(res))
    return res

def map_ticker_to_html_string(tpl):
    res =   ROW_TEMPLATE.format(tpl[0], tpl[1], tpl[2], tpl[3])
    logging.info('Mapped is:{}'.format(res))
    return res

def combine_to_html_rows(elements):
    logging.info('Combining')
    combined =  reduce(lambda acc, current: acc + current, elements, '')
    logging.info('Combined string is:{}'.format(combined))
    return combined

def find_diff(ticker, start_date):
    print('finding prices for:{}'.format(ticker))
    res =  get_latest_price_yahoo_2(ticker, start_date)
    logging.info('Data for {}={}'.format(ticker, res))
    return res

def run_my_pipeline(p, options):
    return (p
             |  'Start'>> beam.Create(create_us_and_foreign_dict(options.iexapikey))
             | 'Getting Prices' >> beam.Map(lambda tpl: (tpl[0], tpl[1], tpl[2], find_diff(tpl[2], date.today())))
             | 'Filtering Increases' >> beam.Filter(lambda tpl: tpl[3] > 0.15)
             | 'Printing out' >> beam.Map(logging.info)
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
    start_list = create_us_and_foreign_dict(pipeline_options.iexapikey)
    logging.info('===== STARTING===== with :{}'.format(start_list))
    with beam.Pipeline(options=pipeline_options) as p:
        result =  (p|  'Start' >> beam.Create(start_list)
                        | 'Getting Prices' >> beam.Map(lambda tpl: (tpl[0], tpl[1], tpl[2], find_diff(tpl[2], date.today())))
                        | 'Filtering Increases' >> beam.Filter(lambda tpl: tpl[3] > 0.05)
                        | 'Map to HTML Table' >> beam.Map(lambda t:map_ticker_to_html_string(t))
                        | 'Combine to one Text' >> beam.CombineGlobally(combine_to_html_rows)
                        | 'SendEmail' >> beam.ParDo(ADREmailSender(pipeline_options.recipients, pipeline_options.key))
                   )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()