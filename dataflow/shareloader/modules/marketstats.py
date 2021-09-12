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
from  .marketstats_utils import is_above_52wk,get_prices,MarketBreadthCombineFn, get_all_stocks, is_below_52wk,\
                            combine_movers,get_prices2

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization


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
        logging.info('Attepmting to send emamil to:{}, using key:{}'.format(self.recipients, self.key))
        template = "<html><body>{}</body></html>"
        content = template.format(element)
        print('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_cloud@mmistroni.com',
            to_emails=self.recipients,
            subject='Market Stats',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        print(response.status_code, response.body, response.headers)


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--key')
        parser.add_argument('--sendgridkey')
        #parser.add_argument('--recipients', default='mmistroni@gmail.com')

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)

    iexapi_key = pipeline_options.key
    logging.info(pipeline_options.get_all_options())
    current_dt = datetime.now().strftime('%Y%m%d-%H%M')
    destination = 'gs://mm_dataflow_bucket/outputs/shareloader/{}_run_{}.csv'
    donefile = 'gs://mm_dataflow_bucket/outputs/shareloader/{}_run_{}.done'

    logging.info('====== Destination is :{}'.format(destination))
    logging.info('SendgridKey=={}'.format(pipeline_options.sendgridkey))


    prices = (p
             | 'Get List of Tickers' >> beam.Create(get_all_stocks(iexapi_key))
             | 'Getting Prices' >> beam.Map(lambda symbol: get_prices2(symbol, iexapi_key))
             | 'Filtering blanks' >> beam.Filter(lambda d: len(d) > 0)
             )
    marketbreadth = (
            prices
            | 'Combine MarketBreadth Statistics' >> beam.CombineGlobally(MarketBreadthCombineFn())
            
    )
    above_52 = (
            prices
            | 'Find 52Week High' >> beam.Filter(is_above_52wk)
            | 'Mapping Tickers1' >> beam.Map(lambda d: d[0])
            | 'Combine Above' >> beam.CombineGlobally(combine_movers, label='Above 52wk high:')
            | 'ADD Label' >> beam.Map(lambda txt: 'Above 52 wk:{}'.format(txt))
            )

    below_52 = (
            prices
            | 'Find 52Week Low' >> beam.Filter(is_below_52wk)
            | 'Mapping Tickers2' >> beam.Map(lambda d: d[0])
            | 'Combine Below' >> beam.CombineGlobally(combine_movers, label='Below 52wk low:')
            | 'ADD Label2' >> beam.Map(lambda txt: 'Below 52 wk:{}'.format(txt))

    )

    final = (
            (marketbreadth, above_52, below_52)
            | 'FlattenCombine all' >> beam.Flatten()
            | 'Combine' >> beam.CombineGlobally(lambda x: '<br><br>'.join(x))
            | 'SendEmail' >> beam.ParDo(EmailSender('mmistroni@gmail.com', pipeline_options.sendgridkey))


    )


    p.run()

    return


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()