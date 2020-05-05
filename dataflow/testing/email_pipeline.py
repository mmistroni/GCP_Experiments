from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode
from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText,ReadAllFromText
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



class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients')
        parser.add_argument('--key')

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

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)

    input_file = 'gs://mm_dataflow_bucket/inputs/shares.txt'

    logging.info('====key is:{}'.format(pipeline_options.key))

    logging.info(pipeline_options.get_all_options())

    logging.info("=== sending to recipients:{}".format(pipeline_options.recipients))

    destination = 'gs://mm_dataflow_bucket/outputs/shareloader/pipeline_test_{}.csv'.format(datetime.now().strftime('%Y%m%d-%H%M'))

    logging.info('====== Destination is :{}'.format(destination))

    lines = (p
             | 'Get List of Tickers' >> ReadFromText(input_file)
             | 'Split fields'  >> beam.Map(lambda item:item.split(','))
             | 'Map to String' >> beam.MapTuple(lambda one, two, three: '<tr><td>{}</td><td>{}</td><td>{}</td></tr>'.format(one, two, three))
             | 'Combine' >>  beam.CombineGlobally(lambda elements: ''.join(elements))
             | 'Print out' >> beam.Map(print)
             #| 'Sending to Email' >> beam.ParDo(EmailSender(pipeline_options.recipients, pipeline_options.key))
             )
    result = p.run()

    print('Result is:{}'.format(result))
    return


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()