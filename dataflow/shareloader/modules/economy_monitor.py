from __future__ import absolute_import

import argparse
import logging
import re
from pandas.tseries.offsets import BDay
from bs4 import BeautifulSoup# Move to aJob
import requests
import itertools
from apache_beam.io.gcp.internal.clients import bigquery

import requests
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
from .economic_utils import ECONOMIC_QUERY


ROW_TEMPLATE =  """<tr><td>{}</td>
                       <td>{}</td>
                       <td>{}</td>
                   </tr>"""

ECONOMIC_TEMPLATE = """<html>
                      <body>
                        <p> UK Price Statistics for {asOfDate} </p>
                        <br>
                        <br>
                        <br>
                        <table border="1">
                            <thead>
                                <tr>
                                    <th>AS OF DATE</th>
                                    <th>ITEM</th>
                                    <th>VALUE</th>
                                </tr>
                            </thead>
                            <tbody>
                                {tableOfData}
                            </tbody>
                        </table>
                    </body>
                </html>"""


def create_economic_data_ppln(p):
    cutoff_date_str = (date.today() - BDay(40)).date().strftime('%Y-%m-%d')
    logging.info('Cutoff is:{}'.format(cutoff_date_str))
    bq_sql = ECONOMIC_QUERY.format(oneMonthAgo=cutoff_date_str)
    logging.info('executing SQL :{}'.format(bq_sql))
    return (p | 'Reading-{}'.format(cutoff_date_str) >> beam.io.Read(
        beam.io.BigQuerySource(query=bq_sql, use_standard_sql=True))

            )

class EconomicOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--sendgridkey')
        parser.add_argument('--recipients', default='mmistroni@gmail.com')


class EconomicDataCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    logging.info('Adding{}'.format(input))
    logging.info('acc is:{}'.format(accumulator))
    row_acc = accumulator
    row_acc.append(ROW_TEMPLATE.format(input['AS_OF_DATE'], input['LABEL'], input['VALUE']))
    return row_acc

  def merge_accumulators(self, accumulators):
    return list(itertools.chain(*accumulators))

  def extract_output(self, sum_count):
    return ''.join(sum_count)

class EmailSender(beam.DoFn):
    def __init__(self, recipients, key):
        self.recipients = recipients.split(';')
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
        template = ECONOMIC_TEMPLATE
        asOfDateStr = date.today().strftime('%d %b %Y')
        content = template.format(asOfDate=asOfDateStr, tableOfData=element)
        logging.info('Sending \n {}'.format(content))
        message = Mail(
            from_email='mmistroni@gmail.com',
            to_emails=self.recipients,
            subject=f'UK Economy Good Prices for {asOfDateStr}',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        print(response.status_code, response.body, response.headers)




def send_email(pipeline, options):
    return (pipeline | 'SendEmail' >> beam.ParDo(EmailSender(options.recipients, options.sendgridkey))
             )

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    pipeline_options = EconomicOptions()

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:

        weeklyPipeline = create_economic_data_ppln(p)
        bqSink = beam.Map(logging.info)

        weeklyEconomicPipeline = (weeklyPipeline | 'combining' >> beam.CombineGlobally(EconomicDataCombineFn()))

        (weeklyEconomicPipeline | 'Mapping' >> beam.Map(
                                    lambda element: ECONOMIC_TEMPLATE.format(asOfDate=date.today(), tableOfData=element))

                                | bqSink)

        send_email(weeklyEconomicPipeline, pipeline_options)

