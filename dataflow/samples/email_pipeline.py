from __future__ import absolute_import

import argparse
import logging
import re


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
import pandas_datareader.data as dr
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import date

#input_file = 'gs://mm_dataflow_bucket/inputs/shares.txt'


def map_to_dict(input_df):
  dd = input_df.to_dict()
  return dict(    (k.replace(' ', ''), [vl for vl in v.values()][0]) for k, v in dd.items())


def get_prices(ticker, start_date, end_date):
  try:
    print('Fetching {} from {} to {}'.format(ticker, start_date, end_date))
    df = dr.get_data_yahoo(ticker, start_date, end_date)
    df['Ticker'] = ticker
    return map_to_dict(df)
  except Exception as e:
    print('could not fetch {}'.format(ticker))


class XyzOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input_file', type=str,
                                           default_value='gs://dataflow-samples/shakespeare/kinglear.txt')


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

def add_year(item):
    return get_prices(item, date.today(), date.today())


def split_fields(line):
    print('processing {}'.format(line))
    return line.split(',')[0]


def get_edgar_table_schema():
  edgar_table_schema = 'Ticker:STRING,AdjClose:FLOAT64,Close:FLOAT64,High:FLOAT64,Low:FLOAT64,Open:FLOAT64,Volume:INT64'
  return edgar_table_schema

def get_edgar_table_spec():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='pipeline_tester')

def run_step_2(p, sink):
    result =  (p
            | 'Map to String' >> beam.Map(add_year)
            | sink
            )

def run_my_pipeline(p, options, sink):
    print('We got options:{}'.format(options))

    lines = (p
             | 'Split fields' >> beam.Map(split_fields)
             | 'Print out' >> beam.Map(logging.info)             )

def get_input_file(option_file):
    return option_file.get()


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    logging.info(pipeline_options.get_all_options())

    #sink = beam.Map(print)

    with beam.Pipeline(options=pipeline_options) as p:
        source = p | 'Get List of Tickers' >> ReadFromText(pipeline_options.input_file.get())
        sink = beam.io.WriteToBigQuery(
            get_edgar_table_spec(),
            schema=get_edgar_table_schema(),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        return run_my_pipeline(source, p.options, sink)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()