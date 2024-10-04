from __future__ import absolute_import

import logging
from itertools import chain

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime, date

import requests


from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
from functools import reduce
import time


class MarketStatsCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, sum_count, input_data):
        holder = sum_count
        logging.info('Adding:{}'.format(input_data))
        holder.append(input_data)
        return holder

    def merge_accumulators(self, accumulators):
        return chain(*accumulators)

    def extract_output(self, sum_count):
        all_data = sum_count
        sorted_els = sorted(all_data, key=lambda t: t[0])
        mapped = list(map (lambda tpl: tpl[1], sorted_els))
        
        stringified = list(map(lambda x: '|{}|{}|{}|'.format(x['AS_OF_DATE'],
                                                           x['LABEL'],
                                                           x['VALUE']), mapped))
                
        
        logging.info('MAPPED IS :{}'.format(mapped))
        return stringified


class MarketStatsSinkCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, sum_count, input_data):
        holder = sum_count
        holder.append(input_data)
        return holder

    def merge_accumulators(self, accumulators):
        return chain(*accumulators)

    def extract_output(self, all_data):
        return [i for i in all_data]

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
            from_email='gcp_cloud_mm@outlook.com',
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
        parser.add_argument('--fredkey')
        parser.add_argument('--sendgridkey')
        parser.add_argument('--recipients', default='mmistroni@gmail.com')


def get_vix(key):
    try:
        base_url = 'https://financialmodelingprep.com/api/v3/quote-short/{}?apikey={}'.format('^VIX', key)
        logging.info('Url is:{}'.format(base_url))
        return requests.get(base_url).json()[0]['price']
    except Exception as e:
        logging.info(f'Exception in getting vix:{str(e)}')
        return 0.0



def run_vix(p, key):
    return (p | 'start run_vix' >> beam.Create(['20210101'])
                    | 'vix' >>   beam.Map(lambda d:  get_vix(key))
                    | 'remap vix' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'VIX', 'VALUE' : str(d)})
            )
def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    # Check  this https://medium.datadriveninvestor.com/markets-is-a-correction-coming-aa609fba3e34


    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    debugSink =  beam.Map(logging.info)


    with beam.Pipeline(options=pipeline_options) as p:

        iexapi_key = pipeline_options.key
        fred_key = pipeline_options.fredkey
        logging.info(pipeline_options.get_all_options())
        current_dt = datetime.now().strftime('%Y%m%d-%H%M')
        
        destination = 'gs://mm_dataflow_bucket/outputs/shareloader/{}_run_{}.csv'

        logging.info('====== Destination is :{}'.format(destination))
        logging.info('SendgridKey=={}'.format(pipeline_options.sendgridkey))

        debugSink = beam.Map(logging.info)

        vix_res = run_vix(p, iexapi_key)

        vix_res | debugSink
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
