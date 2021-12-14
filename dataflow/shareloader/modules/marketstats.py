from __future__ import absolute_import

import argparse
import logging
import re
from pandas.tseries.offsets import BDay
from bs4 import BeautifulSoup# Move to aJob
import requests
from itertools import chain
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
from  .marketstats_utils import is_above_52wk,get_prices,MarketBreadthCombineFn, get_all_stocks, is_below_52wk,\
                            combine_movers,get_prices2, get_vix, ParsePMI, get_all_us_stocks2,\
                            get_all_prices_for_date, InnerJoinerFn, create_bigquery_ppln,\
                            ParseManufacturingPMI

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


def run_pmi(p):
    return (p | 'startstart' >> beam.Create(['20210101'])
                    | 'pmi' >>   beam.ParDo(ParsePMI())
                    | 'remap  pmi' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'PMI', 'VALUE' : d['Last']})
            )

def run_manufacturing_pmi(p):
    return (p | 'startstart' >> beam.Create(['20210101'])
                    | 'pmi' >>   beam.ParDo(ParseManufacturingPMI())
                    | 'remap  pmi' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'MANUFACTURING-PMI', 'VALUE' : d['Last']})
            )



def run_vix(p, key):
    return (p | 'start' >> beam.Create(['20210101'])
                    | 'vix' >>   beam.Map(lambda d:  get_vix(key))
                    | 'remap vix' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'), 'LABEL' : 'VIX', 'VALUE' : str(d)})
            )

def run_exchange_pipeline(p, key, exchange):
    all_us_stocks = list(map(lambda t: (t, {}), get_all_us_stocks2(key, exchange)))
    asOfDate = (date.today() - BDay(1)).date()
    prevDate = (asOfDate - BDay(1)).date()



    dt = get_all_prices_for_date(key, asOfDate.strftime('%Y-%m-%d'))
    ydt = get_all_prices_for_date(key, prevDate.strftime('%Y-%m-%d'))

    filtered = [(d['symbol'], d)  for d in dt]
    y_filtered = [(d['symbol'], {'prevClose': d['close']}) for d in ydt]

    tmp = [tpl[0] for tpl in all_us_stocks]
    fallus = [tpl for tpl in filtered if tpl[0] in tmp]
    yfallus = [tpl for tpl in y_filtered if tpl[0] in tmp]


    pcoll1 = p | f'Create coll1={exchange}' >> beam.Create(all_us_stocks)
    pcoll2 = p | f'Create coll2={exchange}' >> beam.Create(fallus)
    pcoll3 = p | f'Crete ydaycoll={exchange}' >> beam.Create(yfallus)

    pcollStocks = pcoll2 | f'Joining y{exchange}' >> beam.ParDo(InnerJoinerFn(),
                                                     right_list=beam.pvalue.AsIter(pcoll3))

    return  (
                    pcoll1
                    | f'InnerJoiner: JoinValues {exchange}' >> beam.ParDo(InnerJoinerFn(),
                                                              right_list=beam.pvalue.AsIter(pcollStocks))
                    | f'Map to flat tpl {exchange}' >> beam.Map(lambda tpl: (tpl[0], tpl[1]['close'], tpl[1]['close'] - tpl[1]['prevClose']))
                    | f'Combine MarketBreadth Statistics {exchange}' >> beam.CombineGlobally(MarketBreadthCombineFn())
                    | f'mapping {exchange}' >> beam.Map(lambda d: {'AS_OF_DATE' : date.today().strftime('%Y-%m-%d'),
                                                        'LABEL' : '{}_{}'.format(exchange.upper(), d[0:d.find(':')]),
                                                       'VALUE' : d[d.rfind(':')+1:]})
                    
            )


def see_what_is_inside(input):
    logging.info('---- we got:{}'.format(input))


def run_prev_dates_statistics(p) :
    # Need to amend the query to order by asofdate sc
    vbqp = (p | beam.Create([('------- ', 'LAST 5 DAYS PERFORMANCE', '--------')])
    )
    nysebqp = ( create_bigquery_ppln(p, 'NEW YORK STOCK EXCHANGE_MARKET BREADTH')
               | 'map to tpl2' >> beam.Map(lambda d: ( d['AS_OF_DATE'], d['LABEL'], d['VALUE'] ))
    )
    
    return nysebqp
                

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:

        iexapi_key = pipeline_options.key
        logging.info(pipeline_options.get_all_options())
        current_dt = datetime.now().strftime('%Y%m%d-%H%M')
        
        destination = 'gs://mm_dataflow_bucket/outputs/shareloader/{}_run_{}.csv'
        donefile = 'gs://mm_dataflow_bucket/outputs/shareloader/{}_run_{}.done'

        logging.info('====== Destination is :{}'.format(destination))
        logging.info('SendgridKey=={}'.format(pipeline_options.sendgridkey))
        
        bq_sink = beam.io.WriteToBigQuery(
            bigquery.TableReference(
                projectId="datascience-projects",
                datasetId='gcp_shareloader',
                tableId='market_stats'),
            schema='AS_OF_DATE:STRING,LABEL:STRING,VALUE:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        
        logging.info('Run pmi')
        
        pmi_res = run_pmi(p)
        pmi_res | 'Writing to bq' >> bq_sink

        manuf_pmi_res = run_manufacturing_pmi()
        manuf_pmi_res | 'manuf pmi to sink' >> bq_sink

        vix_res = run_vix(p, iexapi_key)
        vix_res | 'vix to sink' >> bq_sink

        logging.info('Run NYSE..')
        nyse = run_exchange_pipeline(p, iexapi_key, "New York Stock Exchange")
        nyse | 'nyse to sink' >> bq_sink
        logging.info('Run Nasdaq..')
        nasdaq = run_exchange_pipeline(p, iexapi_key, "Nasdaq Global Select")
        nasdaq | 'nasdaq to sink' >> bq_sink

        statistics = run_prev_dates_statistics(p)
        final = (
                (manuf_pmi_res, pmi_res, vix_res, nyse, nasdaq)
                | 'FlattenCombine all' >> beam.Flatten()
                | 'Mapping to String' >> beam.Map(lambda data: '{}-{}:{}'.format(data['AS_OF_DATE'], data['LABEL'], data['VALUE']))
                | 'Combine' >> beam.CombineGlobally(lambda x: '<br><br>'.join(x))
                | 'SendEmail' >> beam.ParDo(EmailSender('mmistroni@gmail.com', pipeline_options.sendgridkey))

        )
        

        statistics_dest = 'gs://mm_dataflow_bucket/outputs/market_stats_{}'.format(date.today().strftime('%Y-%m-%d'))

        statistics_sink = beam.io.WriteToText(statistics_dest, header='date,label,value',
                                                       num_shards=1)
    


        logging.info('Running previous statistics...')
        (statistics | 'Printing out stats' >> beam.Map(logging.info))
        (statistics | 'Printing out to siknk' >> statistics_sink)
            

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()