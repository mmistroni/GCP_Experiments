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
from  .marketstats_utils import is_above_52wk,get_prices,MarketBreadthCombineFn, get_all_stocks, is_below_52wk,\
                            combine_movers,get_prices2, get_vix, ParsePMI, get_all_us_stocks2,\
                            get_all_prices_for_date, InnerJoinerFn, create_bigquery_ppln,\
                            ParseManufacturingPMI,get_economic_calendar

from .mail_utils import STOCK_EMAIL_TEMPLATE
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization

ROW_TEMPLATE =  """<tr><td>{}</td>
                       <td>{}</td>
                       <td>{}</td>
                       <td>{}</td>
                       <td>{}</td>
                       <td>{}</td>
                       <td>{}</td>
                       <td>{}</td>
                       <td>{}</td>
                       <td>{}</td>
                       <td>{}</td>
                       </tr>"""


def create_monthly_data_ppln(p):
    cutoff_date_str = (date.today() - BDay(60)).date().strftime('%Y-%m-%d')
    logging.info('Cutoff is:{}'.format(cutoff_date_str))
    bq_sql = """SELECT TICKER, COUNT(*) as COUNTER FROM `datascience-projects.gcp_shareloader.stock_selection` 
        WHERE AS_OF_DATE > PARSE_DATE("%F", "{}") AND LABEL <> 'STOCK_UNIVERSE' GROUP BY TICKER 
  """.format(cutoff_date_str)
    logging.info('executing SQL :{}'.format(bq_sql))
    return (p | 'Reading-{}'.format(cutoff_date_str) >> beam.io.Read(
        beam.io.BigQuerySource(query=bq_sql, use_standard_sql=True))

            )



class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--sendgridkey')
        parser.add_argument('--recipients', default='mmistroni@gmail.com')


def send_email(pipeline, options):
    return (pipeline | 'SendEmail' >> beam.ParDo(EmailSender(options.recipients, options.sendgridkey))
             )



def kickoff_pipeline(weeklyPipeline, monthlyPipeline):

    wMapped = weeklyPipeline | 'MapWS' >> beam.Map(lambda dictionary: (dictionary['TICKER'],
                                                                               dictionary))

    mMapped = monthlyPipeline | 'MapM' >> beam.Map(lambda dictionary: (dictionary['TICKER'],
                                                                       dictionary))

    return (
            wMapped
            | 'InnerJoiner: JoinValues' >> beam.ParDo(InnerJoinerFn(),
                                                      right_list=beam.pvalue.AsIter(mMapped))
            | 'Map to flat tpl' >> beam.Map(lambda tpl: tpl[1])
            | 'Map to tuple' >> beam.Map(lambda row:(row['TICKER'], row['LABEL'], row['PRICE'], row['YEARHIGH'],
                                                     row['YEARLOW'], row['PRICEAVG50'], row['PRICEAVG200'],
                                                     row['BOOKVALUEPERSHARE'] , row['CASHFLOWPERSHARE'],
                                                     row['DIVIDENDRATIO'], row['COUNTER']))
    )


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:
        weeklyPipeline = create_weekly_data_ppln(p)
        monthlyPipeline = create_monthly_data_ppln(p)

        bqPipeline = kickoff_pipeline(weeklyPipeline, monthlyPipeline)

        bqSink = beam.Map(logging.info)

        weeklySelectionPipeline = (bqPipeline | 'combining' >> beam.CombineGlobally(StockSelectionCombineFn()))

        (weeklySelectionPipeline | 'Mapping' >> beam.Map(
                                    lambda element: STOCK_EMAIL_TEMPLATE.format(asOfDate=date.today(), tableOfData=element))

                                | bqSink)

        ## Send email now
        send_email(weeklySelectionPipeline, pipeline_options)



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()