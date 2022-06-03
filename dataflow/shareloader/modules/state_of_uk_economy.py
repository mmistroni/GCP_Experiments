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
import pandas as pd
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


import csv, urllib.request
import io
from urllib.request import Request, urlopen
import csv, requests
import codecs
from bs4 import BeautifulSoup
import requests

def fetchFruitAndVegPrices():
    baseUrl = 'https://www.gov.uk/government/statistical-data-sets/wholesale-fruit-and-vegetable-prices-weekly-average'
    req = requests.get(baseUrl)
    soup = BeautifulSoup(req.text, "html.parser")
    span = soup.find_all('span', {"class": "download"})[0]
    anchor = span.find_all('a', {"class": "govuk-link"})[0]
    link = anchor.get('href')
    r = requests.get(link).text
    dt = pd.read_csv(io.StringIO(r), header=0)
    dt['asOfDate'] = pd.to_datetime(dt['date'], infer_datetime_format=True)
    return dt[dt.asOfDate == dt.asOfDate.max()]

def get_petrol_prices():
    url = 'https://www.gov.uk/government/statistics/weekly-road-fuel-prices'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")
    span = soup.find_all('span', {"class": "download"})[0]
    anchor = span.find_all('a', {"class": "govuk-link"})[0]
    link = anchor.get('href')
    r = requests.get(link).text
    dt = pd.read_csv(io.StringIO(r), header=2)[-1:][['Date','ULSP', 'ULSD']]
    dt.set_index('Date', inplace=True)
    return dt

def get_latest_url():
    url = "https://cy.ons.gov.uk/datasets/online-job-advert-estimates/editions"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")
    anchor = soup.find_all('a', {"id": "edition-time-series"})[0]
    suffix = anchor.get('href')
    return f'https://download.ons.gov.uk/downloads{suffix}.csv'


def get_latest_jobs_statistics():
    latestUrl = get_latest_url()
    print(f'Latest URL from ONS is {latestUrl}')
    res = requests.get(latestUrl, headers={'User-Agent': 'Mozilla/5.0'})
    # 'https://download.ons.gov.uk/downloads/datasets/online-job-advert-estimates/editions/time-series/versions/20.csv'
    text = res.iter_lines()
    data = csv.reader(codecs.iterdecode(text, 'utf-8'), delimiter=',')
    headers = ['v4_1',	'Data Marking', 	'calendar-years',	'Time',	'uk-only',	'Geography', 'adzuna-jobs-category',	'AdzunaJobsCategory',	'week-number',	'Week']
    dataset = [d for d in data]
    jobs_dataset = pd.DataFrame(dataset, columns=headers)
    valid = jobs_dataset[(jobs_dataset.Time == str(date.today().year)) & (jobs_dataset.AdzunaJobsCategory.str.contains('Computing'))]
    valid[['wk', 'wkno']] = valid['week-number'].str.split(pat = '-', expand = True)
    filtered = valid[valid.v4_1.str.contains('.')]
    filtered['wkint'] = filtered.wkno.apply(lambda v: int(v))
    return filtered.sort_values(by=['wkint'])






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