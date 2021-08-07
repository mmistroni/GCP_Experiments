from __future__ import absolute_import
from bs4 import BeautifulSoup
import logging
from bs4 import BeautifulSoup
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import re, requests
from datetime import datetime, date
from collections import OrderedDict
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
import re



class MyOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--iexkey')



def get_all_sectors(sectors_url):
    logging.info('Getting all sectors from:{}'.format(sectors_url))
    page = requests.get(sectors_url)
    soup = BeautifulSoup(page.content, 'html')
    items = soup.find_all('a')
    return [(l.get_text(), 'https://www.stockmonitor.com' + l.get('href')) for l in items if l.get('href').startswith('/sector')]




def get_industry(tpl, token):
    logging.info('Getting Industry')
    lst = list(tpl)
    logging.info('tpl is:{}'.format(tpl))
    ticker = lst[0]
    industry = ''
    sic_code = ''
    try:
        data =  requests.get('https://cloud.iexapis.com/stable/stock/{}/company?token={}'.format(ticker, token))\
                            .json()
        industry = data.get('industry', '')
        sic_code = data.get('primarySicCode', '')
    except Exception as e:
        logging.info('Could not find industry for :{}:{}'.format(ticker, str(e)))

    lst += [industry, sic_code]
    res = tuple(lst)
    logging.info('Returning:{}'.format(res))
    return res

def parse_row(row, sector_name):
  cols = [td for td in row.find_all('td')]
  ticker = cols[1].a.get('href').split('/')[2]
  name = cols[2].get_text().strip()
  return (ticker, name, sector_name)

def get_stocks_for_sector(sector_tpl):
  logging.info('Gettign stocks for sector:{}'.format(sector_tpl))
  sector_name, sector_url = sector_tpl
  logging.info('Finding all stocks for :{} @ {}'.format(sector_name, sector_url))
  page = requests.get(sector_url)
  soup = BeautifulSoup(page.content, 'html')
  main_table = soup.find_all('table', {"class" : "table table-hover top-stocks"})[0]
  rows = [t for t in main_table.find_all('tr')][1:]
  return [parse_row(r, sector_name) for r in rows]

def write_to_bucket(lines):
    bucket_destination = 'gs://mm_dataflow_bucket/outputs/all_sectors_and_industries.csv'.format(
        datetime.now().strftime('%Y%m%d%H%M'))
    return (
            lines
            | 'Map to  String' >> beam.Map(lambda lst: ','.join([re.sub('\W', ' ',  str(i)) for i in lst]))

            | 'WRITE TO BUCKET' >> beam.io.WriteToText(bucket_destination, header='ticker,name,sector,industry,sic_code',
                                                       num_shards=1)
    )

def run_my_pipeline(p, options):
    lines = (p
             | 'Getting Sectors' >> beam.FlatMap(lambda url: get_all_sectors(url))
             | 'Get Stocks For Sector' >> beam.FlatMap(lambda tpl: get_stocks_for_sector(tpl))
             | 'Getting  Industry' >> beam.Map(lambda tpl: get_industry(tpl, options.iexkey))
             )
    return lines


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = MyOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        input = p  | 'get Sectors Url' >> beam.Create(['https://www.stockmonitor.com/sectors'])
        res = run_my_pipeline(input, pipeline_options)
        write_to_bucket(res)
    # Need to get sic codes from https://en.wikipedia.org/wiki/Standard_Industrial_Classification

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()