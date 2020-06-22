import apache_beam as beam
from datetime import date
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import WriteToText
from apache_beam.io.textio import ReadAllFromText
import urllib
from collections import defaultdict
from datetime import date, datetime
from itertools import groupby
import requests
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import date, datetime
import re, requests
from bs4 import BeautifulSoup
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization



class ReadRemote(beam.DoFn):
    def process(self, element):
        try:
            logging.info('REadRemote processing///{}'.format(element))
            data = urllib.request.urlopen(element)  # it's a file like object and works just like a file
            data =  [line for line in data]
            logging.info('data has:{}'.format(len(data)))
            return data
        except Exception as e:
            logging.info('Error fetching {}'.format(element))
            return []

class ParseForm13F(beam.DoFn):

    def open_url_content(self, file_path):
        import requests
        return requests.get(file_path)

    def get_cusips(self, content):
        data = content.text
        data = data.replace('\n', '')
        subset = data[data.rfind('<XML>') + 5: data.rfind("</XML>")]
        from xml.etree import ElementTree
        tree = ElementTree.ElementTree(ElementTree.fromstring(subset))
        root = tree.getroot()
        all_dt = [child.text for infoTable in root.getchildren() for child in infoTable.getchildren()
                  if 'cusip' in child.tag]
        return all_dt

    def _group_data(self, lst):
        all_dict = defaultdict(list)
        if lst:
            logging.info('Attempting to group..')
            data = sorted(lst, key=lambda x: x)
            for k, g in groupby(data, lambda x: x):
                grp = len(list(g))
                if grp > 1:
                    print('{} has {}'.format(k, grp))
                all_dict[k].append(grp)

    def process(self, element):
        try:
            file_content = self.open_url_content(element)
            all_cusips = self.get_cusips(file_content)
            # self._group_data(all_cusips)
            # print('Found:{} in Processing {}'.format(len(all_cusips), element))
            return all_cusips
        except Exception as e:
            print('could not fetch data from {}:{}'.format(element, str(e)))
            return []


def format_string(input_str):
    return str(input_str.replace("b'", "").replace("'", "")).strip()


def cusip_to_ticker(cusip):
    try:
        # print('Attempting to get ticker for {}'.format(cusip))
        cusip_url = "https://us-central1-datascience-projects.cloudfunctions.net/cusip2ticker/{fullCusip}".format(
            fullCusip=cusip)
        # print('Opening:{}'.format(cusip_url))
        req = requests.get(cusip_url).json()
        ticker = req['ticker']
        return format_string(ticker)
    except Exception as e:
        print('Unable to retrieve ticker for {}'.format(cusip))
        return ''

def processUrl(url):
  if 'master.idx' in url:
    return url

def crawl(base_page):
  req=requests.get(base_page)
  good_ones = []
  if req.status_code==200:
      html=BeautifulSoup(req.text,'html.parser')
      pages=html.find_all('a')
      for page in pages:
          url=page.get('href')
          res = processUrl(url)
          if res:
            full_url = '{}{}'.format(base_page, res)
            print('Appending..:{}'.format(full_url))
            good_ones.append(full_url)
      return good_ones



def generate_master_urls(all_url):
    res = map(lambda u: crawl(u), all_url)
    pprint(res)
    from itertools import chain
    unpacked = chain(*res)
    return list(unpacked)


def generate_edgar_urls_for_year(year):
    test_urls = ['https://www.sec.gov/Archives/edgar/full-index/{}/QTR1/'
             ,'https://www.sec.gov/Archives/edgar/full-index/{}/QTR2/'
            # ,'https://www.sec.gov/Archives/edgar/full-index/{}/QTR3/',
            # ,'https://www.sec.gov/Archives/edgar/full-index/{}/QTR4/'
              ]
    urls = map(lambda b_url: b_url.format(year), test_urls)
    return generate_master_urls(urls)


def find_current_year(current_date):
    current_month = current_date.month
    edgar_year = current_date.year
    logging.info('Year to use is{}'.format(edgar_year))
    return edgar_year


class EdgarCombineFn(beam.CombineFn):
    def __init__(self):
        self.ROW_TEMPLATE = '<tr><td>{}</td><td>{}</td><td>{}</td></tr>'


    def create_accumulator(self):
        return ([])

    def add_input(self, accumulator, input):
        return accumulator + input

    def merge_accumulators(self, accumulators):
        return accumulators

    def extract_output(self, aggregated):
        logging.info('Filtering only top 30')
        sorted_accs = sorted(aggregated, key=lambda tpl: tpl[2], reverse=True)
        filtered = sorted_accs
        logging.info('Mapping now to string')
        mapped =  map(lambda row: self.ROW_TEMPLATE.format(*row), filtered)
        return ''.join(mapped)

#TODO this need to be replaced using IEXAPI instead of fmprep
def get_company_stats(tpl, apikey):
    name, ticker, count = tpl

    base_url = 'https://financialmodelingprep.com/api/v3/company/profile/{ticker}?apikey={key}'.format(
        ticker=ticker, key=apikey)
    try:
        data = requests.get(base_url).json()['profile']
        pdict = dict(PRICE=str(data['price']),
                     RANGE=data['range'],
                     BETA=str(data['beta']),
                     INDUSTRY=data['industry'],
                     TICKER=ticker)
        ratings = requests.get('https://financialmodelingprep.com/api/v3/company/rating/{}?apikey=79d4f398184fb636fa32ac1f95ed67e6'.format(ticker)).json()
        pdict['RATING'] = ratings.get("rating")['recommendation'] if ratings.get("rating") else 'N/A'

        metrics = requests.get(
            'https://financialmodelingprep.com/api/v3/company/discounted-cash-flow/{}?apikey=79d4f398184fb636fa32ac1f95ed67e6'.format(ticker)).json()
        pdict['DCF'] = string(metrics.get('dcf'))
    except Exception as  e:
        logging.info('Unable to find data for {}:{}'.format(str(tpl), str(e)))
        pdict = dict(PRICE='0.0',
                     RANGE='N/A',
                     BETA='N/A',
                     INDUSTRY='NOINDUSTRY',
                     TICKER=ticker,
                     RATING='N/A',
                     DCF='NA'
                      )
    pdict['CUSIP'] = name
    pdict['COUNT'] = count
    pdict['COB'] = datetime.now().strftime('%Y-%m-%d')
    return pdict


from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization


class EdgarEmailSender(beam.DoFn):
    def __init__(self, recipients, key, content):
        self.recipients = recipients.split(',')
        self.key = key
        self.content = content

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
        template = "<html><body><p>Edgar Monthly Results Available at {}</p></body></html>"
        content = template.format(self.content)
        print('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_cloud@mmistroni.com',
            to_emails=self.recipients,
            subject='Edgar Monthly Filings',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        print(response.status_code, response.body, response.headers)








