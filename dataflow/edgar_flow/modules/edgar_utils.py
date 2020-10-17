import apache_beam as beam
from xml.etree import ElementTree
from pprint import pprint
import urllib
import re, requests
from bs4 import BeautifulSoup
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
from apache_beam.io.gcp.internal.clients import bigquery



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

    def process(self, element):
        cob_dt, file_url = element
        try:
            file_content = self.open_url_content(file_url)
            all_cusips = self.get_cusips(file_content)
            mapped = list(map(lambda item: (cob_dt, item), all_cusips))
            logging.info('returning:{}'.format(list(mapped)))
            return mapped
        except Exception as e:
            logging.info('could not fetch data from {}:{}'.format(element, str(e)))
            return [(cob_dt, '')]


class ParseForm4(beam.DoFn):

    def open_url_content(self, file_path):
        import requests
        return requests.get(file_path)

    def get_transaction_codes(self, root):
        tcodes = root.findall(".//transactionCode")
        return [tcode.text for tcode in tcodes]

    def get_shares_acquired(self, root):
        ts = root.findall(".//transactionAmounts/transactionShares/value")
        return sum([int(t.text) for t in ts])

    def get_purchase_transactions(self, content):
        data = content.text
        data = data.replace('\n', '')
        subset = data[data.rfind('<XML>') + 5: data.rfind("</XML>")]
        tree = ElementTree.ElementTree(ElementTree.fromstring(subset))
        root = tree.getroot()
        trading_symbol = [child.text for infoTable in root.getchildren() for child in infoTable.getchildren()
                          if 'issuerTradingSymbol' in child.tag][0]
        tcodes = self.get_transaction_codes(root)
        # print('TCODES:{}'.format(tcodes))
        purchases = [c for c in tcodes if 'A' or 'P' in c]
        if purchases:
            logging.info('found purchase transction for {}'.format(trading_symbol))
            shares_acquired = self.get_shares_acquired(root)
            return (trading_symbol, shares_acquired)


    def process(self, element):
        cob_dt, file_url = element
        try:

            logging.info('Processing :{}={}'.format(cob_dt, file_url))
            file_content = self.open_url_content(file_url)
            trading_symbols_tpl = self.get_purchase_transactions(file_content)
            #print('Trarding symblsa re:{}'.format(trading_symbols))
            #print('Returning:{}'.format(result))
            if trading_symbols_tpl:
                return  [trading_symbols_tpl]
            return [('N/A', 0)]
        except Exception as e:
            print('Exceptin fo r:{}/{}:{}'.format(cob_dt, file_url, str(e)))
            return [('N/A', 0)]

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
            logging.info('Appending..:{}'.format(full_url))
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
             ,'https://www.sec.gov/Archives/edgar/full-index/{}/QTR3/'
             ,'https://www.sec.gov/Archives/edgar/full-index/{}/QTR4/'
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
        sorted_accs = sorted(aggregated, key=lambda tpl: tpl[3], reverse=True)
        filtered = sorted_accs
        logging.info('Mapping now to string')
        mapped =  map(lambda row: self.ROW_TEMPLATE.format(*row[1:]), filtered)
        return ''.join(mapped)

def get_company_stats(tpl, apikey):
    cob, name, ticker, count = tpl
    base_url = 'https://cloud.iexapis.com/stable/stock/{}/company?token={}'.format(ticker, apikey)
    price_url = 'https://cloud.iexapis.com/stable/stock/{symbol}/quote?token={token}'.format(symbol=ticker, token=apikey)
    #stats_url = 'https://cloud.iexapis.com/stable/stock/{}/stats?token={}'.format(ticker, apikey)
    pdict = dict()

    try:
        company_data = requests.get(base_url).json()
        pdict['INDUSTRY'] = company_data['industry']
    except Exception as e:
        pdict['INDUSTRY'] = base_url + str(e)
    try :
        prices_data = requests.get(price_url).json()
        pdict['PRICE'] = str(prices_data['iexClose'])
    except Exception as e:
        pdict['PRICE'] = price_url + str(e)
    try:
        #stats_data = requests.get(stats_url).json()
        pdict['RANGE'] = 'NA'#str(stats_data['ytdChangePercent'])
        pdict['BETA'] = 'NA'#str(stats_data['beta'])
    except Exception as e:
        pdict['RANGE'] = 'NA'#stats_url + str(e)
        pdict['BETA'] = 'NA'#stats_url + str(e)

    pdict['TICKER'] = ticker
    pdict['RATINGS']= 'N/A'
    pdict['DCF'] = 'N/A'
    pdict['CUSIP'] = name
    pdict['COUNT'] = count
    pdict['COB'] = cob
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

def get_edgar_table_schema():
  edgar_table_schema = 'COB:STRING,CUSIP:STRING,COUNT:INTEGER,TICKER:STRING'
  return edgar_table_schema

def get_edgar_daily_table_spec():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='form_13hf_daily')

def get_edgar_table_schema_form4():
  edgar_table_schema = 'COB:STRING,TICKER:STRING,COUNT:INTEGER'
  return edgar_table_schema

def get_edgar_daily_table_spec_form4():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='form_4_daily')









