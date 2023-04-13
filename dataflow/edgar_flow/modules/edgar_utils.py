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
from datetime import datetime
from lxml import etree
from io import StringIO, BytesIO


CATEGORIES = set(
    ['issuerTradingSymbol', 'transactionCode', 'transactionShares',
     'sharesOwnedFollowingTransaction', 'transactionPricePerShare', 'isDirector'])

def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        del element.getparent()[0]


def fast_iter2(context):
    test_dict = {}
    for event, element in context:
        if element.tag in CATEGORIES:
            if element.tag == 'issuerTradingSymbol':
                test_dict['Symbol'] = element.text
            elif element.tag == 'isDirector':
                test_dict['IsDirector'] = 'Yes' if element.text == "1" else ''
            elif element.tag == 'transactionCode':
                test_dict['trans_code'] = element.text
            elif element.tag == 'transactionShares':
                vals = [author.text for author in element.findall("value")]
                test_dict['shares'] = float(vals[0]) if vals else 0
            elif element.tag == 'sharesOwnedFollowingTransaction':
                vals = [shares.text for shares in element.findall("value")]
                test_dict['sharesFollowingTransaction'] = float(vals[0]) if vals else 0
            elif element.tag == 'transactionPricePerShare':
                vals = [shares.text for shares in element.findall("value")]
                test_dict['transactionPricePerShare'] = float(vals[0]) if vals else 0
            clear_element(element)
    return test_dict

def get_period_of_report(data):
    data = data.replace('\n', '')
    subset = data[data.find('<periodOfReport>')+16: data.find("</periodOfReport>")]
    return subset

class ReadRemote(beam.DoFn):
    def process(self, element):
        try:
            req = urllib.request.Request(
                element,
                headers={
                    'User-Agent': 'WCorp Services mmistroni@gmail.com'
                }
            )
            data = urllib.request.urlopen(req)  # it's a file like object and works just like a file
            data =  [line for line in data]
            return data
        except Exception as e:
            return []

class ParseForm13F(beam.DoFn):


    def get_filing_data(self, content):
        import re
        xml_str = content[content.rfind('<XML>') + 5: content.rfind("</XML>")].strip()
        tree = ElementTree.ElementTree(ElementTree.fromstring(xml_str))
        root = tree.getroot()
        ns = re.match(r'{.*}', root.tag).group(0)
        ts = root.findall('.//{ns}infoTable')
        return [(table.find(f'.//{ns}cusip').text,
                 table.find(f'.//{ns}sshPrnamt').text) for table in root]



    def get_reporter(self, content):
        xml_str = content[content.find('<XML>') + 5: content.find("</XML>")].strip()
        tree = ElementTree.ElementTree(ElementTree.fromstring(xml_str))
        root = tree.getroot()
        ns = re.match(r'{.*}', root.tag).group(0)
        reporter = root.find(f'.//{ns}credentials/{ns}cik')
        rep_text = reporter.text
        return rep_text


    def open_url_content(self, file_path):
        import requests
        return requests.get(file_path,
                            headers={
                                'User-Agent': 'WCorp Services mmistroni@gmail.com'
                            })

    def get_period_of_report(self, content):
        return get_period_of_report(content)

    def get_cusips(self, content):
        data = content.text
        data = data.replace('\n', '')
        subset = data[data.rfind('<XML>') + 5: data.rfind("</XML>")].strip()
        from xml.etree import ElementTree
        tree = ElementTree.ElementTree(ElementTree.fromstring(subset))
        root = tree.getroot()
        all_dt = [child.text for infoTable in root.getchildren() for child in infoTable.getchildren()
                  if 'cusip' in child.tag]
        return all_dt

    def process(self, element):
        cob_dt, file_url = element
        try:
            cob_dt = datetime.strptime(cob_dt, '%Y%m%d').strftime('%Y-%m-%d')
            
        except:
            cob_dt = datetime.strptime(cob_dt, '%Y-%m-%d').strftime('%Y-%m-%d')

        try:
            logging.info('Opening:{}'.format(file_url))
            file_content = self.open_url_content(file_url)
            all_cusips = self.get_filing_data(file_content.text)
            period_of_report = self.get_period_of_report(file_content.text)
            reporter = self.get_reporter(file_content.text)
            mapped = list(map(lambda item: (cob_dt, period_of_report, item[0], item[1], reporter), all_cusips))
            return mapped
        except Exception as e:
            logging.info('could not fetch data from {}:{}'.format(element, str(e)))
            return None


class ParseForm4(beam.DoFn):

    def open_url_content(self, file_path):
        import requests
        return requests.get(file_path,
                            headers={
                                'User-Agent': 'WCorp Services mmistroni@gmail.com'
                            })

    def get_transaction_codes(self, root):
        tcodes = root.findall(".//transactionCode")
        return [tcode.text for tcode in tcodes]

    def get_shares_acquired(self, root):
        ts = root.findall(".//transactionAmounts/transactionShares/value")
        return sum([float(t.text) for t in ts])

    def get_shares_posttransaction(self, root):
        ts = root.findall(".//postTransactionAmounts/sharesOwnedFollowingTransaction/value")
        return sum([float(t.text) for t in ts])

    def get_transaction_price_per_share(self, root):
        ts = root.findall(".//transactionAmounts/transactionPricePerShare/value")
        return sum([float(t.text) for t in ts])

    def get_purchase_transactions(self, subset, cob_dt):
        tree = ElementTree.ElementTree(ElementTree.fromstring(subset))
        root = tree.getroot()
        trading_symbol = [child.text for infoTable in root.getchildren() for child in infoTable.getchildren()
                          if 'issuerTradingSymbol' in child.tag][0]
        tcodes = self.get_transaction_codes(root)
        # print('TCODES:{}'.format(tcodes))
        purchases = [c for c in tcodes if 'A' or 'P' in c]
        if purchases:
            shares_acquired = self.get_shares_acquired(root)
            shares_post_transaction  = self.get_shares_posttransaction(root)
            shares_pre_transaction = shares_post_transaction - shares_acquired
            share_increase = (shares_post_transaction - shares_pre_transaction) / shares_pre_transaction
            transaction_price = self.get_transaction_price_per_share(root)
            res = ((cob_dt, trading_symbol), shares_acquired, share_increase, transaction_price)
            return

    def parse_xml(self, tree):
        return fast_iter2(tree)


    def get_purchase_transactions_2(self, content, cob_dt, file_content):
        some_file_like = BytesIO(content.encode('utf-8'))
        context = etree.iterparse(some_file_like)
        res = self.parse_xml(context)
        trans_code = res.get('trans_code', 'X')
        if trans_code in ['A' , 'P' ]:
            shares_acquired = res['shares']
            shares_post_transaction = res['sharesFollowingTransaction']
            shares_pre_transaction = shares_post_transaction - shares_acquired
            return ((cob_dt, res['Symbol']), shares_acquired, res['sharesFollowingTransaction'], res['transactionPricePerShare'], file_content)

    def process(self, element):
        edgar_dt, file_url = element
        try:
            cob_dt = datetime.strptime(edgar_dt, '%Y%m%d').strftime('%Y-%m-%d')
        except:
            cob_dt = datetime.strptime(edgar_dt, '%Y-%m-%d').strftime('%Y-%m-%d')
        try:
            file_content = self.open_url_content(file_url)
            data = file_content.text
            data = data.replace('\n', '')
            if data.rfind('<XML>') < 0:
                return [((cob_dt, 'N/A'), 0, 0, 0)]
            else:
                subset = data[data.rfind('<XML>') + 5: data.rfind("</XML>")]
                trading_symbols_tpl = self.get_purchase_transactions_2(subset, cob_dt, file_url)
                if trading_symbols_tpl:
                    return  [trading_symbols_tpl]
                return [((cob_dt, 'N/A'), 0, 0, 0, file_url)]
        except Exception as e:
            print('Exceptin fo r:{}/{}:{}'.format(cob_dt, file_url, str(e)))
            return [((cob_dt, 'N/A'), 0, 0, 0, file_url)]

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

def cusip_to_ticker2(cusip, fmpkey):
    try:
        # print('Attempting to get ticker for {}'.format(cusip))
        cusip_url = "https://financialmodelingprep.com/api/v3/cusip/{}?apikey={}".format(cusip, fmpkey)
        req = requests.get(cusip_url).json()
        return req.get('ticker', '')
    except Exception as e:
        logging.info('Unable to retrieve ticker for {}'.format(cusip))
        return ''

def processUrl(url):
  if 'master.idx' in url:
    return url

def crawl(base_page):
  req=requests.get(base_page, headers={
                    'User-Agent': 'Sample Company Name AdminContact@<sample company domain>.com'
                })
  good_ones = []
  if req.status_code==200:
      html=BeautifulSoup(req.text,'html.parser')
      pages=html.find_all('a')
      for page in pages:
          url=page.get('href')
          res = processUrl(url)
          if res:
            full_url = '{}{}'.format(base_page, res)
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
    return edgar_year


class EdgarCombineFn(beam.CombineFn):
    def __init__(self):
        self.ROW_TEMPLATE = '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'


    def create_accumulator(self):
        return []

    def add_input(self, accumulator, input):
        accumulator.append(input)
        return accumulator

    def merge_accumulators(self, accumulators):
        print('Merging:{}'.format(accumulators))
        from functools import reduce
        return reduce(lambda acc, current: acc + current, accumulators)

    def extract_output(self, aggregated):
        print('Filtering only top 30 for:{}'.format(aggregated))
        sorted_accs = sorted(aggregated, key=lambda tpl: tpl[4], reverse=True)
        filtered = sorted_accs
        mapped =  map(lambda row: self.ROW_TEMPLATE.format(*row[1:]), filtered[0:20])
        return ''.join(mapped)

class EdgarCombineFnForm4(beam.CombineFn):
    def __init__(self):
        self.ROW_TEMPLATE = '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'
        # cob, ticker, shares, increase, trans price, volume
        ##cob, ticker, shares, increase, trans price, volume, url

    def create_accumulator(self):
        return []

    def add_input(self, accumulator, input):
        accumulator.append(input)
        return accumulator

    def merge_accumulators(self, accumulators):
        print('Merging:{}'.format(accumulators))
        from functools import reduce
        return reduce(lambda acc, current: acc + current, accumulators)

    def extract_output(self, aggregated):
        sorted_accs = sorted(aggregated, key=lambda tpl: tpl[5], reverse=True)
        filtered = sorted_accs
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

class EdgarEmailSender(beam.DoFn):
    def __init__(self, recipients, key, content):
        self.recipients = recipients.split(',')
        self.key = key
        self.content = content

    def _build_personalization(self, recipients):
        personalizations = []
        for recipient in recipients:
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
            from_email='mmistroni@gmail.com',
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

def get_edgar_daily_table_spec_form4():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='form_4_daily')

def get_edgar_daily_table_spec_form4_historical():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='form_4_daily_historical')








