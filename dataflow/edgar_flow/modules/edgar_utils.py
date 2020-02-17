import apache_beam as beam
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
import re, requests
from bs4 import BeautifulSoup


class ReadRemote(beam.DoFn):
    def process(self, element):
        print('REadRemote processing///{}'.format(element))
        data = urllib.request.urlopen(element)  # it's a file like object and works just like a file
        return [line for line in data]

class ParseForm13F(beam.DoFn):

    def open_url_content(self, file_path):
        import requests
        print('Attepmting to open:{}'.format(file_path))
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
            print('Attempting to group..')
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
    test_urls = ['https://www.sec.gov/Archives/edgar/full-index/{}/QTR1/',
             'https://www.sec.gov/Archives/edgar/full-index/{}/QTR2/',
             'https://www.sec.gov/Archives/edgar/full-index/{}/QTR3/',
             'https://www.sec.gov/Archives/edgar/full-index/{}/QTR4/']
    urls = map(lambda b_url: b_url.format(year), test_urls)
    return generate_master_urls(urls)




