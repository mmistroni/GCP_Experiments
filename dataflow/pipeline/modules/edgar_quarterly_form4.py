import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date, datetime
from apache_beam.options.value_provider import RuntimeValueProvider
from .utility import returnSomething
from datetime import date
import pandas_datareader.data as dr
import requests
import urllib
import requests

EDGAR_QUARTERLY_URL = 'https://www.sec.gov/Archives/edgar/full-index/{year}/{quarter}/master.idx'


class ReadRemote(beam.DoFn):
    def process(self, element):
        headers={'User-Agent': 'WorldCorp Services info@worldcorpservices.com' ,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                    'Accept-Encoding': 'gzip, deflate',
                    'Host' : 'sec.gov',
                    'Accept-Language': 'en-US,en;q=0.8'}
        logging.info('xxxxREadRemote processing///{}'.format(element))
        logging.info('type of element:{}'.format(type(element)))
        logging.info('Headers are:{}'.format(headers))
        req = urllib.request.Request(element, headers=headers)    
        try:
            
            data = urllib.request.urlopen(req)  # it's a file like object and works just like a file
            data =  [line for line in data]
            logging.info('data has:{}'.format(len(data)))
            return data
        except Exception as e:
            logging.info('Trying different way...:{}'.format(str(e)))
            return []

class QuarterlyForm4Options(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--year', type=str)
        parser.add_value_provider_argument('--quarter', type=str)
        parser.add_value_provider_argument('--fmpkey', type=str)

class GenerateEdgarUrlFn(beam.DoFn):
    def __init__(self, templated_quarter, templated_year, fmpkey):
      self.templated_quarter = templated_quarter
      self.templated_year = templated_year
      self.fmpkey = fmpkey

    def process(self, edgar_url):
        logging.info('Quarter is:{}'.format(RuntimeValueProvider.get_value('quarter', str, 'notfound')))
        logging.info('Processing:{}'.format(edgar_url))
        logging.info('FMPrepKey:{}'.format(self.fmpkey))
        yield  edgar_url.format(self.templated_year.get(), self.templated_quarter.get())

def fetch_shares(input, fmpkey):
    logging.info('Fetching shares....using key:{}'.format(fmpkey))
    result = requests.get('https://financialmodelingprep.com/api/v3/quote/AAPL?apikey={}'.format(fmpkey)).json()
    return result


def write_to_form4_bucket_quarterly(lines, quarter, year):
    logging.info('xxxxQuarter is:{}'.format(RuntimeValueProvider.get_value('quarter', str, 'notfound')))
    logging.info('xxxyear is:{}'.format(RuntimeValueProvider.get_value('quarter', str, 'notfound')))

    destinationUrl = 'gs://mm_dataflow_bucket/outputs/edgar_quarterly_form4_{}_{}_{}.csv'.format(
            year.get(),
            quarter.get(), datetime.now().strftime('%Y%m%d%H%M'))

    logging.info('Writing to:{}'.format(destinationUrl))
    return (
            lines
            | 'Map to  String_{}'.format(quarter) >> beam.Map(lambda lst: ','.join([str(i) for i in lst]))
            | 'Map to something' >> beam.Map(lambda s: returnSomething(s))
            
            | 'WRITE TO BUCKET_{}'.format(quarter) >> beam.io.WriteToText(
        destinationUrl,header='cob,ticker,shares,increase,trans_price,volume,filing_file',
                                                       num_shards=1)


    )


def run_for_quarter(p, quarter, year, fmpkey):
    source =  (p | 'Startup_{}'  >> beam.Create(['https://www.sec.gov/Archives/edgar/full-index/{}/{}/master.idx'])
                | 'Geneeratex URL' >> beam.ParDo(GenerateEdgarUrlFn(quarter, year, fmpkey)) 
                | 'Logging out'  >> beam.Map(logging.info)
    )
    return source
    #lines = run_my_pipeline(source)
    #form4 = filter_form_4(lines, '{}_{}'.format(quarter, year))
    #enhanced_data = enhance_form_4(form4, '{}_{}'.format(quarter, year))
    #write_to_form4_bucket_quarterly(source, quarter, year)


def run(argv=None, save_main_session=True):

    # Look for form N-23C3B

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = QuarterlyForm4Options()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    logging.info('starting pipeline..')
    with beam.Pipeline(options=pipeline_options) as p:
        run0 = run_for_quarter(p, pipeline_options.quarter, pipeline_options.year, pipeline_options.fmpkey)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
