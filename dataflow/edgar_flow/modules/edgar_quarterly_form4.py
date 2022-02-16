import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date, datetime
from apache_beam.options.value_provider import RuntimeValueProvider
from .edgar_daily_form4 import run_my_pipeline, filter_form_4,\
                        send_email, write_to_form4_bq, enhance_form_4, XyzOptions, write_to_form4_bucket
from .edgar_utils import  get_edgar_table_schema, get_edgar_table_schema_form4,\
            get_edgar_daily_table_spec, get_edgar_daily_table_spec_form4,get_edgar_daily_table_spec_form4_historical

from .price_utils import get_current_price
from sendgrid import SendGridAPIClient
EDGAR_QUARTERLY_URL = 'https://www.sec.gov/Archives/edgar/full-index/{year}/{quarter}/master.idx'

class QuarterlyForm4Options(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--year', type=str)
        parser.add_value_provider_argument('--quarter', type=str)

class GenerateEdgarUrlFn(beam.DoFn):
    def __init__(self, templated_quarter, templated_year):
      self.templated_quarter = templated_quarter
      self.templated_year = templated_year

    def process(self, edgar_url):
        logging.info('Quarter is:{}'.format(RuntimeValueProvider.get_value('quarter', str, 'notfound')))
        logging.info('Processing:{}'.format(edgar_url))
        yield  edgar_url.format(self.templated_year.get(), self.templated_quarter.get())

def write_to_form4_bucket_quarterly(lines, quarter, year):
    logging.info('Quarter is:{}'.format(RuntimeValueProvider.get_value('quarter', str, 'notfound')))
    logging.info('year is:{}'.format(RuntimeValueProvider.get_value('quarter', str, 'notfound')))

    destinationUrl = 'gs://mm_dataflow_bucket/outputs/edgar_quarterly_form4_{}_{}_{}.csv'.format(
            quarter.get(),
            year.get(), datetime.now().strftime('%Y%m%d%H%M'))

    logging.info('Writing to:{}'.format(destinationUrl))
    return (
            lines
            | 'Map to  String_{}'.format(quarter) >> beam.Map(lambda lst: ','.join([str(i) for i in lst]))
            # cob, ticker, shares, increase, trans price, volume

            | 'WRITE TO BUCKET_{}'.format(quarter) >> beam.io.WriteToText(
        destinationUrl,header='cob,ticker,shares,increase,trans_price,volume,filing_file',
                                                       num_shards=1)


    )





def run_for_quarter(p, quarter, year):
    source =  (p | 'Startup_{}'  >> beam.Create(['https://www.sec.gov/Archives/edgar/full-index/{}/{}/master.idx'])
                | 'Geneerate URL' >> beam.ParDo(GenerateEdgarUrlFn(quarter, year))
    ) 
                

    lines = run_my_pipeline(source)
    form4 = filter_form_4(lines, '{}_{}'.format(quarter, year))
    enhanced_data = enhance_form_4(form4, '{}_{}'.format(quarter, year))
    write_to_form4_bucket_quarterly(enhanced_data, quarter, year)


def run(argv=None, save_main_session=True):

    # Look for form N-23C3B

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = QuarterlyForm4Options()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    logging.info('starting pipeline..')
    with beam.Pipeline(options=pipeline_options) as p:
        run0 = run_for_quarter(p, pipeline_options.quarter, pipeline_options.year)

