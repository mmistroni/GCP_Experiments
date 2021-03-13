import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date, datetime
from .edgar_daily_form4 import run_my_pipeline, filter_form_4,\
                        send_email, write_to_form4_bq, enhance_form_4, XyzOptions, write_to_form4_bucket
from .edgar_utils import  get_edgar_table_schema, get_edgar_table_schema_form4,\
            get_edgar_daily_table_spec, get_edgar_daily_table_spec_form4,get_edgar_daily_table_spec_form4_historical

from .price_utils import get_current_price
from apache_beam.io.gcp.internal.clients import bigquery
EDGAR_QUARTERLY_URL = 'https://www.sec.gov/Archives/edgar/full-index/{year}/{quarter}/master.idx'

class QuarterlyForm4Options(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--quarter', type=str,
                                           default='QTR1')
        parser.add_value_provider_argument('--year', type=str,
                                           default='2020')

def find_quarter_urls(token, options):
    quarter_dictionary = {
        "QTR1": [1, 2, 3],
        "QTR2": [4, 5, 6],
        "QTR3": [7, 8, 9],
        "QTR4": [10, 11, 12]
    }
    qtr = options.quarter.get()
    yr = options.year.get()
    logging.info('--- from pipeine options we got year:{}, qtr:{}'.format(qtr, yr))
    #available_quarters = {k for i in range(1, date.today().month + 1) for k, v in quarter_dictionary.items() if i in v}
    urls = [EDGAR_QUARTERLY_URL.format(year=yr, quarter=qtr)]
    logging.info('Processing from following urls:{}'.format(urls))
    return urls



'''
def write_to_form4_bq(lines):
    big_query = (
            lines
            | 'Map to BQ FORM4 Dict' >> beam.Map(lambda tpl: dict(COB=tpl[0],
                                                                       TICKER=tpl[2],
                                                                  COUNT=tpl[3],
                                                                  PRICE=float(tpl[4]),
                                                                  VOLUME=float(tpl[5])))
            | 'Write to BigQuery F4' >> beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId="datascience-projects",
            datasetId='gcp_edgar',
            tableId='form_4_daily_quarterly'),
        schema='COB:STRING,TICKER:STRING,COUNT:INTEGER,PRICE:FLOAT,VOLUME:FLOAT',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))
'''

def write_to_form4_bucket_quarterly(lines, quarter):

    bucket_destination = 'gs://mm_dataflow_bucket/outputs/edgar_quarterly_form4_{}_{}.csv'.format(
        quarter, datetime.now().strftime('%Y%m%d%H%M'))
    return (
            lines
            | 'Map to  String_{}'.format(quarter) >> beam.Map(lambda lst: ','.join([str(i) for i in lst]))
            # cob, ticker, shares, increase, trans price, volume

            | 'WRITE TO BUCKET_{}'.format(quarter) >> beam.io.WriteToText(bucket_destination, header='cob,ticker,shares,increase,trans_price,volume,filing_file',
                                                       num_shards=1)


    )





def run_for_quarter(p, quarter):
    qtr_url = 'https://www.sec.gov/Archives/edgar/full-index/2020/{}/master.idx'.format(quarter)
    source = (p | 'Startup_{}'.format(quarter) >> beam.Create([qtr_url])
                  )

    lines = run_my_pipeline(source, quarter)
    form4 = filter_form_4(lines, quarter)
    enhanced_data = enhance_form_4(form4, quarter)
    write_to_form4_bucket_quarterly(enhanced_data, quarter)


def run(argv=None, save_main_session=True):

    # Look for form N-23C3B

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = QuarterlyForm4Options()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    logging.info('starting pipeline..')
    with beam.Pipeline(options=pipeline_options) as p:
        run1 = run_for_quarter(p, 'QTR2')
        run2 = run_for_quarter(p, 'QTR3')
        run4 = run_for_quarter(p, 'QTR4')
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
