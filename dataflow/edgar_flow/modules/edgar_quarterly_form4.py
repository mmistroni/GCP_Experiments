import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date, datetime
from .edgar_daily_form4 import run_my_pipeline, filter_form_4,\
                        send_email, write_to_form4_bq, enhance_form_4, XyzOptions
from .edgar_utils import  get_edgar_table_schema, get_edgar_table_schema_form4,\
            get_edgar_daily_table_spec, get_edgar_daily_table_spec_form4,get_edgar_daily_table_spec_form4_historical



EDGAR_QUARTERLY_URL = 'https://www.sec.gov/Archives/edgar/full-index/{year}/{quarter}/master.idx'


def find_quarter_urls(token, quarters):
    quarter_dictionary = {
        "QTR1": [1, 2, 3],
        "QTR2": [4, 5, 6],
        "QTR3": [7, 8, 9],
        "QTR4": [10, 11, 12]
    }
    logging.info('--- from pipeine options we got quarters:{}'.format(quarters))
    current_date = date.today()
    current_month = current_date.month
    current_year = current_date.year
    prev_month = current_date.month -1
    logging.info('Current month:{}. prev omnth:{}'.format(current_month, prev_month))

    prev_qtrs = [k for k, v in quarter_dictionary.items()]
    logging.info('Selected quarter is:{}'.format(prev_qtrs))
    logging.info('Fetching previous quarter for month:{}'.format(current_month))

    #available_quarters = {k for i in range(1, date.today().month + 1) for k, v in quarter_dictionary.items() if i in v}
    urls = [EDGAR_QUARTERLY_URL.format(year=current_year, quarter=prev_qtr) for prev_qtr in prev_qtrs][2:4]
    logging.info('Processing from following urls:{}'.format(urls))
    return urls

def write_to_form4_bq_historical(lines):
    big_query = (
            lines
            | 'Map to BQ FORM4 Dict' >> beam.Map(lambda tpl: dict(COB=tpl[0],
                                                                       TICKER=tpl[2],
                                                                  COUNT=tpl[3]))
            | 'Write to BigQuery F4' >> beam.io.WriteToBigQuery(
            get_edgar_daily_table_spec_form4_historical(),
            schema=get_edgar_table_schema_form4(),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )

def write_to_form4_bucket(lines):
    bucket_destination = 'gs://mm_dataflow_bucket/outputs/edgar_quarterly_form4_{}.csv'.format(
        date.today().strftime('%Y%m%d'))

    return (
            lines
            | 'Map to BQ FORM4 Dict' >> beam.Map(lambda tpl: dict(COB=tpl[0],
                                                                  TICKER=tpl[2],
                                                                  COUNT=tpl[3]))

            | 'WRITE TO BUCKET' >> beam.io.WriteToText(bucket_destination, header='date,ticker,count',
                                                       num_shards=1)


    )



def run(argv=None, save_main_session=True):

    # Look for form N-23C3B

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    logging.info('starting pipeline..')
    with beam.Pipeline(options=pipeline_options) as p:
        source = (p  | 'Startup' >> beam.Create(find_quarter_urls('foo', pipeline_options.quarters.get()))

                  )
        lines = run_my_pipeline(source)
        form4 = filter_form_4(lines)
        enhanced_data = enhance_form_4(form4)
        write_to_form4_bq_historical(enhanced_data)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
