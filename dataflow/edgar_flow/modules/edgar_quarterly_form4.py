import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date, datetime
from .edgar_daily_form4 import run_my_pipeline, filter_form_4,\
                        send_email, write_to_form4_bq, enhance_form_4, XyzOptions
EDGAR_QUARTERLY_URL = 'https://www.sec.gov/Archives/edgar/full-index/{year}/{quarter}/master.idx'

def find_quarter_urls():
    quarter_dictionary = {
        "QTR1": [1, 2, 3],
        "QTR2": [4, 5, 6],
        "QTR3": [7, 8, 9],
        "QTR4": [10, 11, 12]
    }
    current_date = date.today()
    current_month = current_date.month
    current_year = current_date.year
    logging.info('Fetching previou for month:{}'.format(current_month))
    available_quarters = {k for i in range(1, date.today().month + 1) for k, v in quarter_dictionary.items() if i in v}

    urls = list(map(lambda qtr: EDGAR_QUARTERLY_URL.format(year=current_year, quarter=qtr), available_quarters))
    logging.info('Processing from following urls:{}'.format(urls))
    return urls

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    logging.info('starting pipeline..')
    with beam.Pipeline(options=pipeline_options) as p:
        source = (p  | 'Startup' >> beam.Create(['start_token'])
                     |'Add current Urls' >> beam.Map(find_quarter_urls)
                  )
        lines = run_my_pipeline(source)
        form4 = filter_form_4(lines)
        enhanced_data = enhance_form_4(form4)
        send_email(enhanced_data, pipeline_options)
        write_to_form4_bq(enhanced_data)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
