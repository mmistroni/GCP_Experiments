from __future__ import absolute_import

from apache_beam.options.pipeline_options import SetupOptions, DebugOptions
from datetime import datetime, date
import logging
import apache_beam as beam
import pandas as pd
from datetime import date
from apache_beam.options.pipeline_options import PipelineOptions
from .superperf_metrics import get_descriptive_and_technical
from .marketstats_utils import get_all_stocks
from apache_beam.io.gcp.internal.clients import bigquery

'''
Further source of infos
https://medium.com/@mancuso34/building-all-in-one-stock-economic-data-repository-6246dde5ce02
'''
class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')
        parser.add_argument('--iistocks')
        parser.add_argument('--microcap')
        parser.add_argument('--probe')
        parser.add_argument('--split')


class PremarketLoader(beam.DoFn):
    #'https://medium.datadriveninvestor.com/how-to-create-a-premarket-watchlist-for-day-trading-263a760a31db'
    def __init__(self, key, microcap_flag=True, split=''):
        self.key = key
        self.microcap_flag = microcap_flag
        self.split = split


    def process(self, elements):
        all_dt = []
        tickers_to_process = elements.split(',')
        logging.info('Ticker to process:{len(tickers_to_process}')

        excMsg = ''
        isException = False

        for idx, ticker in enumerate(tickers_to_process):
            # Not good. filter out data at the beginning to reduce stress load for rest of data
            try:
                '''
                Float ≤ 20M.
                The Relative Volume (RVOL) should be equal to or greater than 2. RVOL is the average volume (over 15 or 60 days) divided by the day volume.
                $1.5 ≤ price ≤ $10
                The stocks should be a Gapper and the percentage of change should be greater than %5 if it’s a bear market and %10 if it’s a bull market. A Gapper means there is a gap between yesterday’s close and today’s open.
                The stock should have news or a catalyst for that high relative volume and gap.
                Volume ≥ 100K'''
                descr_and_tech = get_descriptive_and_technical(ticker, self.key)

                if descr_and_tech['open'] is not None \
                    and descr_and_tech['price'] is not None \
                    and descr_and_tech['sharesOutstanding'] is not None \
                    and descr_and_tech['volume'] is not None \
                    and descr_and_tech['avgVolume'] is not None \
                    and descr_and_tech['volume'] > 0 \
                    and descr_and_tech['previousClose'] > 0:
                    logging.info(f'Checks proceed for {ticker}')
                    # checking pct change
                    rVol = descr_and_tech['avgVolume'] /  descr_and_tech['volume']
                    change = descr_and_tech['open'] / descr_and_tech['previousClose']
                    vol = descr_and_tech['volume']
                    price = descr_and_tech['price']

                    if rVol >=2 and change >=0.05 \
                        and 1.5 < price <= 10:
                        print(f'Adding:{descr_and_tech}')
                        all_dt.append(descr_and_tech)
            except Exception as e:
                excMsg = f"{idx/len(tickers_to_process)}Failed to process fundamental loader for {ticker}:{str(e)}"
                isException = True
                break
        if isException:
            raise Exception(excMsg)
        return all_dt

def combine_tickers(input):
    return ','.join(input)



def write_to_bucket(lines, sink):
    return (
            lines | 'Writing to bucket' >> sink
    )

def extract_data_pipeline(p, fmpkey):
    return (p
            | 'Reading Tickers' >> beam.Create(get_all_stocks(fmpkey))
            | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(PremarketLoader(fmpkey))
    )

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    pipeline_options = XyzOptions()

    timeout_secs = 18400
    experiment_value = f"max_workflow_runtime_walltime_seconds={timeout_secs}"
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(DebugOptions).add_experiment(experiment_value)

    test_sink =beam.Map(logging.info)

    with beam.Pipeline(options=pipeline_options) as p:
        data = extract_data_pipeline(p, pipeline_options.fmprepkey)

        data | test_sink







