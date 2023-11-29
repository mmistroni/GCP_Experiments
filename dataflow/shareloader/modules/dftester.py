from __future__ import absolute_import

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date
from .marketstats_utils import get_all_us_stocks2, NewHighNewLowLoader
from .dftester_utils import combine_tickers, DfTesterLoader, get_fields

class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')


def run_stocksel_pipeline(p, fmpKey, inputFile='gs://mm_dataflow_bucket/inputs/history_5y_tickers_US_with_sector_and_industry_reduced.csv',
                                    period='annual'):
    return (p
            | 'Reading Tickers' >> beam.io.textio.ReadFromText(inputFile)
            | 'Converting to Tuple' >> beam.Map(lambda row: row.split(','))
            | 'Extracting only ticker and Industry' >> beam.Map(lambda item: (item[0]))
            | 'Combining Tickers' >> beam.CombineGlobally(combine_tickers)
            | 'Run Loader' >> beam.ParDo(DfTesterLoader(fmpKey, period=period))
            )


def run(fund_data):
    logging.info('Running probe..,')
    logging.info('Returning')
    destination = 'gs://mm_dataflow_bucket/outputs/superperformers_probe_{}'.format(
        date.today().strftime('%Y-%m-%d %H:%M'))
    sink = beam.io.WriteToText(destination, num_shards=1)
    (fund_data | 'Writing to text sink' >> sink)




def run_my_pipeline(p, fmpkey):
    nyse = get_all_us_stocks2(fmpkey, "New York Stock Exchange")
    nasdaq = get_all_us_stocks2(fmpkey, "Nasdaq Global Select")
    full_ticks = '.'.join(nyse + nasdaq)

    return ( p
            | 'Start' >> beam.Create([full_ticks])
            | 'Get all List' >> beam.ParDo(NewHighNewLowLoader(fmpkey))

    )



def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    logging.info('Starting tester pipeline')

    with beam.Pipeline(options=pipeline_options) as p:
        sink = beam.Map(logging.info)

        destination =  'gs://mm_dataflow_bucket/outputs/dftester_{}'.format(date.today().strftime('%Y-%m-%d %H:%M'))
        bucketSink = beam.io.WriteToText(destination, num_shards=1, header=get_fields())

        data = run_stocksel_pipeline(p, pipeline_options.fmprepkey)
        data | 'Wrrting to bucket' >> bucketSink
