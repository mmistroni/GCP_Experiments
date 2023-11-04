from __future__ import absolute_import

import logging
from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date
from .marketstats_utils import get_all_us_stocks2, NewHighNewLowLoader

class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')


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

        data = run_my_pipeline(p, pipeline_options.fmprepkey)
        data | 'Writing to sink' >> sink
