from __future__ import absolute_import

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from mypackage.obb_utils import OBBLoader
from mypackage.finviz_utils import get_leaps

class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')
        parser.add_argument('--input')
        parser.add_argument('--output')
        parser.add_argument('--period')
        parser.add_argument('--limit')
        parser.add_argument('--pat')

def run_obb_pipeline(p, key):
    
    return ( p
             | 'Start' >> beam.Create(get_leaps())
             | 'Mapping ticks' >> beam.Map(lambda d: d['Ticker'])
             | 'combining' >> beam.CombineGlobally(lambda x: ','.join(x))
             | 'Get all List' >> beam.ParDo(OBBLoader(key))

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

        if bool(pipeline_options.pat):
            logging.info('running OBB....')
            obb = run_obb_pipeline(p, pipeline_options.fmprepkey)
            obb | sink




