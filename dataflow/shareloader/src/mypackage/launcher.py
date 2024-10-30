from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime, date


class AbcOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--key')
        parser.add_argument('--fredkey')
        parser.add_argument('--sendgridkey')
        parser.add_argument('--recipients', default='mmistroni@gmail.com')



def run_vix(p, key):
    return (p | 'start run_vix' >> beam.Create(['20210101'])
            | 'remap vix' >> beam.Map(
                lambda d: {'AS_OF_DATE': date.today().strftime('%Y-%m-%d'), 'LABEL': 'VIX', 'VALUE': str(d)})
            )


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    pipeline_options = AbcOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    debugSink = beam.Map(logging.info)

    with beam.Pipeline(options=pipeline_options) as p:
        fmp_key = pipeline_options.key
        logging.info(pipeline_options.get_all_options())
        destination = 'gs://mm_dataflow_bucket/outputs/shareloader/{}_run_{}.csv'

        logging.info('====== Destination is :{}'.format(destination))
        logging.info('SendgridKey=={}'.format(pipeline_options.sendgridkey))

        p = run_vix(p, fmp_key)

        p | 'Writing to sink' >> debugSink



