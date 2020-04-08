from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode
from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import re, requests
from bs4 import BeautifulSoup

quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
full_dir = "https://www.sec.gov/Archives/edgar/full-index/{year}/{QUARTER}/"


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--abc')
        parser.add_argument('--xyz', default='end')


def get_edgar_urls(years: list):
    print('fetching master.idx for year {}'.format(years))
    idx_directories = [full_dir.format(year=year, QUARTER=qtr) for year in years for qtr in quarters]
    return ['{}'.format(edgar_dir) for edgar_dir in idx_directories]


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)

    print(pipeline_options.get_all_options())

    print("=== readign from textfile:{}".format(pipeline_options.abc))

    lines = (p
             | beam.Create(['One', 'two', 'Three']

                           )
             | beam.Map(print))

    result = p.run()

    return


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()