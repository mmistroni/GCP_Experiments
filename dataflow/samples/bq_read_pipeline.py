from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode
from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText,ReadAllFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import re, requests
from datetime import datetime, date
from collections import OrderedDict
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
import pandas_datareader.data as dr
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import date


class LogElements(beam.PTransform):
    class _LoggingFn(beam.DoFn):

        def __init__(self, prefix=''):
            super(LogElements._LoggingFn, self).__init__()
            self.prefix = prefix

        def process(self, element, **kwargs):
            print (self.prefix + str(element))
            yield element

    def __init__(self, label=None, prefix=''):
        super(LogElements, self).__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._LoggingFn(self.prefix))



def join_sinks(source1, source2):
    return (
            {'left': source1, 'right':  source2}
            | 'LeftJoiner: Combine' >> beam.CoGroupByKey()
    )



def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    logging.info(pipeline_options.get_all_options())

    #sink = beam.Map(print)
    query = "select TICKER, SUM(COUNT) AS TOTAL FROM datascience-projects.gcp_edgar.form_13hf_daily GROUP BY TICKER "

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (p
                 | "ReadFromBigQuery" >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=True))
                 | "Extract text column" >> beam.Map(lambda row: (row.get("TICKER"), row.get('TOTAL')))
                 | LogElements()
                 )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()