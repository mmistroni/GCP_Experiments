from __future__ import absolute_import

import logging
from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from .bq_utils import get_table_schema, get_table_spec, map_to_bq_dict
from datetime import date
from .news_util import find_news_scores_for_ticker, df_to_dict, NewsEmailSender, combine_news, stringify_news
from .bq_utils import get_news_table_schema, get_news_table_spec


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--sector', default='Utilities,Consumer Cyclical,Energy')
        parser.add_argument('--business_days', default=1)
        parser.add_argument('--key')




def map_to_bq_dict(original_dict):
    logging.info('... input dict is:{}'.format(original_dict))
    logging.info('input is of type:{}.hl{}'.format(type(original_dict), type(original_dict['headline'])))
    return dict(     RUN_DATE=date.today().strftime('%Y-%m-%d'),
                     TICKER=original_dict.get('ticker', 'NA'),
                     HEADLINE=original_dict['headline'][0:50],
                     SCORE=original_dict.get(0, 0))

def write_data(data, sink):
    return (data | 'MAP TO BigQuery' >> beam.Map(map_to_bq_dict)
                 | sink)

def prepare_for_big_query(dframes):
    return (dframes
            | 'Convert to Dictionary' >> beam.Map(df_to_dict)
            | 'Filter out Positive News' >> beam.Filter(lambda dct: dct.get(0,-1) > 0.5)

    )

def send_notification(list_of_dicts, options):
    return (list_of_dicts
            | 'Map to List of Strings' >> beam.Map(stringify_news)
            | 'Combining News' >> beam.CombineGlobally(combine_news)
            | 'SendEmail' >> beam.ParDo(NewsEmailSender(options.recipients, options.key))
            )


def find_news_for_ticker(tickers, bus_days):

    return (tickers
                | 'Find News' >> beam.Map(lambda tick: find_news_scores_for_ticker([tick], bus_days))
                | 'Filter out Nones' >> beam.Filter(lambda df: df is not None)
                )

def run_my_pipeline(source, options):
    sector = options.sector.split(',')
    logging.info('Finding news for sector:{}'.format(sector))
    return (source
                | 'Map to Tpl' >> beam.Map(lambda ln: ln.split(','))
                | 'Filter by Sector' >> beam.Filter(lambda tpl: sector.count(tpl[1]) > 0)
                | 'Map to Ticker Only' >> beam.Map(lambda tpl: tpl[0])
                )

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    logging.info('Fetching data for sectors {} '.format(pipeline_options.sector))

    with beam.Pipeline(options=pipeline_options) as p:
        source = p  | 'Read Source File' >> ReadFromText('gs://datascience-bucket-mm/all_sectors.csv')
        sink = beam.io.WriteToBigQuery(
            get_news_table_spec(),
            schema=get_news_table_schema(),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        tickers = run_my_pipeline(source, pipeline_options)
        news = find_news_for_ticker(tickers, pipeline_options.business_days)
        bq_data = prepare_for_big_query(news)
        write_data(bq_data, sink)
        send_notification(bq_data, pipeline_options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()