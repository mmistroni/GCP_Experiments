from __future__ import absolute_import

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from main_tester import run
from shareloader.modules.finviz_utils import FinvizLoader
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.bigquery import TableRowJsonCoder
from shareloader.modules.obb_utils import AsyncProcess

from datetime import date


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')
        parser.add_argument('--input')
        parser.add_argument('--output')
        parser.add_argument('--period')
        parser.add_argument('--limit')
        parser.add_argument('--pat')
        parser.add_argument('--runtype')


def get_bq_schema():
    field_dict =  {
        "cob": "DATE",
        "symbol": "STRING", "price": "FLOAT", "change": "FLOAT", "yearHigh": "FLOAT",
        "yearLow": "FLOAT", "marketCap": "INTEGER", "priceAvg50": "FLOAT", "priceAvg200": "FLOAT", "exchange": "STRING",
        "avgVolume": "INTEGER", "open": "FLOAT", "eps": "FLOAT",  "pe" : "FLOAT", "sharesOutstanding": "INTEGER",
        "institutionalOwnershipPercentage": "FLOAT", "epsGrowth": "FLOAT", "epsGrowth5yrs": "FLOAT",
        "OPERATING_INCOME_CAGR": "STRING",
        "positiveEps": "INTEGER", "positiveEpsLast5Yrs": "INTEGER",
        "netIncome": "INTEGER", "income_statement_date": "STRING",
        "debtOverCapital": "INTEGER", "enterpriseDebt": "FLOAT",
        "totalAssets": "INTEGER", "inventory": "INTEGER",
        "totalCurrentAssets": "INTEGER", "totalCurrentLiabilities": "INTEGER",
        "dividendPaid": "BOOLEAN", "dividendPaidEnterprise": "BOOLEAN",
        "dividendPayoutRatio": "FLOAT", "numOfDividendsPaid": "INTEGER",
        "returnOnCapital": "FLOAT",
        "peRatio": "FLOAT", "netProfitMargin": "FLOAT",
        "currentRatio": "FLOAT", "priceToBookRatio": "FLOAT",
        "grossProfitMargin": "FLOAT", "returnOnEquity": "FLOAT",
        "dividendYield": "FLOAT", "pegRatio": "FLOAT",
        "payoutRatio" : "FLOAT",
        "tangibleBookValuePerShare": "FLOAT", "netCurrentAssetValue": "FLOAT",
        "freeCashFlowPerShare": "FLOAT",
        "earningsYield": "FLOAT", "bookValuePerShare": "FLOAT",
        "canBuyAllItsStock": "FLOAT", "netQuickAssetPerShare": "FLOAT",
        "rsi": "FLOAT", "piotroskyScore": "INTEGER", "ticker": "String",
        "52weekChange": "FLOAT", "label": "STRING", "country": "STRING",
    }

    schemaFields = []
    for fname, ftype in field_dict.items():
        schemaFields.append({"name" : fname, "type" : ftype, "mode": "NULLABLE"})

    schema = {
        "fields": schemaFields
    }


    return schema

def get_finviz_schema():
    field_dict = {
        "symbol": "STRING",
        "marketCap": "FLOAT",
        "price": "FLOAT",
        "open": "FLOAT",
        "change": "FLOAT",
        "previousClose": "FLOAT",
        "exchange": "STRING",
        "country": "STRING",
        "ticker": "STRING",
        "cob"   : "DATE",
        "asodate" : "DATE"
    }

    schemaFields = []
    for fname, ftype in field_dict.items():
        schemaFields.append({"name": fname, "type": ftype, "mode": "NULLABLE"})

    schema = {
        "fields": schemaFields
    }

    return schema

def run_obb_pipeline(p, fmpkey):
    logging.info('Running OBB ppln')
    return ( p
             | 'Start' >> beam.Create(['AAPL,AMZN'])
             | 'Get all List' >> beam.ParDo(FinvizLoader(fmpkey))
             | 'Map to BQable' >> beam.Map(lambda d: map_to_bq_dict(d))

    )

def run_premarket_pipeline(p, fmpkey):
    logging.info('Running OBB ppln')
    return ( p
             | 'Start' >> beam.Create(['AAPL'])
             | 'Get all List' >> beam.ParDo(FinvizLoader(fmpkey, runtype='premarket'))
             | 'Map to BQable' >> beam.Map(lambda d: map_to_bq_dict(d))

    )


def map_to_bq_dict(input_dict):

    custom_dict = input_dict.copy()
    custom_dict['cob']  = date.today()
    custom_dict['ticker'] = None
    return custom_dict


def run_yfinance_pipeline(p):
    cob = date(2024, 9, 25)
    return  (p | 'Start' >> beam.Create(['AAPL'])
             | 'Run Loader' >> beam.ParDo(AsyncProcess({}, cob))
             )


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    logging.info('Starting tester pipeline')

    # connecting dataflow to http running on gcp
    # https://www.trycatchdebug.net/news/1314929/gcp-dataflow-and-http-server#:~:text=To%20connect%20a%20GCP%20Dataflow%20job%20to%20the,transform%20to%20fetch%20data%20from%20the%20HTTP%20server.
    # https://cloud.google.com/dataflow/docs/guides/routes-firewall
    '''
    With gcloud, you'll use the --subnetwork flag and specify the subnetwork URL in the format projects/your-project-id/regions/your-region/subnetworks/your-subnetwork-name.
        projects/datascience-projects/regions/us-central1/subnetworks/default
        regions/us-central1/subnetworks/default
    '''



    bq_sink = beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId="datascience-projects",
            datasetId='gcp_shareloader',
            tableId='finviz_selection'),
        schema=get_bq_schema(),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    finviz_sink = beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId="datascience-projects",
            datasetId='gcp_shareloader',
            tableId='finviz-premarket'),
        schema=get_finviz_schema(),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    with beam.Pipeline(options=pipeline_options) as p:
        sink = beam.Map(logging.info)

        if not pipeline_options.runtype:
            logging.info('running OBB....')
            obb = run_obb_pipeline(p, pipeline_options.fmprepkey)
            logging.info('printing to sink.....')
            obb | sink
            #logging.info('Storing to BQ')
            obb | bq_sink
        else:
            logging.info('Running premarket loader')
            obb = run_premarket_pipeline(p, pipeline_options.fmprepkey)
            obb | sink
            obb | finviz_sink

        yfinance = run_yfinance_pipeline(p)
        yfinance | sink











