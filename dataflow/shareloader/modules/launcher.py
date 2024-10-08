from __future__ import absolute_import

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from shareloader.modules.finviz_utils import FinvizLoader
from shareloader.modules.obb_utils import AsyncProcess, create_bigquery_ppln
from apache_beam.io.gcp.internal.clients import bigquery

from datetime import date
from shareloader.modules.superperformers import combine_tickers
import argparse


class MyPipelineOptions(PipelineOptions):
    @classmethod
    def from_dictionary(cls, options):
        pipeline_options = super().from_dictionary(options)
        pipeline_options.view_as(cls).fmprepkey = options.get('fmprepkey')
        pipeline_options.view_as(cls).input = options.get('input')
        pipeline_options.view_as(cls).output = options.get('output')
        pipeline_options.view_as(cls).period = options.get('period')
        pipeline_options.view_as(cls).limit = options.get('limit')
        pipeline_options.view_as(cls).pat = options.get('pat')  # Default to 4 workers
        pipeline_options.view_as(cls).runtype = options.get('runtype')
        return pipeline_options

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
             | 'OBBStart' >> beam.Create(['AAPL,AMZN'])
             | 'OBBGet all List' >> beam.ParDo(FinvizLoader(fmpkey))
             | 'OBBMap to BQable' >> beam.Map(lambda d: map_to_bq_dict(d))

    )

def run_premarket_pipeline(p, fmpkey):
    logging.info('Running OBB ppln')
    return ( p
             | 'PMStart' >> beam.Create(['AAPL'])
             | 'PMGet all List' >> beam.ParDo(FinvizLoader(fmpkey, runtype='premarket'))
             | 'PMMap to BQable' >> beam.Map(lambda d: map_to_bq_dict(d))

    )


def map_to_bq_dict(input_dict):

    custom_dict = input_dict.copy()
    custom_dict['cob']  = date.today()
    custom_dict['ticker'] = None
    return custom_dict


def run_yfinance_pipeline(p):
    cob = date.today()
    return  (p | 'yfStart' >> beam.Create(['AAPL'])
             | 'YFRun Loader' >> beam.ParDo(AsyncProcess({}, cob))
             )

def run_test_pipeline(p):
    cob = date.today()
    test_ppln = create_bigquery_ppln(p)
    return  (test_ppln
                | 'Maping BP ticker' >> beam.Map(lambda d: d['ticker'])
                | 'Filtering' >> beam.Filter(lambda tick: '.' not in tick and '-' not in tick)
                | 'Combine all tickers' >> beam.CombineGlobally(combine_tickers)
               | 'Plus500YFRun' >> beam.ParDo(AsyncProcess({}, cob))
             )

def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--fmprepkey')
  parser.add_argument('--input')
  parser.add_argument('--output')
  parser.add_argument('--period')
  parser.add_argument('--limit')
  parser.add_argument('--pat')
  return parser.parse_known_args(argv)



def run(argv = None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    known_args, pipeline_args = parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    logging.info(f'fmp key:{known_args.fmprepkey}')

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

        logging.info('Running premarket loader')
        obb = run_premarket_pipeline(p, known_args.fmprepkey)
        obb | 'oBB2 TO SINK' >>sink
        obb | ' to finvbiz' >> finviz_sink

        yfinance = run_yfinance_pipeline(p)
        yfinance | 'yf To SINK' >>sink

        tester = run_test_pipeline(p)
        tester | 'tester TO SINK' >>sink












