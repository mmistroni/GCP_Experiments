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
from shareloader.modules.finviz_utils import get_extra_watchlist, get_leaps
import argparse
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
import itertools


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



def run_test_pipeline(p):
    cob = date.today()
    test_ppln = create_bigquery_ppln(p)
    return  (test_ppln
                | 'Maping BP ticker' >> beam.Map(lambda d: d['ticker'])
                | 'Filtering' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers' >> beam.CombineGlobally(combine_tickers)
               | 'Plus500YFRun' >> beam.ParDo(AsyncProcess({}, cob, price_change=0.05))
             )
def run_etoro_pipeline(p):
    cob = date.today()
    test_ppln = get_leaps()
    return  (test_ppln
                | 'Maping extra ticker' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering extra' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all extratickers' >> beam.CombineGlobally(combine_tickers)
               | 'Etoro' >> beam.ParDo(AsyncProcess({}, cob, price_change=0.07, selection='EToro'))
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
  parser.add_argument('--sendgridkey')
  return parser.parse_known_args(argv)


class EmailSender(beam.DoFn):
    def __init__(self, key):
        self.recipients = ['mmistroni@gmail.com']
        self.key = key

    def _build_personalization(self, recipients):
        personalizations = []
        for recipient in recipients:
            logging.info('Adding personalization for {}'.format(recipient))
            person1 = Personalization()
            person1.add_to(Email(recipient))
            personalizations.append(person1)
        return personalizations

    def process(self, element):
        msg = element
        logging.info('Attepmting to send emamil to:{self.recipient} with diff {msg}')
        template = \
            "<html><body><table><th>Ticker</th><th>PrevDate</th><th>Prev Close</th><th>Last Date</th><th>Last Close</th><th>Change</th><th>Broker</th>{}</table></body></html>"
        content = template.format(msg)
        logging.info('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_cloud_mm@outlook.com',
            subject='Pre-Market Movers',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        logging.info('Mail Sent:{}'.format(response.status_code))
        logging.info('Body:{}'.format(response.body))


class StockSelectionCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    ROW_TEMPLATE = f"""<tr><td>{input['ticker']}</td>
                          <td>{input['prev_date']}</td>
                          <td>{input['prev_close']}</td>
                          <td>{input['date']}</td>
                          <td>{input['close']}</td>
                          <td>{input['change']}</td>
                          <td>{input['selection']}</td>
                        </tr>"""

    row_acc = accumulator
    row_acc.append(ROW_TEMPLATE)
    return row_acc

  def merge_accumulators(self, accumulators):
    return list(itertools.chain(*accumulators))

  def extract_output(self, sum_count):
    return ''.join(sum_count)


def send_email(pipeline, sendgridkey):
    return (pipeline | 'SendEmail' >> beam.ParDo(EmailSender(sendgridkey))
             )



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

        tester = run_test_pipeline(p)
        etoro = run_etoro_pipeline(p)


        logging.info('----combining ------')


        premarket_results =  ( (tester, etoro) |  "fmaprun" >> beam.Flatten()
                  | 'Combine' >> beam.CombineGlobally(StockSelectionCombineFn()))

        send_email(premarket_results, known_args.sendgridkey)

        premarket_results   | 'tester TO SINK' >> sink










