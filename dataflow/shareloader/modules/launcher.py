from __future__ import absolute_import

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from shareloader.modules.finviz_utils import FinvizLoader
from shareloader.modules.obb_utils import AsyncProcess, create_bigquery_ppln, ProcessHistorical
from apache_beam.io.gcp.internal.clients import bigquery
from shareloader.modules.sectors_utils import get_finviz_performance

from datetime import date
from shareloader.modules.superperformers import combine_tickers
from shareloader.modules.finviz_utils import get_extra_watchlist, get_leaps, get_universe_stocks, overnight_return
import argparse
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
import itertools


class AnotherLeftJoinerFn(beam.DoFn):

    def __init__(self):
        super(AnotherLeftJoinerFn, self).__init__()

    def process(self, row, **kwargs):

        right_dict = dict(kwargs['right_list'])

        left_key = row[0]
        left = row[1]
        if left_key in right_dict:
            right = right_dict[left_key]
            left.update(right)
            yield (left_key, left)


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
        "selection": "STRING",
        "asodate" : "DATE",
        "ADX" : "FLOAT",
        "RSI": "FLOAT",
        "SMA20": "FLOAT",
        "SMA50": "FLOAT",
        "SMA200": "FLOAT",
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
             | 'PREMARKET PMStart' >> beam.Create(['AAPL'])
             | 'PMGet all List' >> beam.ParDo(FinvizLoader(fmpkey, runtype='premarket'))
             | 'PMMap to BQable' >> beam.Map(lambda d: map_to_bq_dict(d))

    )

def run_sector_performance(p):
    return (p | 'Starting' >> beam.Create(get_finviz_performance())
     )


def create_row(dct):
            return f"""<tr>
                <td>{dct.get('Name', '')}</td>
                <td>{dct.get('Perf Month', '')}</td>
                <td>{dct.get('Perf Quart', '')}</td>
                <td>{dct.get('Perf Half', '')}</td>
                <td>{dct.get('Perf Year', '')}</td>
                <td>{dct.get('Recom', '')}</td>
                <td>{dct.get('Avg Volume', '')}</td>
                <td>{dct.get('Rel Volume', '')}</td>
            </tr>"""

def combine_rows(rows):
    return ','.join(rows)
        
        


def map_to_bq_dict(input_dict):
    custom_dict = {}
    custom_dict['symbol'] = input_dict.get('symbol')
    custom_dict['price'] = input_dict.get('close')
    custom_dict['open'] = input_dict.get('open')
    custom_dict['previousClose'] = input_dict.get('prev_close')
    custom_dict['change'] = input_dict.get('change')
    custom_dict['ticker'] = input_dict.get('ticker')
    custom_dict['asodate'] = date.today()
    custom_dict['cob'] = date.today()
    custom_dict['selection'] = input_dict.get('selection')
    custom_dict["ADX"] =  input_dict.get('ADX')
    custom_dict["RSI"] =  input_dict.get('RSI')
    custom_dict["SMA20"] =  input_dict.get('SMA20')
    custom_dict["SMA50"] =  input_dict.get('SMA50')
    custom_dict["SMA200"] =  input_dict.get('SMA200')
    return custom_dict


def run_swingtrader_pipeline(p, fmpkey):
    cob = date.today()
    return  (p  | 'Starting Swingrder'  >> beam.Create(overnight_return())
                | 'SwingTraderList' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering Blanks swt' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers swt' >> beam.CombineGlobally(combine_tickers)
               | 'SwingTraderRun' >> beam.ParDo(AsyncProcess({'key': fmpkey}, cob, price_change=0.1))
             )


def run_test_pipeline(p, fmpkey):
    cob = date.today()
    test_ppln = create_bigquery_ppln(p)
    return  (test_ppln
                | 'TEST PLUS500Maping BP ticker' >> beam.Map(lambda d: d['ticker'])
                | 'Filtering' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers' >> beam.CombineGlobally(combine_tickers)
               | 'Plus500YFRun' >> beam.ParDo(AsyncProcess({'key': fmpkey}, cob, price_change=0.05))
             )
def run_etoro_pipeline(p, fmpkey, tolerance=0.1):
    cob = date.today()
    test_ppln = get_universe_stocks()
    return  (p  | 'Starting etoro' >> beam.Create(get_universe_stocks())
                | 'ETORO LEAPSMaping extra ticker' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering extra' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all extratickers' >> beam.CombineGlobally(lambda x: ','.join(x))
               | 'Etoro' >> beam.ParDo(AsyncProcess({'key':fmpkey}, cob, price_change=tolerance, selection='EToro'))
             )


def combine_tester_and_etoro(fmpKey, tester,etoro):
    '''
    Redo.
    :param fmpKey:
    :param tester:
    :param etoro:
    :return:
    '''
    mapped =  ((tester, etoro) | "etorox combined fmaprun" >> beam.Flatten()
                         | 'Remap to tuple x' >> beam.Map(lambda dct: (dct['ticker'], dct))
                         | 'filtering' >> beam.Filter(lambda tpl: tpl[0] is not None)
                         )



    historicals =  (mapped | 'Mapping t and e x' >> beam.Map(lambda tpl: tpl[0])
                         | 'Combine both x' >> beam.CombineGlobally(lambda x: ','.join(x))
                         | 'Find ADXand RSI x' >> beam.ParDo(ProcessHistorical(fmpKey, date.today()))

    )

    return (
            mapped
            | 'InnerJoiner: JoinValues between two pips' >> beam.ParDo(AnotherLeftJoinerFn(),
                                                      right_list=beam.pvalue.AsIter(historicals))
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

        key, value_dict = element
        stocks = list(value_dict['collection1'])
        sectors = list(value_dict['collection2'])

        logging.info('Attepmting to send emamil to:{self.recipient} with diff {msg}')
        template = \
            '''<html>
                  <body>
                    <table>
                        <th>Name</th><th>Perf Month</th><th>Perf Quart</th><th>Perf Half</th><th>Perf Year</th><th>Recom</th><th>Avg Volume</th><th>Rel Volume</th>
                        {}
                    </table>
                    <br>
                    <table>
                       <th>WATCH</th><th>Ticker</th><th>PrevDate</th><th>Prev Close</th><th>Last Date</th><th>Last Close</th><th>Change</th><th>Adx</th><th>RSI</th><th>SMA20</th><th>SMA50</th><th>SMA200</th><th>Broker</th>
                       {}
                    </table>
                  </body>
                </html>'''
        content = template.format(sectors, stocks)
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
    ROW_TEMPLATE = f"""<tr>
                          <td><b>{input.get('highlight', '')}</b></td>
                          <td>{input['ticker']}</td>
                          <td>{input['prev_date']}</td>
                          <td>{input['prev_close']}</td>
                          <td>{input['date']}</td>
                          <td>{input['close']}</td>
                          <td>{input['change']}</td>
                          <td>{input.get('ADX', -1)}</td>
                          <td>{input.get('RSI', -1)}</td>
                          <td>{input.get('SMA20', -1)}</td>
                          <td>{input.get('SMA50', -1)}</td>
                          <td>{input.get('SMA200', -1)}</td>
                          <td>{input['selection']}</td>
                          
                        </tr>"""

    row_acc = accumulator
    row_acc.append(ROW_TEMPLATE)
    return row_acc

  def merge_accumulators(self, accumulators):
    return list(itertools.chain(*accumulators))

  def extract_output(self, sum_count):
    return ''.join(sum_count)


class FinvizCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, accumulator, input):
        accumulator.append(input)
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = []
        for acc in accumulators:
            merged.extend(acc)
        return merged

    def extract_output(self, accumulator):
        # Process the accumulated rows and return the result
        return ''.join(accumulator)

def create_row(dct):
    return f"""<tr>
        <td>{dct.get('Name', '')}</td>
        <td>{dct.get('Perf Month', '')}</td>
        <td>{dct.get('Perf Quart', '')}</td>
        <td>{dct.get('Perf Half', '')}</td>
        <td>{dct.get('Perf Year', '')}</td>
        <td>{dct.get('Recom', '')}</td>
        <td>{dct.get('Avg Volume', '')}</td>
        <td>{dct.get('Rel Volume', '')}</td>
    </tr>"""



def send_email(pipeline,  sendgridkey):
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
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        additional_bq_parameters={
            'ignoreUnknownValues': True  # Ignore unknown columns
        }
    )

    finviz_sink = beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId="datascience-projects",
            datasetId='gcp_shareloader',
            tableId='finviz-premarket'),
        schema=get_finviz_schema(),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        additional_bq_parameters={
            'ignoreUnknownValues': True  # Ignore unknown columns
        }
    )

    with beam.Pipeline(options=pipeline_options) as p:
        sink = beam.Map(logging.info)

        logging.info('Running premarket loader')
        #obb = run_premarket_pipeline(p, known_args.fmprepkey)
        #obb | 'oBB2 TO SINK' >>sink
        #obb | ' to finvbiz' >> finviz_sink

        tester = run_test_pipeline(p, known_args.fmprepkey)
        tester | 'tester to sink' >> sink

        (tester  | 'tester mapped'  >> beam.Map(lambda d: map_to_bq_dict(d))
                | 'tster to finviz sink' >>  finviz_sink)

        etoro = run_etoro_pipeline(p, known_args.fmprepkey)

        etoro | 'etoro to sink' >> sink


        (etoro | 'etorotester mapped' >> beam.Map(lambda d: map_to_bq_dict(d))
               | 'etoro to finvizsink' >> finviz_sink)


        finviz_sectors = run_sector_performance(p)

        finviz_results = (finviz_sectors | 'mapping ' >> beam.Map(create_row)
                             | beam.CombineGlobally(FinvizCombineFn())
                             # | 'extracting' >> beam.ParDo(ProcessStringFn())
                             # | 'to sink' >> self.debugSink
                             )

        finviz_results | 'finviz to sink' >> sink
        keyed_finviz= finviz_results | beam.Map(lambda element: (1, element))



        premarket_results =  ( (tester, etoro) |  "fmaprun all" >> beam.Flatten()
                  | 'Combine Premarkets Reseults' >> beam.CombineGlobally(StockSelectionCombineFn()))

        keyed_etoro = premarket_results | beam.Map(lambda element: (1, element))

        combined = ({'collection1': keyed_etoro, 'collection2': keyed_finviz}
                    | beam.CoGroupByKey())


        send_email(combined,  known_args.sendgridkey)

        premarket_results   | 'tester TO SINK' >> sink












