from __future__ import absolute_import

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions, DebugOptions
from shareloader.modules.obb_utils import AsyncProcess, create_bigquery_ppln, ProcessHistorical
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

from datetime import date
import argparse
import itertools
from shareloader.modules.launcher_pipelines import run_test_pipeline, run_eodmarket_pipeline, \
                                                   run_sector_performance, run_swingtrader_pipeline, \
                                                   run_etoro_pipeline, finviz_pipeline, \
                                                   StockSelectionCombineFn, write_to_ai_stocks, \
                                                   run_peterlynch_pipeline, run_extra_pipeline, run_newhigh_pipeline,\
                                                   run_test_pipeline2, run_plus500_pipeline, run_gemini_pipeline, \
                                                   run_congresstrades_pipeline, run_finviz_marketdown, \
                                                   run_gcloud_agent
                                                   
from shareloader.modules.launcher_email import EmailSender, send_email

# TaLib article:https://medium.com/@wl8380/meet-ta-lib-your-new-best-friend-in-technical-analysis-60e3e7c62274

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
        "previous_obv" : "FLOAT",
        "current_obv" : "FLOAT",
        "previous_cmf" : "FLOAT",
        "last_cmf" : "FLOAT",
        "obv_last_20_days" : "FLOAT",
        "cmf_last_20_days" : "FLOAT",
        "slope": "INTEGER",
        "trend_velocity_gap": "FLOAT",
        "fib_161": "FLOAT",
        "demarker": "FLOAT",
        "choppiness": "FLOAT",
        "ewo": "FLOAT",
        "spx_choppyness": "FLOAT"

    }

    schemaFields = []
    for fname, ftype in field_dict.items():
        if fname not in ['obv_last_20_days', 'cmf_last_20_days']:
            schemaFields.append({"name": fname, "type": ftype, "mode": "NULLABLE"})
        else:
            schemaFields.append({"name": fname, "type": ftype, "mode": "REPEATED"})
    schema = {
        "fields": schemaFields
    }

    return schema

def get_ai_stocks_schema():
    field_dict = {
        "cob": "DATE",
        "ticker": "STRING",
        "action": "STRING",
        "explanation": "STRING"
    }

    schemaFields = []
    for fname, ftype in field_dict.items():
        schemaFields.append({"name": fname, "type": ftype, "mode": "NULLABLE"})

    schema = {
        "fields": schemaFields
    }

    return schema


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
    custom_dict['previous_obv'] = input_dict.get('previous_obv')
    custom_dict['current_obv'] = input_dict.get('current_obv')
    custom_dict['previous_cmf'] = input_dict.get('previous_cmf')
    custom_dict['last_cmf'] = input_dict.get('last_cmf')
    custom_dict['obv_last_20_days'] = input_dict.get('obv_historical', [0]*20)
    custom_dict['cmf_last_20_days'] = input_dict.get('cmf_historical', [0]*20)
    custom_dict['trend_velocity_gap'] = input_dict.get('trend_velocity_gap', 0)
    custom_dict['fib_161'] = input_dict.get('fib_161', 0)
    custom_dict['demarker'] = input_dict.get('demarker', 0)
    custom_dict['choppiness'] = input_dict.get('choppiness', 0)
    custom_dict['ewo'] = input_dict.get('ewo', 0)
    custom_dict['spx_choppyness'] = input_dict.get('spx_choppyness', 0)
    custom_dict['slope'] = input_dict.get('slope')

    return custom_dict

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
  parser.add_argument('--runtype')
  parser.add_argument('--sendgridkey')
  parser.add_argument('--openaikey')
  parser.add_argument('--googleapikey')
  parser.add_argument('--agent_url')
  return parser.parse_known_args(argv)



class StockSelectionEodCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    {'symbol': 'PSTV', 'name': 'Plus Therapeutics Inc', 'country': 'USA', 'sector': 'Healthcare', 'industry': 'Biotechnology', 'market_cap': 8480000.0, 'price': 1.44, 'change_percent': 3.1143, 'volume': 331918742}
    ROW_TEMPLATE = f"""<tr>
                          <td>{input['symbol']}</td>
                          <td>{input.get('sector', '')}</td>
                          <td>{input.get('industry')}</td>
                          <td>{input['price']}</td>
                          <td>{input['change_percent']}</td>
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
        <td>{dct.get('Perf Week', '')}</td>
        <td>{dct.get('Perf Month', '')}</td>
        <td>{dct.get('Perf Quart', '')}</td>
        <td>{dct.get('Perf Half', '')}</td>
        <td>{dct.get('Perf Year', '')}</td>
        <td>{dct.get('Recom', '')}</td>
        <td>{dct.get('Avg Volume', '')}</td>
        <td>{dct.get('Rel Volume', '')}</td>
    </tr>"""


def run(argv = None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    known_args, pipeline_args = parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    timeout_secs = 80000
    experiment_value = f"max_workflow_runtime_walltime_seconds={timeout_secs}"
    pipeline_options.view_as(DebugOptions).add_experiment(experiment_value)
    #pipeline_options.view_as(DebugOptions).add_experiment(10800)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    #google_cloud_options.max_workflow_runtime_walltime_seconds = 3600
    logging.info(f'fmp key:{known_args.fmprepkey}')
    logging.info(f'RUNTYPEy:{known_args.runtype}')
    logging.info(f'GKEY:{known_args.googleapikey}')
    logging.info(f'AgentUrl:{known_args.agent_url}')
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

    ai_sink = beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId="datascience-projects",
            datasetId='gcp_shareloader',
            tableId='ai-stocks'),
        schema=get_ai_stocks_schema(),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        additional_bq_parameters={
            'ignoreUnknownValues': True  # Ignore unknown columns
        }
    ) 


    with beam.Pipeline(options=pipeline_options) as p:
        sink = beam.Map(logging.info)

        logging.info('Running premarket loader')
        
        finviz_sectors = run_sector_performance(p)

        finviz_results = (finviz_sectors | 'mapping ' >> beam.Map(create_row)
                                | beam.CombineGlobally(FinvizCombineFn())
                                # | 'extracting' >> beam.ParDo(ProcessStringFn())
                                # | 'to sink' >> self.debugSink
                                )

        finviz_results | 'finviz to sink' >> sink
        keyed_finviz= finviz_results | beam.Map(lambda element: (1, element))


        if known_args.runtype == 'eod':
            obb = run_eodmarket_pipeline(p, known_args.fmprepkey)

            premarket_results_eod = (obb | 'Combine Premarkets Reseults EOD' >> beam.CombineGlobally(
                StockSelectionCombineFn()))

            keyed_eod = premarket_results_eod | beam.Map(lambda element: (1, element))
            
            llm_out_eod = run_gemini_pipeline(obb, known_args.googleapikey)
            keyed_llm_eod = llm_out_eod | 'mapping llm2xx eod' >> beam.Map(lambda element: (1, element))
            

            combined = ({'collection1': keyed_eod,
                         'collection2': keyed_finviz,
                         'collection3' :keyed_llm_eod
                         }
                        | beam.CoGroupByKey())

            send_email(combined,  known_args.sendgridkey)

            obb | 'oBB2 TO SINK' >>sink

            (obb | 'obb2 mapped' >> beam.Map(lambda d: map_to_bq_dict(d))
                   | 'obb2 to finvizsink' >> finviz_sink)


            #write_to_ai_stocks(llm_out_eod, ai_sink)


        elif known_args.runtype == 'marketdown':
            plus500 = run_test_pipeline(p, known_args.fmprepkey, price_change=-0.10)
            finviz_md = run_finviz_marketdown(p, known_args.fmprepkey, price_change=-0.10)
            stp = run_swingtrader_pipeline(p, known_args.fmprepkey, price_change=-0.10)

            all_pipelines = ((plus500, finviz_md, stp, ) | "fmaprun all" >> beam.Flatten())
            premarket_results = (all_pipelines | 'Combine Premarkets Reseults' >> beam.CombineGlobally(
                StockSelectionCombineFn()))

            (all_pipelines | 'allp mapped' >> beam.Map(lambda d: map_to_bq_dict(d))
                   | 'allp to finvizsink' >> finviz_sink)

            
            keyed_etoro = premarket_results | beam.Map(lambda element: (1, element))

            
            llm_out = run_gemini_pipeline(all_pipelines, known_args.googleapikey)

            llm_out | sink

            keyed_llm = llm_out | 'mapping llm2' >> beam.Map(lambda element: (1, element))
            
            combined = ({'collection1': keyed_etoro, 'collection2': keyed_finviz,
                         'collection3': keyed_llm
                         }
                        | beam.CoGroupByKey())

            send_email(combined, known_args.sendgridkey, subject='MarketDown movers')

        elif known_args.runtype == 'tester':
            #run_congresstrades_pipeline(p, known_args.googleapikey)
            obb = run_extra_pipeline(p, known_args.fmprepkey)
            (obb | 'obb new test mapped' >> beam.Map(lambda d: map_to_bq_dict(d))
                   | 'test to sink' >> sink)
            if known_args.agent_url:
                run_gcloud_agent(p, known_args.agent_url)

        else:

            plus500 = run_test_pipeline(p, known_args.fmprepkey)
            plus500 | 'plus500 to sink' >> sink

            tester = run_extra_pipeline(p, known_args.fmprepkey)
            tester | 'tester to sink' >> sink

            #(tester  | 'tester mapped'  >> beam.Map(lambda d: map_to_bq_dict(d))
            #        | 'tster to finviz sink' >>  finviz_sink)

            etoro = run_etoro_pipeline(p, known_args.fmprepkey)

            etoro | 'etoro to sink' >> sink

            nhp = run_newhigh_pipeline(p, known_args.fmprepkey)
            nhp | 'newhighgs to sink' >> sink

            
            stp = run_swingtrader_pipeline(p, known_args.fmprepkey)
            stp | 'stp to sink' >> sink

            

            all_pipelines = ((plus500, tester, etoro, stp, nhp) |  "fmaprun all" >> beam.Flatten())

            full_ppln = (all_pipelines | 'allp mapped to map' >> beam.Map(lambda d: map_to_bq_dict(d))
                         )
            full_ppln | 'allp o finvizsink' >> finviz_sink

            full_ppln | 'allp o debug sink' >> sink


            premarket_results =  (all_pipelines |'Combine Premarkets Reseults' >> beam.CombineGlobally(StockSelectionCombineFn()))

            keyed_etoro = premarket_results | beam.Map(lambda element: (1, element))

            llm_out = run_gemini_pipeline(all_pipelines, known_args.googleapikey)

            llm_out | sink

            write_to_ai_stocks(llm_out, ai_sink)
            
            
            keyed_llm = llm_out | 'mapping llm2' >> beam.Map(lambda element: (1, element))
            
            combined = ({'collection1': keyed_etoro, 'collection2': keyed_finviz,
                         'collection3' : keyed_llm
                         }
                        | beam.CoGroupByKey())


            send_email(combined,  known_args.sendgridkey)


            



