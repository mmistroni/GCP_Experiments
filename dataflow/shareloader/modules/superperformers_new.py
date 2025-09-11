from __future__ import absolute_import
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import SetupOptions, DebugOptions
import logging
import apache_beam as beam
import argparse
from .superperf_pipelines import combine_fund1, combine_fund2, combine_benchmarks, PipelineCombinerFn,\
                            EnhancedFundamentalLoader, EnhancedBenchmarkLoader, StockSelectionCombineFn, send_email,\
                            store_superperformers
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import itertools

'''
Further source of infos
https://medium.com/@mancuso34/building-all-in-one-stock-economic-data-repository-6246dde5ce02
https://wire.insiderfinance.io/implement-buffets-approach-with-python-and-streamlit-5d3a7bc42b89
'''

ROW_TEMPLATE =  """<tr><td>{}</td>
                       <td>{}</td>
                   </tr>"""

def parse_known_args(argv):
    """Parses args for the workflow."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--fmprepkey')
    parser.add_argument('--runtype')
    parser.add_argument('--sendgridkey')
    return parser.parse_known_args(argv)

def run_fund1(p, key):
    res = combine_fund1(p)
    return (res | 'fSuperperf fcombining tickets' >> beam.Map(
        lambda d: dict(ticker=d.get('Ticker'), label=d.get('label')))
     | 'fCombineAllIntoSingleList' >> beam.CombineGlobally(PipelineCombinerFn())
     | 'fGetting fundamentals' >> beam.ParDo(EnhancedFundamentalLoader(key))
            )

def run_fund2(p, key):
    res = combine_fund2(p)
    return (res | 'fSuperperf fcombining2 tickets' >> beam.Map(lambda d: dict(ticker=d.get('Ticker'), label=d.get('label')))
         |'fCombineAllIntoSingleList2' >> beam.CombineGlobally(PipelineCombinerFn())
         | 'fGetting fundamentals2' >> beam.ParDo(EnhancedFundamentalLoader(key))
         )

def run_benchmarks(p, key):
    res = combine_benchmarks(p)
    return (res | 'Superperf bench combining tickets' >> beam.Map(lambda d: dict(ticker=d.get('Ticker'), label=d.get('label')))
         | 'Bench CombineAllIntoSingleList' >> beam.CombineGlobally(PipelineCombinerFn())
         | 'Getting fundamentals bench' >> beam.ParDo(EnhancedBenchmarkLoader(key))
            )

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    timeout_secs = 18400
    known_args, pipeline_args = parse_known_args(argv)

    experiment_value = f"max_workflow_runtime_walltime_seconds={timeout_secs}"

    pipeline_optionss = PipelineOptions(pipeline_args)
    pipeline_optionss.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_optionss.view_as(DebugOptions).add_experiment(experiment_value)

    debugSink = beam.Map(logging.info)

    bq_sink = beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId="datascience-projects",
            datasetId='gcp_shareloader',
            tableId='stock_selection'),
        schema='AS_OF_DATE:DATE,TICKER:STRING,LABEL:STRING,PRICE:FLOAT,YEARHIGH:FLOAT,YEARLOW:FLOAT,PRICEAVG50:FLOAT,PRICEAVG200:FLOAT,BOOKVALUEPERSHARE:FLOAT,TANGIBLEBOOKVALUEPERSHARE:FLOAT,CASHFLOWPERSHARE:FLOAT,MARKETCAP:FLOAT,ASSET_VALUE:FLOAT,EXCESS_MARKETCAP:FLOAT,DIVIDENDRATIO:FLOAT,PERATIO:FLOAT,INCOME_STMNT_DATE:STRING,INCOME_STMNT_DATE_QTR:STRING,RSI:FLOAT,PIOTROSKY_SCORE:FLOAT,NET_INCOME:FLOAT,RETURN_ON_CAPITAL:FLOAT,NUM_OF_DIVIDENDS:FLOAT,LYNCH_RATIO:FLOAT',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    with beam.Pipeline(options=pipeline_optionss) as p:
        run_type = known_args.runtype
        key = known_args.fmprepkey
        logging.info(f'RunType:{run_type}')
        if run_type == 'fund1':
            res = run_fund1(p, key)
        elif run_type == 'fund2':
            res = run_fund2(p, key)

        elif run_type == 'benchmark':
            res = run_benchmarks(p, key)


        # sending to sink
        res | 'bench to sink' >> debugSink

        combined = (res | 'combining' >> beam.CombineGlobally(StockSelectionCombineFn()))

        combined | 'combined to sink'  >> debugSink

        send_email(combined, known_args.sendgridkey, runType=run_type)

        store_superperformers(res, bq_sink)



