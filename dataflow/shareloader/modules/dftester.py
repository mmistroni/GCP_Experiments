from __future__ import absolute_import

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date
from .marketstats_utils import get_all_us_stocks2, NewHighNewLowLoader
from .dftester_utils import combine_tickers, DfTesterLoader, get_fields


''' 
Big Query Table needs to have following fields
'cob'
'symbol', 
'price', 
'change', 
'yearHigh', 
'yearLow', 
'marketCap', 
'priceAvg50', 
'priceAvg200', 
'exchange', 
'avgVolume', 
'open', 
'eps', 
'pe', 
'sharesOutstanding', 
'institutionalOwnershipPercentage', 
'epsGrowth', 
'epsGrowth5yrs', 
'OPERATING_INCOME_CAGR', 
'positiveEps', 
'positiveEpsLast5Yrs', 
'netIncome', 
'income_statement_date', 
'debtOverCapital', 
'enterpriseDebt', 
'totalAssets', 
'inventory', 
'totalCurrentAssets', 
'totalCurrentLiabilities', 
'dividendPaid', 
'dividendPaidEnterprise', 
'dividendPayoutRatio', 
'numOfDividendsPaid', 
'returnOnCapital', 
'peRatio', 
'netProfitMargin', 
'currentRatio', 
'priceToBookRatio', 
'grossProfitMargin', 
'returnOnEquity', 
'dividendYield', 
'pegRatio', 
'payoutRatio', 
'tangibleBookValuePerShare', 
'netCurrentAssetValue', 
'freeCashFlowPerShare', 
'earningsYield', 
'bookValuePerShare', 
'canBuyAllItsStock', 
'netQuickAssetPerShare', 
'rsi', 
'piotroskyScore', 
'ticker', 
'52weekChange', 
'label'

'''



class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')
        parser.add_argument('--input')
        parser.add_argument('--output')
        parser.add_argument('--period')
        parser.add_argument('--limit')
        parser.add_argument('--pat')



def run_stocksel_pipeline(p, fmpKey, inputFile='gs://mm_dataflow_bucket/inputs/history_5y_tickers_US_with_sector_and_industry_20231126.csv',
                                    period='quarter'):
    return (p
            | 'Reading Tickers' >> beam.io.textio.ReadFromText(inputFile)
            | 'Converting to Tuple' >> beam.Map(lambda row: row.split(','))
            | 'Extracting only ticker and Industry' >> beam.Map(lambda item: (item[0]))
            | 'Combining Tickers' >> beam.CombineGlobally(combine_tickers)
            | 'Run Loader' >> beam.ParDo(DfTesterLoader(fmpKey, period=period, limit=10))
            )

def run_my_pipeline(p, fmpkey):
    nyse = get_all_us_stocks2(fmpkey, "New York Stock Exchange")
    nasdaq = get_all_us_stocks2(fmpkey, "Nasdaq Global Select")
    full_ticks = '.'.join(nyse + nasdaq)

    return ( p
            | 'Start' >> beam.Create([full_ticks])
            | 'Get all List' >> beam.ParDo(NewHighNewLowLoader(fmpkey))

    )

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    logging.info('Starting tester pipeline')

    with beam.Pipeline(options=pipeline_options) as p:
        sink = beam.Map(logging.info)

        if bool(pipeline_options.pat):
            logging.info('running OBB....')
        #'obb = run_obb_pipeline(p, pipeline_options.pat)
            obb = run_my_pipeline(p, pipeline_options.fmprepkey)
            obb | sink
        else:
            destination =  f"gs://mm_dataflow_bucket/datasets/{pipeline_options.output}_{date.today().strftime('%Y-%m-%d %H:%M')}"
            bucketSink = beam.io.WriteToText(destination, num_shards=1, header=','.join(get_fields()))

            # TDO parameterize period and limits

            data = run_stocksel_pipeline(p, pipeline_options.fmprepkey, period=pipeline_options.period)
            data | 'Wrrting to bucket' >> bucketSink




