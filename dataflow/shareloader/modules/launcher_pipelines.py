### Launcher Pipelines
import logging
import apache_beam as beam
from datetime import date
from shareloader.modules.obb_utils import AsyncProcess, create_bigquery_ppln, AsyncFMPProcess
from shareloader.modules.superperformers import combine_tickers
from shareloader.modules.finviz_utils import get_extra_watchlist, get_leaps, get_universe_stocks, overnight_return,\
                                            get_eod_screener, get_new_highs, get_peter_lynch, get_finviz_marketdown
from shareloader.modules.obb_processes import AsyncProcessFinvizTester
from shareloader.modules.sectors_utils import get_finviz_performance
import itertools
import requests
from shareloader.modules.dftester_utils import to_json_string, extract_json_list, create_congress_bigquery_ppln
from apache_beam.ml.inference.base import RunInference
from shareloader.modules.beam_inferences import run_gemini_pipeline, run_gemini_congress_pipeline
from shareloader.modules.beam_inferences import GeminiModelHandler
import csv
from io import StringIO
import re

def parse_csv_line(line):
    """
    Parses a single CSV line and yields individual tickers.
    
    This function uses Python's built-in csv module for robust parsing,
    handling potential quotes or delimiters within the data (though less
    likely for simple tickers).
    """
    # Use StringIO to treat the string as a file-like object
    reader = csv.reader(StringIO(line))
    
    # csv.reader returns a list of fields (a single row)
    try:
        # Assuming only one row in the input
        row = next(reader)
        # Tickers are typically alphanumeric; a simple regex filter can
        # help ensure we only yield valid looking tickers.
        for field in row:
            ticker = field.strip()
            # Simple validation: checks if the ticker is non-empty and
            # contains only letters and numbers.
            if ticker and re.match(r'^[A-Z0-9]+$', ticker, re.IGNORECASE):
                yield ticker
    except Exception as e:
        # Log the error for debugging. In a real pipeline, you might use 
        # a side output for error handling.
        print(f"Error processing line: {line}. Error: {e}")



def run_eodmarket_pipeline(p, fmpkey, tolerance=-0.1):
    logging.info('Running OBB ppln')
    cob = date.today()
    return (p | 'Starting eod' >> beam.Create(get_eod_screener())
            | 'EOD Market ' >> beam.Map(lambda d: d['Ticker'])
            | 'Filtering extra eod market' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
            | 'Combine all eod extratickers' >> beam.CombineGlobally(lambda x: ','.join(x))
            | 'EOD' >> beam.ParDo(AsyncProcess({'key': fmpkey}, cob, price_change=tolerance, selection='EOD'))
            )

def run_sector_performance(p):
    # Momentum hqm https://medium.datadriveninvestor.com/quantitative-momentum-strategy-94ff09df25e5
    return (p | 'Starting' >> beam.Create(get_finviz_performance())
     )

def run_swingtrader_pipeline(p, fmpkey, price_change=0.07):
    cob = date.today()
    return  (p  | 'Starting Swingrder'  >> beam.Create(overnight_return())
                | 'SwingTraderList' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering Blanks swt' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers swt' >> beam.CombineGlobally(combine_tickers)
               | 'SwingTraderRun' >> beam.ParDo(AsyncProcess({'key': fmpkey}, cob, price_change=price_change, selection='SwingTrader'))
             )


def run_congresstrades_pipeline(p, google_key):
    congress_trades = create_congress_bigquery_ppln(p)
    congress_trades | 'logging results' >> beam.Map(logging.info)
    run_gemini_congress_pipeline(congress_trades, google_key)

def run_test_pipeline2(p, google_key):

    return run_gemini_pipeline(p, google_key)

def run_plus500_pipeline(p, bucket_path=None):
  file_name = bucket_path or 'gs://mm_dataflow_bucket/inputs/Plus500.csv'
  # 1. Read the single line from the GCS file
  lines = p | 'Read CSV from GCS' >> beam.io.ReadFromText(file_name)

  # 2. Parse the line to extract all tickers
  tickers = (lines | 'Extract Tickers' >> beam.FlatMap(parse_csv_line)
              | 'Filter Valid Tickers' >> beam.Filter(lambda tick: tick and '.' not in tick and '-' not in tick)
              | 'Strip Whitespace' >> beam.Map(lambda tick: tick.strip())
              | 'Combine Tickers' >> beam.CombineGlobally(combine_tickers)
              )
  return tickers


def run_test_pipeline(p, fmpkey, price_change=0.1):
    logging.info('Delegating to plus500')
    test_ppln = run_plus500_pipeline(p)
    cob = date.today()
    return  (test_ppln
                | 'Plus500YFRun' >> beam.ParDo(AsyncFMPProcess({'fmp_api_key': fmpkey}, cob, price_change=price_change, selection='Plus500'))
             )
def run_etoro_pipeline(p, fmpkey, tolerance=0.08):
    cob = date.today()
    return  (p  | 'Starting etoro' >> beam.Create(get_new_highs())
                | 'ETORO LEAPSMaping extra ticker' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering extra' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all extratickers' >> beam.CombineGlobally(lambda x: ','.join(x))
               | 'Etoro' >> beam.ParDo(AsyncProcess({'key':fmpkey}, cob, price_change=tolerance, selection='EToro'))
             )

def run_newhigh_pipeline(p, fmpkey, tolerance=0.01):#even 1% will be godod for ne w hight
    cob = date.today()
    return  (p  | 'Starting nh' >> beam.Create(get_new_highs())
                | 'nh Watchlist' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering nh ' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers from nh' >> beam.CombineGlobally(lambda x: ','.join(x))
               | 'NHighs' >> beam.ParDo(AsyncProcess({'key':fmpkey}, cob, price_change=tolerance, selection='NewHigh'))
             )

def run_finviz_marketdown(p, fmpkey, price_change=-0.05):#even 1% will be godod for ne w hight
    cob = date.today()
    return  (p  | 'Starting finviz md' >> beam.Create(get_finviz_marketdown())
                | 'nh FinvizMarketDown' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering finvix md ' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers from fmd' >> beam.CombineGlobally(lambda x: ','.join(x))
               | 'FinvixMd' >> beam.ParDo(AsyncProcess({'key':fmpkey}, cob, price_change=price_change, selection='FinvizMarketDown'))
             )


def run_extra_pipeline(p, fmpkey, tolerance=0.04):
    cob = date.today()
    return  (p  | 'Starting extras' >> beam.Create(get_extra_watchlist())
                | 'Extra Watchlist' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering extras ' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers from Extraextratickers' >> beam.CombineGlobally(lambda x: ','.join(x))
               | 'Extras' >> beam.ParDo(AsyncFMPProcess({'fmp_api_key':fmpkey}, cob, price_change=tolerance, selection='ExtraWatch'))
             )



def run_peterlynch_pipeline(p, fmpkey, tolerance=0.1):
    cob = date.today()
    return  (p  | 'Starting plynch' >> beam.Create(get_peter_lynch())
                | 'PeterLynch extra ticker' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering plynch' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all plyncch' >> beam.CombineGlobally(lambda x: ','.join(x))
               | 'PLynch' >> beam.ParDo(AsyncProcess({'key':fmpkey}, cob, price_change=tolerance, selection='Peter Lynch'))
             )

def finviz_pipeline(p):
    return (p | 'Test Finviz' >> beam.Create(['AAPL'])
                | 'OBBGet all List' >> beam.ParDo(AsyncProcessFinvizTester())
                | 'Combine Finvviz Reseults' >> beam.CombineGlobally(StockSelectionCombineFn())
                )


class StockSelectionCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    ROW_TEMPLATE = f"""<tr>
                          <td>{input.get('selection', 'noselection')}</td>
                          <td><b>{input.get('highlight', '')}</b></td>
                          <td>{input.get('ticker', '')}({input.get('sector', '')})</td>
                          <td>{input.get('prev_date', '')}</td>
                          <td>{input.get('prev_close', '')}</td>
                          <td>{input.get('date', '')}</td>
                          <td>{input.get('close', '')}</td>
                          <td>{input.get('change', '')}</td>
                          <td>{input.get('ADX', -1)}</td>
                          <td>{input.get('RSI', -1)}</td>
                          <td>{input.get('SMA20', -1)}</td>
                          <td>{input.get('SMA50', -1)}</td>
                          <td>{input.get('SMA200', -1)}</td>
                          
                        </tr>"""

    row_acc = accumulator
    row_acc.append(ROW_TEMPLATE)
    return row_acc

  def merge_accumulators(self, accumulators):
    return list(itertools.chain(*accumulators))

  def extract_output(self, sum_count):
    return ''.join(sum_count)
  

def calculate_hqm_score(ticker, key):
   def get_latest_price(ticker, key):
            stat_url = 'https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={token}'.format(symbol=ticker,
                                                                                                               token=key)
            res = requests.get(stat_url).json()[0]


def run_inference(output, openai_key):
    template = '''
                            I will provide you a json string containing a list of stocks.
                            For each stock i will provide the following information
                            1 - prev_close: the previous close of the stock
                            2 - change: the change from yesterday
                            3 - ADX: the adx
                            4 - RSI : the RSI
                            5 - SMA20: the 20 day simple moving average
                            6 - SMA50: the 50 day simple moving average
                            7 - SMA200: the 200 day simple moving average
                            8 - slope, this will be slope of linear regression for past 30 days.
                            9 - prev_obv: this is on balance volume from previous day
                            10 - current_obv: this is the on balance volume for the current day
                            11 - previous_cmf: this is the value for the previous day of  Chaikin Money Flow (CMF), calculated over previous 20 days
                            12 - current_cmf: this is the value for the current  day of  Chaikin Money Flow (CMF), calculated over previous 20 days
                            13 - obv_historical: these are the on balance volumes for the last 20 days
                            14 - cmf_historical: these are the cmf values for past 20 days
                            As a stock trader and statistician, based on that information, please find which stocks which are candidates to rise in next days.
                            If any of the stocks on the list have dropped more than 10%, then evaluate if it is worth to short sell them based on the
                            same criterias
                            Once you finish your analysis, please summarize your finding indicating, for each
                            stock what is your recommendation and why. 
                            At the end of the message, for the stocks  you recommend as buy or watch or sell, you should generate
                            a json message with fields ticker, action (buy or watch or sell) and an explanation.
                            The json string should be written between a <STARTJSON> and <ENDJSON> tags.
                            Here is my json
            '''
    instructions = '''You are a powerful stock researcher that recommends stock that are candidate to buy or to sell.'''

    return (output | "xxToJson" >> beam.Map(to_json_string)
     | 'xxCombine jsons' >> beam.CombineGlobally(lambda elements: "".join(elements))
     | 'xxanotheer map' >> beam.Map(lambda item: f'{template} \n {item}')

     | "xInference" >> RunInference(model_handler=GeminiModelHandler(openai_key))

     )

def write_to_ai_stocks(pipeline, ai_sink):
    (pipeline | "ExtractJSONLists" >> beam.FlatMap(extract_json_list)
              | "Map to bq dict" >> beam.Map(lambda d: dict(cob=date.today(), ticker=d.get('ticker', ''),
                                                    action=d.get('action', ''), 
                                                    explanation=d.get('explanation', '')))
              | "Write to AI Sink" >> ai_sink 
     
    )

def  run_gcloud_agent(pipeline, agent_url):
        from apache_beam.ml.inference.base import RunInference, PredictionResult
        from shareloader.modules.beam_inferences import CloudRunAgentHandler, PostProcessor

        agent_handler = CloudRunAgentHandler(
            app_url=agent_url,
            app_name="stock_agent",
            user_id="user_123",
            metric_namespace="stock_agent_inference"
        )
        sink = beam.Map(print)
        handler_result = (pipeline | 'Sourcinig prompt' >> beam.Create(
            ["Run a technical analysis for yesterday's stock picks and give me your recommendations"])
         | 'ClouodagentRun' >> RunInference(agent_handler)
         )

        handler_result | sink

        llm_response = (handler_result | "Checking PostProcess" >> beam.ParDo(PostProcessor())
                        )




        



   


