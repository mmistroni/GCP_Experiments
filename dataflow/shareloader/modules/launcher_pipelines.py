### Launcher Pipelines
import logging
import apache_beam as beam
from datetime import date
from shareloader.modules.finviz_utils import FinvizLoader
from shareloader.modules.obb_utils import AsyncProcess, create_bigquery_ppln, AsyncFMPProcess
from shareloader.modules.superperformers import combine_tickers
from shareloader.modules.finviz_utils import get_extra_watchlist, get_leaps, get_universe_stocks, overnight_return
from datetime import datetime
from shareloader.modules.finviz_utils import get_extra_watchlist, get_leaps, get_universe_stocks, overnight_return,\
                                            get_eod_screener, get_new_highs, get_peter_lynch
from shareloader.modules.obb_processes import AsyncProcessFinvizTester
from shareloader.modules.sectors_utils import get_finviz_performance
import itertools
import requests
from shareloader.modules.dftester_utils import to_json_string, SampleOpenAIHandler, extract_json_list
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference





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

def run_swingtrader_pipeline(p, fmpkey):
    cob = date.today()
    return  (p  | 'Starting Swingrder'  >> beam.Create(overnight_return())
                | 'SwingTraderList' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering Blanks swt' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers swt' >> beam.CombineGlobally(combine_tickers)
               | 'SwingTraderRun' >> beam.ParDo(AsyncProcess({'key': fmpkey}, cob, price_change=0.07, selection='SwingTrader'))
             )

def run_test_pipeline(p, fmpkey):
    cob = date.today()
    test_ppln = create_bigquery_ppln(p)
    return  (test_ppln
                | 'TEST PLUS500Maping BP ticker' >> beam.Map(lambda d: d['ticker'])
                | 'Filtering' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers' >> beam.CombineGlobally(combine_tickers)
               | 'Plus500YFRun' >> beam.ParDo(AsyncFMPProcess({'fmp_api_key': fmpkey}, cob, price_change=0.1, selection='Plus500'))
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


def run_extra_pipeline(p, fmpkey, tolerance=0.04):
    cob = date.today()
    return  (p  | 'Starting extras' >> beam.Create(get_extra_watchlist())
                | 'Extra Watchlist' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering extras ' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers from Extraextratickers' >> beam.CombineGlobally(lambda x: ','.join(x))
               | 'Extras' >> beam.ParDo(AsyncProcess({'key':fmpkey}, cob, price_change=tolerance, selection='ExtraWatch'))
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
                          <td><b>{input.get('highlight', '')}</b></td>
                          <td>{input['ticker']}({input.get('sector', '')})</td>
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
  

def calculate_hqm_score(ticker, key):
   def get_latest_price(ticker, key):
            stat_url = 'https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={token}'.format(symbol=ticker,
                                                                                                               token=key)
            res = requests.get(stat_url).json()[0]


def run_inference(output, openai_key, debug_sink):
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
                            Based on that information, please find which stocks which are candidates to rise in next days.
                            Once you finish your analysis, please summarize your finding indicating, for each
                            stock what is your recommendation and why. 
                            At the end of the message, for the stocks  you recommend as buy or watch, you should generate
                            a json message with fields ticker, action (buy or watch) and an explanation.
                            The json string should be written between a <STARTJSON> and <ENDJSON> tags.
                            Here is my json
            '''
    instructions = '''You are a powerful stock researcher that recommends stock that are candidate to buy.'''

    return (output | "xxToJson" >> beam.Map(to_json_string)
     | 'xxCombine jsons' >> beam.CombineGlobally(lambda elements: "".join(elements))
     | 'xxanotheer map' >> beam.Map(lambda item: f'{template} \n {item}')

     | "xInference" >> RunInference(model_handler=SampleOpenAIHandler(openai_key,
                                                                     instructions))

     )

def write_to_ai_stocks(pipeline, ai_sink):
    (pipeline | "ExtractJSONLists" >> beam.FlatMap(extract_json_list)
              | "Map to bq dict" >> beam.Map(lambda d: dict(cob=date.today(), ticker=d.get('ticker', ''),
                                                    action=d.get('action', ''), 
                                                    explanation=d.get('explanation', '')))
              | "Write to AI Sink" >> ai_sink 
     
    )

        



   


