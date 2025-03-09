### Launcher Pipelines
import logging
import apache_beam as beam
from datetime import date
from shareloader.modules.finviz_utils import FinvizLoader
from shareloader.modules.obb_utils import AsyncProcess, create_bigquery_ppln, ProcessHistorical
from shareloader.modules.superperformers import combine_tickers
from shareloader.modules.finviz_utils import get_extra_watchlist, get_leaps, get_universe_stocks, overnight_return
from datetime import datetime
from shareloader.modules.finviz_utils import get_extra_watchlist, get_leaps, get_universe_stocks, overnight_return,\
                                            get_eod_screener
from shareloader.modules.obb_processes import AsyncProcessFinvizTester
from shareloader.modules.sectors_utils import get_finviz_performance
import itertools




def run_eodmarket_pipeline(p, fmpkey):
    logging.info('Running OBB ppln')
    cob = date.today()
    return (p | 'Starting eod' >> beam.Create(get_eod_screener())
            | 'EOD Market ' >> beam.Map(lambda d: d['Ticker'])
            | 'Filtering extra eod market' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
            | 'Combine all eod extratickers' >> beam.CombineGlobally(lambda x: ','.join(x))
            | 'EOD' >> beam.ParDo(AsyncProcess({'key': fmpkey}, cob, price_change=0.02, selection='EOD'))
            )

def run_sector_performance(p):
    return (p | 'Starting' >> beam.Create(get_finviz_performance())
     )

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

def finviz_pipeline(p):
    (p | 'Test Finviz' >> beam.Create(['AAPL'])
                | 'OBBGet all List' >> beam.ParDo(AsyncProcessFinvizTester())
                | 'Combine Finvviz Reseults' >> beam.CombineGlobally(StockSelectionCombineFn())
                )


class StockSelectionCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    ROW_TEMPLATE = f"""<tr>
                          <td><b>{input.get('highlight', '')}</b></td>
                          <td>{input['ticker']}({input.get('sector', '')}</td>
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

