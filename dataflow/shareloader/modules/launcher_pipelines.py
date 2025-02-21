### Launcher Pipelines
import logging
import apache_beam as beam
from datetime import date
from shareloader.modules.finviz_utils import FinvizLoader
from shareloader.modules.obb_utils import AsyncProcess, create_bigquery_ppln, ProcessHistorical
from shareloader.modules.superperformers import combine_tickers
from shareloader.modules.finviz_utils import get_extra_watchlist, get_leaps, get_universe_stocks, overnight_return


from datetime import datetime
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

def run_swingtrader_pipeline(p, fmpkey):
    cob = date.today()
    test_ppln = overnight_return()
    return  (test_ppln
                | 'SwingTraderList' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering Blanks swt' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all tickers swt' >> beam.CombineGlobally(combine_tickers)
               | 'SwingTraderRun' >> beam.ParDo(AsyncProcess({'key': fmpkey}, cob, price_change=0.07))
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
    return  (test_ppln
                | 'ETORO LEAPSMaping extra ticker' >> beam.Map(lambda d: d['Ticker'])
                | 'Filtering extra' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                | 'Combine all extratickers' >> beam.CombineGlobally(lambda x: ','.join(x))
               | 'Etoro' >> beam.ParDo(AsyncProcess({'key':fmpkey}, cob, price_change=tolerance, selection='EToro'))
             )

def map_to_bq_dict(input_dict):
    for k, v in input_dict.items():
        logging.info(f'{k} = {v} = {type(v)}')
    custom_dict = input_dict.copy()
    custom_dict['cob']  = date.today()
    custom_dict['ticker'] = custom_dict['symbol']
    return custom_dict



