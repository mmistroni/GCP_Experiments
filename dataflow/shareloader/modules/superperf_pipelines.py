### SuperPerformers Pipelines
import logging
import apache_beam as beam
from datetime import date
from shareloader.modules.finviz_utils import FinvizLoader
from shareloader.modules.obb_utils import AsyncProcess, create_bigquery_ppln, ProcessHistorical
from shareloader.modules.superperformers import combine_tickers
from shareloader.modules.finviz_utils import get_extra_watchlist, get_leaps, get_universe_stocks, get_canslim, get_buffett_six, \
                                                get_graham_enterprise, get_graham_defensive, get_new_highs
from datetime import datetime
from shareloader.modules.obb_processes import AsyncProcessFinvizTester
from shareloader.modules.sectors_utils import get_finviz_performance
import itertools
import requests


def update_dict_beam(element, label):
  """Updates a dictionary within a Beam pipeline element.

  Args:
    element: A dictionary.
    key: The key to update or add.
    value: The value to associate with the key.

  Returns:
    A new dictionary with the updated key-value pair.
  """
  return {**element, 'label': label}


def run_universe(p):
    return (p | 'Starting universe' >> beam.Create(get_universe_stocks())
            | 'adding Universe Label' >> beam.Map(lambda d: update_dict_beam(d,  'UNIVERSE'))
            )

def run_graham_defensive(p):
    return (p | 'Starting gd' >> beam.Create(get_graham_defensive())
            | 'adding gd Label' >> beam.Map(lambda d: update_dict_beam(d,  'DEFENSIVE'))
            )

def run_graham_enterprise(p):
    return (p | 'Starting ge' >> beam.Create(get_graham_enterprise())
            | 'adding ge' >> beam.Map(lambda d: update_dict_beam(d,  'ENTEPRISE'))
            )
def run_leaps(p):
    return (p | 'Starting leaps' >> beam.Create(get_leaps())
            | 'adding leaps' >> beam.Map(lambda d: update_dict_beam(d,  'LEAPS'))
            )

def run_canslim(p):
    return (p | 'Starting cs' >> beam.Create(get_canslim())
            | 'adding cs' >> beam.Map(lambda d: update_dict_beam(d,  'CANSLIM'))
            )

def run_newhighs(p):
    return (p | 'Starting ns' >> beam.Create(get_new_highs())
            | 'adding nh' >> beam.Map(lambda d: update_dict_beam(d,  'NEWHIGHS'))
            )

def run_buffetsix(p):
    return (p | 'Starting bs' >> beam.Create(get_buffett_six())
            | 'adding bs' >> beam.Map(lambda d: update_dict_beam(d,  'BUFFET_SIX'))
            )

def run_extrawl(p):
    return (p | 'Starting ewl' >> beam.Create(get_buffett_six())
            | 'adding ewl' >> beam.Map(lambda d: update_dict_beam(d,  'WATCHLIST'))
            )

'''

We  need to combine and see differences betweeen the loaders so that we all retrieve the same data

- Fundamental Loader calls
    - get_fundamental_parameters
    - get_financial_ratios
    - get_analyst_estimates
    - get_descriptive_and_technical
    - get_asset_play_parameter
    - calculate_piotrosky_score
    - compute_rsi
    - get_key_metrics_benchmark
    - get_peter_lynch_ratio

- MicroCap Loader
    - get_descriptive_and_technical
    - get_price_change (priceChangeDict.get('52weekChange', 0)
    - get_fundamental_parameter
    - get_financial_ratios
    - get_dividend_paid
    

- Benchmark Loader
    - get_quote_benchmark (to retrieve institutional ownership. can be replaced by finviz)
    - get_income_benchmark - should be same as get_fundamental_params
    - get_balancesheet_benchmark
    - get_financial_ratios_benchmark fund
    - get_key_metrics_benchmark   fund
    - get_asset_play_parameters  fund
    - get_peter_lynch_ratio      fund
        


def load_fundamental_data(source,fmpkey, split=''):
    # fundamental works for everything minus microcap, 
    return (source
            | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(FundamentalLoader(fmpkey, split_flag=split))
            | 'Filtering out none fundamentals' >> beam.Filter(lambda item: item is not None)
            | 'filtering on descr and technical' >> beam.Filter(get_descriptive_and_techincal_filter)
            | 'Using fundamental filters' >> beam.Filter(get_fundamental_filter)
            )
def load_microcap_data(source,fmpkey):
    return (source
            | 'Combine all at fundamentals microcap' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals microcap' >> beam.ParDo(MicrocapLoader(fmpkey, microcap_flag=True))
            | 'Filtering out none fundamentals microcap' >> beam.Filter(lambda item: item is not None)
            | 'MicroCap Sanity Check' >> beam.Filter(microcap_sanity_check)
            | 'Filtering microcap' >> beam.Filter(microcap_filter)
            )

def load_benchmark_data(source,fmpkey, split=None):
    return (source
            | 'Combine all at fundamentals bench' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals bench' >> beam.ParDo(BenchmarkLoader(fmpkey, split))
            | 'Filtering  benchmark by price' >>  beam.Filter(lambda d: d.get('price', 0) > 10)
            )
'''


def combine_bernchmarks(p):
    extrawl = run_extrawl(p)
    buffetsix = run_buffetsix(p)
    newhighs = run_newhighs(p)
    canslim = run_canslim(p)
    leaps = run_leaps(p)


def combine_fundamental(p):
    ge = run_graham_enterprise(p)
    gd = run_graham_defensive(p)
    universe = run_universe(p)



    return (
            (ge, gd)
            | 'FlattenCombine all' >> beam.Flatten()
            #| 'Superperf combining tickets' >> beam.Map(lambda d: dict(ticker=d.get('Ticker'), label=d.get('label')))
            #| 'Combine all at fundamentals bench' >> beam.CombineGlobally(combine_tickers)
    )








   


