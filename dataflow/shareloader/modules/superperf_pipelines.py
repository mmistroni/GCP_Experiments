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

def combine_pipelines(p):
    extrawl = run_extrawl(p)
    buffetsix = run_buffetsix(p)
    newhighs = run_newhighs(p)
    canslim = run_canslim(p)
    leaps = run_leaps(p)
    ge = run_graham_enterprise(p)
    gd = run_graham_defensive(p)
    universe = run_universe(p)



    return (
            (extrawl, buffetsix, newhighs, canslim,
             leaps, ge, gd, universe)
            | 'FlattenCombine all' >> beam.Flatten()
    )








   


