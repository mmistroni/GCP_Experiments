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
import requests
from shareloader.modules.dftester_utils import to_json_string, SampleOpenAIHandler, extract_json_list
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference

from shareloader.modules.finviz_utils import get_high_low, get_new_highs, get_leaps, get_graham_defensive,\
                                                get_extra_watchlist, get_universe_stocks, get_canslim, \
                                                get_buffett_six, get_graham_enterprise

def _update_dictionary(element, key, value):
    """
    Updates a dictionary with a new key-value pair.

    Args:
        element: The input element, which is expected to be a dictionary.
        key: The key to add or update in the dictionary.
        value: The value associated with the key.

    Returns:
        A new dictionary with the added/updated key-value pair.  Returns
        the original element if it is not a dictionary.
    """
    if isinstance(element, dict):
        new_dict = element.copy()  # Create a copy to avoid modifying the original
        new_dict[key] = value
        return new_dict
    else:
        # Handle the case where the input element is not a dictionary.
        # You might want to log an error, raise an exception, or
        # return a special value depending on your pipeline's needs.
        print(f"Warning: Input element is not a dictionary: {element}")
        return element # Returns the original element, unchanged
def graham_defensive_pipeline(p):
    return ( p | 'Starting Graham' >> beam.Create(get_graham_defensive())
               | 'Updatinig gd' >> beam.Map(lambda d: _update_dictionary(d, 'LABEL', 'GRAHAM_DEFENSIVE'))
             )

def graham_enterprise_pipeline(p):
    return ( p | 'Starting Graham' >> beam.Create(get_graham_enterprise())
               | 'Updatinig gd' >> beam.Map(lambda d: _update_dictionary(d, 'LABEL', 'GRAHAM_ENTERPRISE'))
             )

def extra_watchlist_pipeline(p):
    return ( p | 'Starting ew' >> beam.Create(get_extra_watchlist())
               | 'Updatinig ew' >> beam.Map(lambda d: _update_dictionary(d, 'LABEL', 'EXTRA_WATCHLIST'))
             )
def universe_pipeline(p):
    return (p | 'Starting universe' >> beam.Create(get_universe_stocks())
              | 'Updatinig UP' >> beam.Map(lambda d: _update_dictionary(d, 'LABEL', 'UNIVERSE'))
            )
def canslim_pipeline(p):
    return (p | 'Starting cs' >> beam.Create(get_canslim())
              | 'Updatinig cs' >> beam.Map(lambda d: _update_dictionary(d, 'LABEL', 'CANSLIM'))
            )
def leaps_pipeline(p):
    return (p | 'Starting leaps' >> beam.Create(get_leaps())
              | 'Updatinig lp' >> beam.Map(lambda d: _update_dictionary(d, 'LABEL', 'LEAPS'))
            )

def new_higs_pipeline(p):
    return (p | 'Starting nh' >> beam.Create(get_new_highs())
              | 'Updatinig nh' >> beam.Map(lambda d: _update_dictionary(d, 'LABEL', 'NEW_HIGHS'))
            )

def buffet_six(p):
    return (p | 'Starting bs' >> beam.Create(get_buffett_six())
              | 'Updatinig nh' >> beam.Map(lambda d: _update_dictionary(d, 'LABEL', 'BUFFETT_SIX'))
            )


