import math
import unittest
import numpy as np
from shareloader.modules.superperf_metrics import get_all_data, get_descriptive_and_technical, \
                get_financial_ratios, get_fmprep_historical

from itertools import chain
from pandas.tseries.offsets import BDay
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
import os
import requests
import pandas as pd
from collections import OrderedDict
from datetime import date, datetime
import logging



class TestPremarketLoader(unittest.TestCase):

    def test_get_fmprep_historical(self):
        key = os.environ['FMPREPKEY']
        res = get_fmprep_historical('AAPL', key, numdays=40, colname=None)

        data = [dict( (k, v)  for k, v in d.items() if k in ['date', 'symbol', 'open', 'adjClose', 'volume']) for d in res]

        df = pd.DataFrame(data=data)
        df['symbol'] = 'AAPL'

        self.assertTrue(df.shape[0] > 0)















