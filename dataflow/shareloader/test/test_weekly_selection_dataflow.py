import unittest
import os
import unittest
from unittest.mock import patch
import apache_beam as beam
import itertools
from shareloader.modules.weekly_selection_dataflow import kickoff_pipeline, StockSelectionCombineFn, ROW_TEMPLATE
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
import logging
from shareloader.modules.mail_utils import STOCK_EMAIL_TEMPLATE
from datetime import date



class WeeklySelectionTestCase(unittest.TestCase):

    def test_run(self):
        with TestPipeline() as p:
            pcoll1 = p | 'Create coll1' >> beam.Create([{'TICKER': 'FDX', 'LABEL': 'MYLB', 'COUNT': 100},
                                                        {'TICKER': 'FDX', 'LABEL': 'abcd', 'COUNT': 100},
                                                        {'TICKER': 'AMZN', 'LABEL': 'another', 'COUNT': 20},
                                                        {'TICKER': 'MSFT', 'LABEL': 'MYLB1', 'COUNT': 8}
                                                        ])

            pcoll2 = p | 'Create coll2' >> beam.Create(
                [{'TICKER': 'FDX', 'PRICE': 20, 'PRICEAVG20': 200, 'DIVIDEND': 1,
                   'LABEL' : 'MYLB', 'PRICE' : 1.0,
                    'YEARHIGH'  :1, 'YEARLOW' : 4, 'PRICEAVG50' : 5,
                     'PRICEAVG200' : 6, 'BOOKVALUEPERSHARE':7 , 'CASHFLOWPERSHARE' : 8,
                      'DIVIDENDRATIO' : 0, 'COUNTER' : 1},
                 {'TICKER': 'AMZN', 'PRICE': 2000, 'PRICEAVG20': 1500, 'DIVIDEND': 0
                    ,'LABEL' : 'MYLB1', 'PRICE' : 11.0,
                    'YEARHIGH'  :11, 'YEARLOW' : 41, 'PRICEAVG50' : 51,
                     'PRICEAVG200' : 61, 'BOOKVALUEPERSHARE':71 , 'CASHFLOWPERSHARE' : 81,
                      'DIVIDENDRATIO' : 12, 'COUNTER': 10}
                 ])

            (kickoff_pipeline(pcoll1, pcoll2)
                            | 'combining' >> beam.CombineGlobally(StockSelectionCombineFn())
                            | 'Mapping' >> beam.Map(lambda r: STOCK_EMAIL_TEMPLATE.format(asOfDate=date.today(), tableOfData=r))
                            | 'Printing out' >> beam.Map(print))





if __name__ == '__main__':
    unittest.main()
