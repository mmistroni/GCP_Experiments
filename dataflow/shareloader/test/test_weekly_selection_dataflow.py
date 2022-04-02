import unittest
import os
import unittest
from unittest.mock import patch
import apache_beam as beam
from shareloader.modules.weekly_selection_dataflow import kickoff_pipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline


class WeeklySelectionTestCase(unittest.TestCase):

    def test_run(self):
        with TestPipeline() as p:
            pcoll1 = p | 'Create coll1' >> beam.Create([{'TICKER': 'FDX', 'COUNT': 100},
                                                        {'TICKER': 'AMZN', 'COUNT': 20},
                                                        {'TICKER': 'MSFT', 'COUNT': 8}
                                                        ])

            pcoll2 = p | 'Create coll2' >> beam.Create(
                [{'TICKER': 'FDX', 'PRICE': 20, 'PRICEAVG20': 200, 'DIVIDEND': 1},
                 {'TICKER': 'AMZN', 'PRICE': 2000, 'PRICEAVG20': 1500, 'DIVIDEND': 0}
                 ])

            kickoff_pipeline(pcoll1, pcoll2) | 'Printing out' >> beam.Map(print)


if __name__ == '__main__':
    unittest.main()
