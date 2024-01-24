import unittest

from shareloader.modules.economic_utils import get_latest_jobs_statistics, get_fruit_and_veg_prices, get_petrol_prices,\
                                get_card_spending, get_gas_prices

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions
import argparse


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class EconomicUtilsTestCase(unittest.TestCase):

    def setUp(self):
        parser = argparse.ArgumentParser(add_help=False)


    def test_latest_job_statistics(self):
        res = get_latest_jobs_statistics()
        print(res)
        self.assertTrue(len(res) > 0)

    def test_fruit_and_veg_prices(self):
        res = get_fruit_and_veg_prices()
        print(res)
        self.assertTrue(len(res) > 0)

    def test_petrol_prices(self):
        res = get_petrol_prices()
        print(res)
        self.assertTrue(len(res) > 0)

    def test_get_card_spending(self):
        #https://stackoverflow.com/questions/68850502/pandas-read-csv-with-storage-options-working-locally-but-not-in-dataflow
        res = get_card_spending()
        print(res)
        self.assertTrue(len(res) > 0) # need to upgrade pandas

    def test_get_gas_prices(self):
        res =  get_gas_prices()
        print(res)
        self.assertTrue(len(res) > 0)




    def test_create_pipeline(self):
        from shareloader.modules.state_of_uk_economy import kickoff_pipeline

        non_empty_sink = Check(is_not_empty())

        with TestPipeline(options=PipelineOptions()) as p:
            jobstats =  kickoff_pipeline(p)
            jobstats | non_empty_sink

if __name__ == '__main__':
    unittest.main()
