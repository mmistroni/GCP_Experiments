import unittest

from shareloader.modules.economic_utils import get_latest_jobs_statistics, get_fruit_and_veg_prices, get_petrol_prices

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline

class EconomicUtilsTestCase(unittest.TestCase):
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

    def test_create_pipeline(self):
        with TestPipeline() as p:
            jobstats = (p | 'Create jobs' >> beam.Create(get_latest_jobs_statistics())
                    )

            fruitandveg = (p | 'Create fandv' >> beam.Create(get_fruit_and_veg_prices())
                      )

            pprices = (p | 'Create pprices' >> beam.Create(get_petrol_prices())
                     )

            final = (
                    (jobstats, fruitandveg, pprices)
                    | 'FlattenCombine all' >> beam.Flatten()
                    | ' rint out' >> beam.Map(print)

            )


if __name__ == '__main__':
    unittest.main()
