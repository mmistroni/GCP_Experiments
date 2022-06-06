import unittest

from shareloader.modules.economic_utils import get_latest_jobs_statistics, get_fruit_and_veg_prices, get_petrol_prices


class EconomicUtilsTestCase(unittest.TestCase):
    def test_latest_job_statistics(self):
        res = get_latest_jobs_statistics()
        print(res.columns)
        print(res)
        self.assertTrue(res.shape[1] > 0)

    def test_fruit_and_veg_prices(self):
        res = get_fruit_and_veg_prices()
        print(res)
        self.assertTrue(res.shape[1] > 0)

    def test_petrol_prices(self):
        res = get_petrol_prices()
        print(res)
        self.assertTrue(res.shape[1] > 0)



if __name__ == '__main__':
    unittest.main()
