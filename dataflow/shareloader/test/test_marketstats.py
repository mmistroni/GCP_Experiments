import os
import unittest
from shareloader.modules.marketstats_utils import get_all_stocks

class TestShareLoader(unittest.TestCase):
    def test_all_stocks(self):
        key = os.environ['FMPREPKEY']
        print(get_all_stocks(key))

if __name__ == '__main__':
    unittest.main()
