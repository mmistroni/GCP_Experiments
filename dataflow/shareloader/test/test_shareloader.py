import os
import unittest
from shareloader.modules.shareloader import get_prices

class TestShareLoader(unittest.TestCase):
    def test_get_prices(self):
        key = os.environ['FMPREPKEY']
        tpl = ('AAPL', 100, 120)

        res = get_prices(tpl, key)
        self.assertIsNotNone(res)

if __name__ == '__main__':
    unittest.main()
