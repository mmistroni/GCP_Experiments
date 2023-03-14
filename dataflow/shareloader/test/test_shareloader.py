import os
import unittest
from shareloader.modules.shareloader import get_prices
from unittest.mock import patch
import argparse
class TestShareLoader(unittest.TestCase):

    def setUp(self):
        self.patcher = patch('shareloader.modules.sector_loader.XyzOptions._add_argparse_args')
        self.mock_foo = self.patcher.start()
        parser = argparse.ArgumentParser(add_help=False)


    def tearDown(self):
        self.patcher.stop()


    def test_get_prices(self):
        key = os.environ['FMPREPKEY']
        tpl = ('AAPL', 100, 120)

        res = get_prices(tpl, key)
        self.assertIsNotNone(res)

if __name__ == '__main__':
    unittest.main()
