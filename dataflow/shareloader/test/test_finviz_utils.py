import unittest
from shareloader.modules.finviz_utils import get_universe_stocks, get_canslim

class MyTestCase(unittest.TestCase):
    def test_canslim(self):
        print(get_canslim())

    def test_leaps(self):
        print(get_canslim())



if __name__ == '__main__':
    unittest.main()
