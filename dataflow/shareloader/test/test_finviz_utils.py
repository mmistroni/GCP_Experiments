import unittest
from shareloader.modules.finviz_utils import get_universe_stocks, get_canslim, get_leaps,\
                                            get_graham_defensive
from pprint import pprint


class MyTestCase(unittest.TestCase):
    def test_canslim(self):
        res = get_canslim()
        item = res[0]
        pprint(item.keys())

    def test_leaps(self):
        res = get_leaps()
        item = res[0]

        pprint(item.keys())

    def test_universe(self):
        rres = get_universe_stocks()
        print(rres)

    def test_gdefensive(self):
        res = get_graham_defensive()
        print(res)




if __name__ == '__main__':
    unittest.main()
