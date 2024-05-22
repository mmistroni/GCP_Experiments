import unittest
from shareloader.modules.finviz_utils import get_universe_stocks, get_canslim, get_leaps,\
                                            get_graham_defensive
from pprint import pprint
import os
from shareloader.modules.superperf_metrics import get_dividend_paid

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
        key = os.environ['FMPREPKEY']

        res = get_graham_defensive()

        tickers = [data['Ticker'] for data in res]

        # Now, narrowing down. we need constant divi for last 20 years
        new_dict = dict((t, get_dividend_paid(t, key)) for t in tickers)

        good_dict = dict((k, v) for k, v in new_dict.items() if v['numOfDividendsPaid'] > 20)


        pprint(good_dict)




if __name__ == '__main__':
    unittest.main()
