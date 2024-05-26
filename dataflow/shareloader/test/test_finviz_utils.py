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

        res = get_graham_defensive(key)

        for data in res:
            if not data.get('dividendPaid'):
                print(f"Skipping {data['Ticker']}..dividend check failed ")
                continue
            if not data.get('priceToBookRatio') or data['priceToBookRatio'] > 1.5:
                print(f"Skipping {data['Ticker']}..price to book ratiofailed ")
                continue
            if not data.get('epsGrowth') or data['epsGrowth'] < 0.33:
                print(f"Skipping {data['Ticker']}..epsGrowth failed  ")
                continue
            pprint(data)






if __name__ == '__main__':
    unittest.main()
