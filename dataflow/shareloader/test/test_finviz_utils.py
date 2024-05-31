import unittest
from shareloader.modules.finviz_utils import get_universe_stocks, get_canslim, get_leaps,\
                                            get_graham_defensive, get_graham_enterprise
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


    def filter_defensive(self, input_dict):
        if ('debtOverCapital' in input_dict and input_dict['debtOverCapital'] < 0) \
             and ('dividendPaid' in input_dict and input_dict['dividendPaid']  == True) \
                 and ('epsGrowth' in input_dict and input_dict['epsGrowth'] >= 0.33) \
                 and ('positiveEps' in input_dict and  input_dict['positiveEps'] > 0) \
                 and ('priceToBookRatio' in input_dict and input_dict['priceToBookRatio'] > 0) :
            return True
        return False

    def filter_enterprise(self, input_dict):
        if ('debtOverCapital' in input_dict and input_dict['debtOverCapital'] < 0) \
             and ('dividendPaid' in input_dict and input_dict['dividendPaid']  == True) \
                 and ('epsGrowth' in input_dict and input_dict['epsGrowth'] >= 0.33) \
                 and ('positiveEps' in input_dict and  input_dict['positiveEps'] > 0) \
                 and ('priceToBookRatio' in input_dict and input_dict['priceToBookRatio'] > 0) :
            return True
        return False



    def test_gdefensive(self):
        key = os.environ['FMPREPKEY']

        res = get_graham_defensive(key)

        for data in res:
            if self.filter_defensive(data):
                pprint(data)

    def test_genterprise(self):
        key = os.environ['FMPREPKEY']

        res = get_graham_enterprise(key)

        print(res)


if __name__ == '__main__':
    unittest.main()
