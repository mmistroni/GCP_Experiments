import unittest
import os
from shareloader.modules.dftester_utils import  get_financial_ratios, calculate_peter_lynch_ratio, \
                            get_key_metrics, get_fundamental_data


class MyTestCase(unittest.TestCase):

    def test_get_financial_ratios(self):
        key = os.environ['FMPREPKEY']
        data = get_financial_ratios('AAPL', key)

        asOfDates = list(data.keys())

        expectedKeys = keys = ["currentRatio", "quickRatio", "cashRatio", "date", "calendarYear",
                "returnOnAssets", "returnOnEquity", "returnOnCapitalEmployed"
                                                    "priceToBookRatio", "priceToSalesRatio",
                "priceEarningsRatio", "priceToFreeCashFlowsRatio",
                "priceEarningsToGrowthRatio", "dividendYield", "priceFairValue"
                ]
        self.assertEquals(5, len(asOfDates))
        for asOfDate in asOfDates:
            dataDict = data[asOfDate]
            dataKeys  = [k in dataDict.keys() for k in expectedKeys ]
            self.assertTrue(all(dataKeys))




if __name__ == '__main__':
    unittest.main()
