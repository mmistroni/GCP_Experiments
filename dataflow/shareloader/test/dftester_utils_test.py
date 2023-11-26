import unittest
import os
from shareloader.modules.dftester_utils import  get_financial_ratios, calculate_peter_lynch_ratio, \
                            get_key_metrics, get_fundamental_data


class MyTestCase(unittest.TestCase):

    def test_get_financial_ratios(self):
        key = os.environ['FMPREPKEY']
        data = get_financial_ratios('AAPL', key)

        asOfDates = list(data.keys())

        expectedKeys = ["currentRatio", "quickRatio", "cashRatio", "date", "calendarYear",
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

    def test_get_financial_ratios_first(self):
        key = os.environ['FMPREPKEY']
        data = get_financial_ratios('AAPL', key)

        asOfDates = list(data.keys())

        expectedKeys = ["currentRatio", "quickRatio", "cashRatio", "date", "calendarYear",
                "returnOnAssets", "returnOnEquity", "returnOnCapitalEmployed",
                "priceToBookRatio", "priceToSalesRatio",
                "priceEarningsRatio", "priceToFreeCashFlowsRatio",
                "priceEarningsToGrowthRatio", "dividendYield", "priceFairValue"
                ]
        self.assertEquals(5, len(asOfDates))
        first = asOfDates[0]
        dataDict = data[first]

        self.assertEquals('2023-09-30', dataDict.get('date'))
        self.assertEquals(0.9880116717592975, dataDict.get('currentRatio'))
        self.assertEquals(0.8433121369780053, dataDict.get('quickRatio'))
        self.assertEquals(0.20621713876730807, dataDict.get('cashRatio'))
        self.assertEquals(0.27509834563776475, dataDict.get('returnOnAssets'))
        self.assertEquals( 1.5607601454639075, dataDict.get('returnOnEquity'))
        self.assertEquals(0.551446146423833, dataDict.get('returnOnCapitalEmployed'))
        self.assertEquals(43.37479145093811, dataDict.get('priceToBookRatio'))
        self.assertEquals( 7.032807935374461, dataDict.get('priceToSalesRatio'))
        self.assertEquals(27.790811789370586, dataDict.get('priceEarningsRatio'))
        self.assertEquals(27.06830203155125, dataDict.get('priceToFreeCashFlowsRatio'))
        self.assertEquals(170.91349250463276, dataDict.get('priceEarningsToGrowthRatio'))
        self.assertEquals( 0.005573960673721321, dataDict.get('dividendYield'))
        self.assertEquals(43.37479145093811, dataDict.get('priceFairValue'))

    def test_get_financial_ratios_last(self):
        key = os.environ['FMPREPKEY']
        data = get_financial_ratios('AAPL', key)

        asOfDates = list(data.keys())

        expectedKeys = ["currentRatio", "quickRatio", "cashRatio", "date", "calendarYear",
                "returnOnAssets", "returnOnEquity", "returnOnCapitalEmployed",
                "priceToBookRatio", "priceToSalesRatio",
                "priceEarningsRatio", "priceToFreeCashFlowsRatio",
                "priceEarningsToGrowthRatio", "dividendYield", "priceFairValue"
                ]
        self.assertEquals(5, len(asOfDates))
        first = asOfDates[-1]
        dataDict = data[first]

        self.assertEquals('2019-09-28', dataDict.get('date'))
        self.assertEquals(1.540125617208044, dataDict.get('currentRatio'))
        self.assertEquals(1.3844473032028604, dataDict.get('quickRatio'))
        self.assertEquals(0.46202160464632325, dataDict.get('cashRatio'))
        self.assertEquals(0.16323009842961633, dataDict.get('returnOnAssets'))
        self.assertEquals(0.6106445053487756, dataDict.get('returnOnEquity'))
        self.assertEquals( 0.2746157613037913, dataDict.get('returnOnCapitalEmployed'))
        self.assertEquals(11.167964730793033, dataDict.get('priceToBookRatio'))
        self.assertEquals( 3.8841959325682045, dataDict.get('priceToSalesRatio'))
        self.assertEquals(18.28881555957724, dataDict.get('priceEarningsRatio'))
        self.assertEquals(17.15849620619397, dataDict.get('priceToFreeCashFlowsRatio'))
        self.assertEquals(-54.866446678732885, dataDict.get('priceEarningsToGrowthRatio'))
        self.assertEquals( 0.013971367458288728, dataDict.get('dividendYield'))
        self.assertEquals(11.167964730793033, dataDict.get('priceFairValue'))

    def test_get_key_metrics(self):
        key = os.environ['FMPREPKEY']
        data = get_key_metrics('AAPL', key)

        asOfDates = list(data.keys())

        expectedKeys = [
            "date", "calendarYear", "revenuePerShare", "earningsYield", "debtToEquity",
            "debtToAssets", "capexToRevenue", "grahamNumber"
        ]
        self.assertEquals(5, len(asOfDates))
        for asOfDate in asOfDates:
            dataDict = data[asOfDate]
            dataKeys  = [k in dataDict.keys() for k in expectedKeys ]
            self.assertTrue(all(dataKeys))

    def test_get_key_metrics_last(self):
        key = os.environ['FMPREPKEY']
        data = get_key_metrics('AAPL', key)

        asOfDates = list(data.keys())

        self.assertEquals(5, len(asOfDates))
        lastDate = asOfDates[-1]

        last = data[lastDate]

        self.assertEquals('2019-09-28', last.get('date'))
        self.assertEquals(14.085283273500087, last.get('revenuePerShare'))
        self.assertEquals(0.05467822652278504, last.get('earningsYield'))
        self.assertEquals(1.1940478295464592, last.get('debtToEquity'))
        self.assertEquals(0.3191784140188352, last.get('debtToAssets'))
        self.assertEquals(-0.04033838892433525, last.get('capexToRevenue'))
        self.assertEquals(18.158424594467483, last.get('grahamNumber'))


    def test_get_fundamental_data(self):
        key = os.environ['FMPREPKEY']
        data = get_fundamental_data('AAPL', key)

        expectedKeys = [
            "date", "calendarYear", "revenuePerShare", "earningsYield", "debtToEquity",
            "debtToAssets", "capexToRevenue", "grahamNumber",
             "currentRatio", "quickRatio", "cashRatio", "date", "calendarYear",
            "returnOnAssets", "returnOnEquity", "returnOnCapitalEmployed",
            "priceToBookRatio", "priceToSalesRatio",
            "priceEarningsRatio", "priceToFreeCashFlowsRatio",
            "priceEarningsToGrowthRatio", "dividendYield", "priceFairValue",
            "lynchRatio"

        ]
        self.assertEquals(5, len(data))
        for dataDict in data:
            dataKeys = [k in dataDict.keys() for k in expectedKeys]
            self.assertTrue(all(dataKeys))


    def test_get_fundamental_data_last(self):
        key = os.environ['FMPREPKEY']
        data = get_fundamental_data('AAPL', key)
        last = data[-1]
        self.assertEquals('2019-09-28', last.get('date'))
        self.assertEquals(1.540125617208044, last.get('currentRatio'))
        self.assertEquals(1.3844473032028604, last.get('quickRatio'))
        self.assertEquals(0.46202160464632325, last.get('cashRatio'))
        self.assertEquals(0.16323009842961633, last.get('returnOnAssets'))
        self.assertEquals(0.6106445053487756, last.get('returnOnEquity'))
        self.assertEquals(0.2746157613037913, last.get('returnOnCapitalEmployed'))
        self.assertEquals(11.167964730793033, last.get('priceToBookRatio'))
        self.assertEquals(3.8841959325682045, last.get('priceToSalesRatio'))
        self.assertEquals(18.28881555957724, last.get('priceEarningsRatio'))
        self.assertEquals(17.15849620619397, last.get('priceToFreeCashFlowsRatio'))
        self.assertEquals(-54.866446678732885, last.get('priceEarningsToGrowthRatio'))
        self.assertEquals(0.013971367458288728, last.get('dividendYield'))
        self.assertEquals(11.167964730793033, last.get('priceFairValue'))
        self.assertEquals(14.085283273500087, last.get('revenuePerShare'))
        self.assertEquals(0.05467822652278504, last.get('earningsYield'))
        self.assertEquals(1.1940478295464592, last.get('debtToEquity'))
        self.assertEquals(0.3191784140188352, last.get('debtToAssets'))
        self.assertEquals(-0.04033838892433525, last.get('capexToRevenue'))
        self.assertEquals(18.158424594467483, last.get('grahamNumber'))


if __name__ == '__main__':
    unittest.main()
