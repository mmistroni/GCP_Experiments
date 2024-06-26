import unittest
import os
from shareloader.modules.fred_utils import get_gdp, get_high_yields_spreads
from shareloader.modules.utils import get_similar_companies, get_peers
import requests
from pprint import pprint

class FedUtilsTestCase(unittest.TestCase):
    def test_gdp(self):
        key = os.environ['FREDKEY']
        data = get_gdp(key)
        self.assertIsNotNone(data)  # add assertion here

    def test_appple_competitors(self):
        key = os.environ['FMPREPKEY']
        aapl_data = requests.get(f'https://financialmodelingprep.com/api/v3/profile/AAPL?apikey={key}').json()[0]

        industry = aapl_data['industry']
        exchange = aapl_data['exchangeShortName']


        res = get_similar_companies(key, industry, exchange)

        old = [d.get('symbol') for d in res]
        res2 = get_peers(key, 'AAPL')


        pprint(old)
        pprint('-----')
        pprint(res2)

    def test_appple_peers(self):
        key = os.environ['FMPREPKEY']
        res = get_peers(key, 'AAPL')
        pprint(res)

    def test_fred_URLS(self):
        key = os.environ['FREDKEY']
        from pprint import pprint
        # consumer sentiment = UMCSENT
        # PMI. USSLIND
        # Checkout this page to see series  https://fred.stlouisfed.org/tags/series?t=nonmanufactured
        # https://fred.stlouisfed.org/categories/1
        series = 'BAMLH0A0HYM2'# 'UMCSENT' #'USSLIND'  hhigy yiueld spreadds
        pprint(requests.get(
            f'https://api.stlouisfed.org/fred/series/observations?series_id={series}&file_type=json&api_key={key}&file_type=json').json())

        # treasury constant amt
        pprint(requests.get(
            f'https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&file_type=json&api_key={key}&file_type=json').json())

    def test_high_yield_spreads(self):
        key = os.environ['FREDKEY']

        data = requests.get(
            f'https://api.stlouisfed.org/fred/series/observations?series_id=BAMLH0A0HYM2&file_type=json&api_key={key}&file_type=json').json()

        pprint(data['observations'][-3:])

        #print(get_high_yields_spreads(key))


if __name__ == '__main__':
    unittest.main()
