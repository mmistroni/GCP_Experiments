## Utility to get dataf from fred
import requests


def get_gdp(apiKey):
    #requests.get(f'https://api.stlouisfed.org/fred/category/series?category_id=32291&api_key={apiKey}&file_type=json').json()
    return requests.get(f'https://api.stlouisfed.org/fred/series/categories?series_id=USARGDPE&api_key={apiKey}&file_type=json').json()