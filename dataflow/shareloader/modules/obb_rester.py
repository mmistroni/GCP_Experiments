import requests
import json
import logging
from pandas.tseries.offsets import BDay
from datetime import date


DATA_DICT = {
    'Fundamentals' :  [('Balance', '{}/api/v1/equity/fundamental/balance?provider=fmp&symbol={}&limit={}&period={}'),
                       ('Cashflow', '{}}/api/v1/equity/fundamental/cash?provider=fmp&symbol={}&limit={}&period={}'),
                        ('Income', '{}}/api/v1/equity/fundamental/income?provider=fmp&symbol={}&limit={}&period={}')
                       ],
    'Economy'      : [('Consumer Sentiment', '{}/api/v1/economy/survey/university_of_michigan?provider=fred&start_date={}'),
                      ('NonFarm Payroll' , '{}/api/v1/economy/survey/nonfarm_payrolls?provider=fred&category=employees_nsa&start_date={}'),
                      ('Consumer price index', '{}/api/v1/economy/cpi?provider=fred&country=united_states&transform=yoy&frequency=monthly&harmonized=false&start_date={}&expenditure=total'),
                      ('Producer price index', '{}/api/v1/economy/indicators?provider=econdb&symbol=PPI&country=US&start_date={}&frequency=month&use_cache=true'),
                      ('Job layoffs rate', '{}/api/v1/economy/indicators?provider=econdb&symbol=JLR&country=US&start_date={}&frequency=month&use_cache=true'),
                      ('Job hires rate', '{}/api/v1/economy/indicators?provider=econdb&symbol=JHR&country=US&start_date={}&frequency=month&use_cache=true'),
                      ('House  Price Index', '{}/api/v1/economy/house_price_index?provider=oecd&country=united_states&frequency=quarter&transform=index&start_date={}')

                      ],
    'Quote'        : '{}/api/v1/equity/price/quote?provider=fmp&symbol={}&source=iex',
    'Senate'       : '/',
    'EconomicCalendar' : '{}/api/v1/economy/calendar?provider=fmp&start_date={}&end_date={}&country=US',
    'Markets'           :  [ ('SP500'  , '{}/api/v1/index/price/historical?provider=fmp&symbol=^SPX^II&interval=1d&limit=125&sort=asc'), #125 days to compute mv average for fear and greed
                             ('Nasdaq' , '{}/api/v1/index/price/historical?provider=fmp&symbol=QQQ&interval=1d&limit=125&sort=asc'), #125 days to compute mv average for fear and greed
                             ('Russell' ,'{}/api/v1/index/price/historical?provider=fmp&symbol=^RUT&interval=1d&limit=125&sort=asc'), #125 days to compute mv average for fear and greed
                             ('VIX'     , '{}/api/v1/index/price/historical?provider=fmp&symbol=^VIX&interval=1d&limit=5&sort=asc'),
                             ('IVW'    , '{}/api/v1/index/price/historical?provider=fmp&symbol=IVW&interval=1d&limit=125&sort=asc'), #125 days to compute mv average for fear and greed
                             ('IVE'    , '{}/api/v1/index/price/historical?provider=fmp&symbol=IVE&interval=1d&limit=125&sort=asc'), #125 days to compute mv average for fear and greed
                           ],
    'Profile'          : [('Company Profile', '{}/api/v1/equity/profile?provider=fmp&symbol={}'),
                          ('Competitors', '{}/api/v1/equity/profile?provider=fmp&symbol={}'),
                          ('Insider Trading', '{}/api/v1/equity/ownership/insider_trading?provider=fmp&symbol={}&limit=500&start_date={}&sort_by=updated_on')


                          ],
    'News'             :  '{}/api/v1/news/world?provider=fmp&limit={}&display=full&sort=created&order=desc&offset=0',
    'Ratios'           : '{}/api/v1/equity/fundamental/ratios?provider=fmp&symbol={}&limit={}&period={}'
}



class OBBRester:
    def __init__(self, base_url='http://locahost:8000'):
        self.base_url = base_url

    def fundamentals(self, ticker, period='annual', limit=5):
        fundamental_urls = DATA_DICT['Markets']
        holder = {}

        try:
            for key, url in fundamental_urls:
                formatted_url = fundamental_urls.format(self.base_url, ticker, period, limit)
                logging.info(f'Calling:{formatted_url}')
                holder[key] = requests.get(formatted_url).json()
        except Exception as e:
            logging.info(f'Exception in querying :{str(e)}')
        return holder

    def ratios(self, ticker, period='annual', limit=5):
        overview_urls = DATA_DICT['Ratios']
        formatted_url = overview_urls.format(self.base_url, ticker, period, limit)
        logging.info(f'Opening:{formatted_url}')
        return requests.get(formatted_url).json()

    def economy(self, ticker, annual=True, max_period=5):
        market_urls = DATA_DICT['Markets']
        holder = {}

        start_date = date(date.today().year, 1, 1) # we collect economic indic from beginning of year
        start_date_str = start_date.strftime('%Y-%m-%d')

        try:
            for key, url in market_urls:
                formatted_url = url.format(self.base_url, start_date_str)
                logging.info(f'Calling:{formatted_url}')
                holder[key] = requests.get(formatted_url).json()
        except Exception as e:
            logging.info(f'Exception in querying :{str(e)}')
        return holder

    def markets(self):
        market_urls = DATA_DICT['Markets']
        holder = {}
        try:
            for key, url in market_urls:
                formatted_url = url.format(self.base_url)
                logging.info(f'Calling:{formatted_url}')
                holder[key] = requests.get(formatted_url).json()
        except Exception as e:
            logging.info(f'Exception in querying :{str(e)}')
        return holder
    def quote(self, ticker):
        pass

    def senate(self):
        pass

    def overview(self, ticker):
        overview_urls = DATA_DICT['Profile']
        holder = []
        try:
            for key, url in overview_urls:
                formatted_url = overview_urls.format(self.base_url, ticker)
                logging.info(f'Opening:{formatted_url}')
                holder[key] =  requests.get(formatted_url).json()

            insiders = overview_urls[-1]
            start_date = (date.today() - BDay(60)).strftime('%Y-%m-%d')
            insiders_url = insiders.format(self.base_url, ticker, start_date)
            holder['Insider Trading'] = requests.get(insiders_url).json()
        except Exception as e:
            logging.info(f'Exception in querying :{str(e)}')
        return holder
    def screener(self, preset):
        pass

    def news(self, limit=20):
        overviewUrls = DATA_DICT['News']
        formattedUrl = overviewUrls.format(self.base_url, limit)
        logging.info(f'Opening:{formattedUrl}')
        return requests.get(formattedUrl).json()






