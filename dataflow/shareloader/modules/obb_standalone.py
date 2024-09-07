import requests
import json
import logging
from pandas.tseries.offsets import BDay
from datetime import date
from openbb_fmp.models.equity_quote import FMPEquityQuoteFetcher as quote_fetcher
from openbb_fmp.models.equity_profile import FMPEquityProfileFetcher as profile_fetcher
from openbb_fmp.models.balance_sheet import FMPBalanceSheetFetcher as balance_fetcher
from openbb_fmp.models.income_statement import FMPIncomeStatementFetcher as income_fetcher
from openbb_fmp.models.cash_flow import FMPCashFlowStatementFetcher as cashflow_fetcher
from openbb_fmp.models.financial_ratios import FMPFinancialRatiosFetcher as ratio_fetcher
from openbb_fmp.models.company_news import FMPCompanyNewsData as news_fetcher
import asyncio

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


class OBBStandaloneClient:

    def __init__(self, keys_dict):
        self.credentials = keys_dict

    async def fundamentals(self, ticker, period='annual', limit=5):


        pass
    async def ratios(self, ticker, period='annual', limit=5):
        pass

    async def economy(self):
        pass
    async def markets(self):
        pass

    async def quote(self, ticker):
        params = {'symbol': ticker}
        quote = await quote_fetcher.fetch_data(params, self.credentials)
        return [d.model_dump(exclude_none=True) for d in quote]

    async def senate(self):
        pass

    def overview(self, ticker):
        params = {'symbol': ticker}
        return  profile_fetcher.fetch_data(params, self.credentials)

    async def company_news(self, ticker, limit=20):
        params =  {'symbol' : ticker, 'limit' : limit }
        news_data = await news_fetcher.fetch_data(params, self.credentials)
        return  [d.model_dump(exclude_none=True) for d in news_data]

    async def market_news(self, ticker, limit=20):
        pass








