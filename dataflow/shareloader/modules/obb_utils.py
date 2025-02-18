import requests
import logging
from datetime import datetime
from openbb_yfinance.models.equity_historical import YFinanceEquityHistoricalFetcher
import apache_beam as beam
from pandas.tseries.offsets import BDay
from datetime import date
from pandas.tseries.offsets import BDay
import asyncio

import pandas as pd
from openbb_multpl.models.sp500_multiples import MultplSP500MultiplesFetcher

def create_bigquery_ppln(p):
    plus500_sql = """SELECT *  FROM `datascience-projects.gcp_shareloader.plus500`"""
    logging.info('executing SQL :{}'.format(plus500_sql))
    return (p | 'Reading-plus500}' >> beam.io.Read(
        beam.io.BigQuerySource(query=plus500_sql, use_standard_sql=True))

            )

class AsyncProcessSP500Multiples(beam.DoFn):

    def __init__(self, credentials):
        self.credentials = credentials
        self.fetcher = MultplSP500MultiplesFetcher

    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')

        params = dict(series_name=element)
        try:
            # 1. We need to get the close price of the day by just querying for 1d interval
            # 2. then we get the pre-post market. group by day and get latest of yesterday and latest of
            #    today
            # 3. we aggregate and store in bq
            # 4 .send email for everything that increased over 10% overnight
            # 5 . also restrict only for US. drop every ticker which has a .<Exchange>

            data = await self.fetcher.fetch_data(params, {})
            result =  [d.model_dump(exclude_none=True) for d in data]
            if result:



                logging.info(f'Result is :{result}. Looking for latest close ')
                latest = result[-1]
                return [{'AS_OF_DATE' : latest['date'].strftime('%Y-%m-%d'),
                        'LABEL' : element.upper(), 'VALUE': latest['value']}]
            else:
                return -1
        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return -1

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))
    #https://wire.insiderfinance.io/implement-buffets-approach-with-python-and-streamlit-5d3a7bc42b89



class AsyncProcessCorporate(beam.DoFn):

    def __init__(self, credentials):
        self.credentials = credentials
        self.fetcher = MultplSP500MultiplesFetcher

    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')

        params = dict(series_name=element)
        try:
            data = await self.fetcher.fetch_data(params, {})
            result = [d.model_dump(exclude_none=True) for d in data]
            if result:
                logging.info(f'Result is :{result}. Looking for latest close ')
                latest = result[-1]
                return [{'AS_OF_DATE': latest['date'].strftime('%Y-%m-%d'),
                         'LABEL': element.upper(), 'VALUE': latest['value']}]
            else:
                return -1
        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return -1

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))


class ProcessHistorical(beam.DoFn):

    def __init__(self, fmpKey, end_date):
        self.fmpKey = fmpKey
        self.end_date = end_date
        self.start_date = (end_date -BDay(30)).date()

    def get_adx_and_rsi(self, ticker):
        adx_url = f'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=adx&period=14&apikey={self.fmpKey}'
        rsi_url = f'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=rsi&period=10&apikey={self.fmpKey}'

        try:
            adx = requests.get(adx_url).json()
            latest = adx[0]
            rsi = requests.get(rsi_url).json()
            latest_rsi= rsi[0]
            return  (ticker , {'ADX' : latest['adx'],'RSI' : latest_rsi['rsi']})
        except Exception as e:
            logging.info(f'Failed tor etrieve data for {ticker}:{str(e)}')
            return (ticker , {'ADX': 0, 'RSI': 0})



    def fetch_data(self, element: str):
        logging.info(f'element is:{element},start_date={self.start_date}, end_date={self.end_date}')
        ticks = element.split(',')
        all_records = []
        for t in ticks:
            data = self.get_adx_and_rsi(t)
            all_records.append(data)
        return all_records

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        return self.fetch_data(element)

class AsyncProcess(beam.DoFn):

    def __init__(self, credentials, start_date, price_change=0.07, selection='Plus500'):
        self.credentials = credentials
        self.fetcher = YFinanceEquityHistoricalFetcher
        self.end_date = start_date
        self.start_date = (self.end_date - BDay(1)).date()
        self.price_change = price_change
        self.selection = selection
        self.fmpKey = credentials['key']


    def get_adx_and_rsi(self, ticker):
        adx_url = f'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=adx&period=14&apikey={self.fmpKey}'
        rsi_url = f'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=rsi&period=10&apikey={self.fmpKey}'

        try:
            adx = requests.get(adx_url).json()
            latest = adx[0]
            rsi = requests.get(rsi_url).json()
            latest_rsi= rsi[0]
            return  {'ADX' : latest['adx'],'RSI' : latest_rsi['rsi']}
        except Exception as e:
            logging.info(f'Failed tor etrieve data for {ticker}:{str(e)}')
            return {'ADX': 0, 'RSI': 0}


    def _sma(self, smaUrl):
        try:
            data = requests.get(smaUrl).json()
            if len(data) > 0:
                return data[0]['sma']
            return 0.0
        except Exception as e:
            logging.info(f'Failed to fetch:{smaUrl}: {str(e)}')
            return -1
    def calculate_smas(self, ticker):
        # https://medium.com/@wl8380/a-simple-yet-powerful-trading-strategy-the-moving-average-slope-method-b06de9d91455
        sma20 = f'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=sma&period=20&apikey={self.fmpKey}'
        sma50 = f'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=sma&period=50&apikey={self.fmpKey}'
        sma200 = f'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=sma&period=200&apikey={self.fmpKey}'
        try:

           r1 = self._sma(sma20)
           r2 = self._sma(sma50)
           r3 = self._sma(sma200)
           return {'SMA20': r1, 'SMA50': r2, 'SMA200' : r3} 

        except Exception as e:
            logging.info('CalculateSmas Failed to retreivve smas for {ticker}')
            return {'SMA20': 0, 'SMA50': 0, 'SMA200' : 0}

    def calculate_slope(self, ticker):
        # https://medium.com/@wl8380/a-simple-yet-powerful-trading-strategy-the-moving-average-slope-method-b06de9d91455
        
        try:
           sma20 = 'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=sma&period=20&apikey={self.fmpKey}'    
           r1 = requests.get(sma20).json()[0] 
           sma50 = 'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=sma&period=50&apikey={self.fmpKey}'     
           r2 = requests.get(sma50).json()[0] 
           sma200 = 'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=sma&period=200&apikey={self.fmpKey}'     
           r3 = requests.get(sma200).json()[0] 
           return {'SMA20': r1, 'SMA50': r2, 'SMA200' : r3} 

        except Exception as e:
            logging.info('Failed to retreivve smas for {ticker}')
            return {'SMA20': 0, 'SMA50': 0, 'SMA200' : 0}



    async def fetch_data(self, element: str):
        logging.info(f'element is:{element},start_date={self.start_date}, end_date={self.end_date}')

        ticks = element.split(',')
        all_records = []
        for t in ticks:
            params = dict(symbol=t, interval='1h', extended_hours=True, start_date=self.start_date,
                            end_date=self.end_date)

            logging.info(f'Attempting to retrieve data for {t}')
            try:
                # 1. We need to get the close price of the day by just querying for 1d interval
                # 2. then we get the pre-post market. group by day and get latest of yesterday and latest of
                #    today
                # 3. we aggregate and store in bq
                # 4 .send email for everything that increased over 10% overnight
                # 5 . also restrict only for US. drop every ticker which has a .<Exchange>

                data = await self.fetcher.fetch_data(params, {})
                result =  [d.model_dump(exclude_none=True) for d in data]

                # we can include adx and rsi,but we need to fetch it from a different run
                if result:
                    logging.info(f'{t} Result is :{result}. Looking for latest close @{self.start_date}')
                    last_close = [d for d in result if d['date'] == datetime(self.start_date.year, self.start_date.month,
                                                                            self.start_date.day,16, 0)][0]
                    latest = result[-1]
                    increase = latest['close'] / last_close['close']
                    if increase > (1 + self.price_change):
                        logging.info(f'Adding ({t}):{latest}')
                        latest['ticker'] = t
                        latest['symbol'] = t
                        latest['prev_date'] = last_close['date']
                        latest['prev_close'] = last_close['close']
                        latest['change'] = increase
                        latest['selection'] = self.selection

                        tech_dict = self.get_adx_and_rsi(t)
                        smas = self.calculate_smas(t)
                        latest.update(tech_dict)
                        latest.update(smas)
                        if latest['close'] > latest['SMA20']:
                            latest['highlight'] = True


                        all_records.append(latest)
                    else:
                        logging.info(f'{t} increase ({increase}) change below tolerance:{1 + self.price_change}')
                else:
                    logging.info(f'No result sfor {t}')
            except Exception as e:
                logging.info(f' x Failed to fetch data for {t}:{str(e)}')
        logging.info(f'Returningn records with :{len(all_records)}')
        return all_records

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))






