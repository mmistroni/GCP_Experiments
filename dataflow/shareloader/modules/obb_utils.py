import requests
import logging
from datetime import datetime
from openbb_yfinance.models.equity_historical import YFinanceEquityHistoricalFetcher
import apache_beam as beam
from pandas.tseries.offsets import BDay
from datetime import date
from pandas.tseries.offsets import BDay
import asyncio
import pandas_ta as ta
import pandas as pd
from openbb_multpl.models.sp500_multiples import MultplSP500MultiplesFetcher

def create_bigquery_ppln(p):
    plus500_sql = """SELECT *  FROM `datascience-projects.gcp_shareloader.plus500`"""
    logging.info('executing SQL :{}'.format(plus500_sql))
    return (p | 'Reading-plus500}' >> beam.io.Read(
        beam.io.BigQuerySource(query=plus500_sql, use_standard_sql=True))

            )

class AsyncProcess(beam.DoFn):

    def __init__(self, credentials, start_date, price_change=0.07, selection='Plus500'):
        self.credentials = credentials
        self.fetcher = YFinanceEquityHistoricalFetcher
        self.end_date = start_date
        self.start_date = (self.end_date - BDay(1)).date()
        self.price_change = price_change
        self.selection = selection
    async def fetch_data(self, element: str):
        logging.info(f'element is:{element},start_date={self.start_date}, end_date={self.end_date}')

        ticks = element.split(',')
        all_records = []
        for t in ticks:
            params = dict(symbol=t, interval='1h', extended_hours=True, start_date=self.start_date,
                            end_date=self.end_date)
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
                    logging.info(f'Result is :{result}. Looking for latest close @{self.start_date}')
                    last_close = [d for d in result if d['date'] == datetime(self.start_date.year, self.start_date.month,
                                                                            self.start_date.day,16, 0)][0]
                    latest = result[-1]
                    increase = latest['close'] / last_close['close']
                    if increase > (1 + self.price_change):
                        logging.info(f'Adding ({t}):{latest}')
                        latest['ticker'] = t
                        latest['prev_date'] = last_close['date']
                        latest['prev_close'] = last_close['close']
                        latest['change'] = increase
                        latest['selection'] = self.selection

                        all_records.append(latest)
                    else:
                        logging.info(f'{t} increase ({increase}) change below tolerance:{1 + self.price_change}')
                else:
                    logging.info(f'No result sfor {t}')
            except Exception as e:
                logging.info(f'Failed to fetch data for {t}:{str(e)}')
        logging.info(f'Returningn records with :{len(all_records)}')
        return all_records

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))



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

class AsyncProcessHistorical(beam.DoFn):

    def __init__(self, credentials, start_date, price_change=0.07, selection='Plus500'):
        self.credentials = credentials
        self.fetcher = YFinanceEquityHistoricalFetcher
        self.end_date = start_date
        self.start_date = (self.end_date - BDay(27)).date()
        self.price_change = price_change
        self.selection = selection
    async def fetch_data(self, element: str):
        logging.info(f'element is:{element},start_date={self.start_date}, end_date={self.end_date}')

        ticks = element.split(',')
        all_records = []
        for t in ticks:
            params = dict(symbol=t, interval='1d', extended_hours=True, start_date=self.start_date,
                            end_date=self.end_date)
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
                    df = pd.DataFrame(data=result)

                    df['RSI'] = ta.rsi(close=df['close'], length=14)
                    df['ADX'] = ta.adx(high=df['high'], low=df['Low'], close=df['close'], length=14)

                    latest = df[-1].to_dict('records')[0]

                    all_records.append({'ticker' : latest['ticker'], 'rsi' :latest['rsi'],'adx' : latest['adx']})

                else:
                    logging.info(f'No result sfor {t}')
            except Exception as e:
                logging.info(f'Failed to fetch data for {t}:{str(e)}')
        logging.info(f'Returningn records with :{len(all_records)}')
        return all_records

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))






