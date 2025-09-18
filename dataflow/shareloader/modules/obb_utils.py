import requests
import logging
from datetime import datetime
from openbb_yfinance.models.equity_historical import YFinanceEquityHistoricalFetcher
from openbb_fmp.models.equity_quote import FMPEquityQuoteFetcher
import apache_beam as beam
from pandas.tseries.offsets import BDay
import asyncio
from openbb_multpl.models.sp500_multiples import MultplSP500MultiplesFetcher
import time
from scipy.stats import linregress
import pandas as pd
import pandas as pd
from ta.volume import OnBalanceVolumeIndicator, ChaikinMoneyFlowIndicator
from typing import List

def create_bigquery_ppln(p):
    plus500_sql = """SELECT *  FROM `datascience-projects.gcp_shareloader.plus500`"""
    logging.info('executing SQL :{}'.format(plus500_sql))
    return (p | 'Reading-plus500}' >> beam.io.Read(
        beam.io.BigQuerySource(query=plus500_sql, use_standard_sql=True))

            )

def get_ta_indicators(data:List[dict]) -> dict:
    try:
        df = pd.DataFrame(data)
        # Calculate On-Balance Volume (OBV)
        # The OBV indicator uses 'close' and 'volume' columns.
        obv_indicator = OnBalanceVolumeIndicator(close=df['close'], volume=df['volume'])
        df['obv'] = obv_indicator.on_balance_volume()

        # Calculate Chaikin Money Flow (CMF)
        # The CMF indicator requires 'high', 'low', 'close', and 'volume' columns.
        cmf_indicator = ChaikinMoneyFlowIndicator(
            high=df['high'],
            low=df['low'],
            close=df['close'],
            volume=df['volume']
        )
        df['cmf'] = cmf_indicator.chaikin_money_flow()

        # Get column names
        cmf_column = [col for col in df.columns if 'cmf' in col][0]
        obv_column = 'obv'  # pandas_ta default name for OBV

        # Extract the last two rows and convert to a dictionary for clear output
        last_two_values = df.iloc[-2:][[obv_column, cmf_column]].to_dict(orient='records')

        obvlist = df['obv'].tolist()[-50:]
        cmflist = df['cmf'].tolist()[-50:]

        # 4. Display the results
        # We'll print the last 5 rows to show the newly added columns.
        logging.info("\nDataFrame with OBV and CMF indicators:")
        logging.info(df.tail())
        last_record_dict = df.iloc[-1].to_dict()
        logging.info(f'returning :{last_record_dict}')

        volume_dict = {'previous_obv' : last_two_values[0][obv_column],
                       'current_obv' : last_two_values[1][obv_column],
                       'previous_cmf' : last_two_values[0][cmf_column],
                       'last_cmf'     : last_two_values[1][cmf_column],
                       'obv_last_50_days' : obvlist,
                       'cmf_last_50_days' : cmflist
                       }

        return volume_dict
    except Exception as e:
        logging.info(f'Faile dto fetch obv for {str(e)}')
        return {}






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

    def __init__(self, credentials, start_date, price_change=0.07,
                    selection='Plus500', batchsize=20, linregdays=30):
        self.credentials = credentials
        self.fetcher = YFinanceEquityHistoricalFetcher
        self.end_date = start_date
        self.start_date = (self.end_date - BDay(1)).date()
        self.price_change = price_change
        self.selection = selection
        self.fmpKey = credentials['key']
        self.batch_size = batchsize
        self.linregdays = linregdays


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

    def get_pandas_ta_indicators(self, ticker):
        data = self._fetch_historical_data(ticker)[::-1]
        return get_ta_indicators(data)

    def get_profile(self, ticker):
        profile_url = f'https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={self.fmpKey}'
        try:
            profile = requests.get(profile_url).json()
            latest = profile[0]
            return {'sector' : latest['sector'], 'industry' : latest['industry']}
        except Exception as e :
            logging.info(f'Exceptioin  for {ticker}:{str(e)}')
            return {'sector': 'NA', 'industry': 'NA'}

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

    def _fetch_historical_data(self, ticker):
        logging.info(f'Fetching historical for {ticker}')
        try:
            hist_url = f'https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?apikey={self.fmpKey}'
            data = requests.get(hist_url).json().get('historical')
            return data
        except Exception as e:
            logging.info(f'Could not find historical for {ticker}:@{str(e)}')
            return []


    def calculate_slope(self, ticker):
        # https://medium.com/@wl8380/a-simple-yet-powerful-trading-strategy-the-moving-average-slope-method-b06de9d91455
        logging.info('Calculating slope for {ticker}')
        try:
            data = self.fetch_data(ticker)
            if data:
                prices =  [d['adjClose'] for d in data[:self.linregdays]][::-1]

                xs = range(1, len(prices) + 1)

                slope, intercept, r_value, p_value, std_err = linregress(xs, prices)

                # --- 3. Interpret the Slope ---
                logging.info(f"Calculated Slope: {slope:.4f}")
                logging.info(f"Intercept: {intercept:.4f}")
                logging.info(f"R-squared value: {r_value ** 2:.4f}")  # R-squared tells you how well the line fits the data
                return slope
            return 0
        except Exception as e:
            logging.info(f'Failed to retreivve  slope for {ticker}:{str(e)}')
            return 0

    def calculate_slope2(self, ticker):
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

        items = element.split(',')
        batches = []
        for i in range(0, len(items), self.batch_size):
            batch = items[i : i + self.batch_size]

            batches.append(batch)
        for b in batches:
            symbol = ','.join(b)
            params = dict(symbol=symbol, interval='1h', extended_hours=True, start_date=self.start_date,
                            end_date=self.end_date)
            #logging.info(f'xxxttempting to retrieve data for {t}')
            try:
                # 1. We need to get the close price of the day by just querying for 1d interval
                # 2. then we get the pre-post market. group by day and get latest of yesterday and latest of
                #    today
                # 3. we aggregate and store in bq
                # 4 .send email for everything that increased over 10% overnight
                # 5 . also restrict only for US. drop every ticker which has a .<Exchange>

                data = await self.fetcher.fetch_data(params, {})
                result =  [d.model_dump(exclude_none=True) for d in data]

                for ticker in b:
                    if len(b) > 1:
                        ticker_result = [d for d in result if d['symbol'] == ticker]
                    else:
                        ticker_result = [d for d in result]
                # we can include adx and rsi,but we need to fetch it from a different run
                    if ticker_result:
                        #logging.info(f'StartDate:{self.start_date} {t} Result is :{result[-1]}. Looking for latest close @{self.start_date}')
                        last_close = [d for d in ticker_result if d['date'] == datetime(self.start_date.year, self.start_date.month,
                                                                                self.start_date.day,16, 0)][0]
                        logging.info(f'Last close\n:{last_close}')
                        latest = ticker_result[-1]
                        logging.info(f'Latest\n{latest}')
                        increase = latest['close'] / last_close['close']

                        checker_negative = lambda x: x < (1 + self.price_change)
                        checker_positive = lambda x: x > (1 + self.price_change)

                        func_checker = checker_negative if self.price_change < 0 else checker_positive

                        logging.info(f'Increase for {ticker}={increase} vs {1 + self.price_change}')
                        if func_checker(increase) :
                            slope = self.calculate_slope(ticker)
                            logging.info(f'Adding ({ticker}):{latest}')
                            latest['ticker'] = ticker
                            latest['symbol'] = ticker
                            latest['date'] = 'today'
                            latest['prev_date'] = 'prevdate'
                            latest['prev_close'] = last_close['close']
                            latest['change'] = increase
                            latest['selection'] = self.selection
                            latest['slope'] = slope
                            tech_dict = self.get_adx_and_rsi(ticker)
                            profile = self.get_profile(ticker)
                            latest.update(profile)
                            pandas_indic_dict = self.get_pandas_ta_indicators(ticker)
                            latest.update(pandas_indic_dict)
                            smas = self.calculate_smas(ticker)
                            latest.update(tech_dict)
                            latest.update(smas)
                            if latest.get('close') > latest.get('SMA20'):
                                latest['highlight'] = 'True'

                            all_records.append(latest)
                        else:
                            logging.info(f"{ticker} change ({increase}) missed tolerance:{1 + self.price_change}.Latest:{latest['close']}.Last:{last_close['close']}")
                            continue

                else:
                    #logging.info(f'No result sfor {t}')
                    continue
            except Exception as e:
                import time
                logging.info(f' x Failed to fetch data for {b}:{str(e)}')
        logging.info(f'Returningn records with :{len(all_records)}')
        return all_records

    def process(self, element: str):
        #logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))


class AsyncFMPProcess(AsyncProcess):

    def __init__(self, credentials, start_date, price_change=0.07, selection='Plus500', batchsize=20,
                 linregdays=30):
        self.credentials = credentials
        self.fetcher = FMPEquityQuoteFetcher
        self.end_date = start_date
        self.start_date = (self.end_date - BDay(1)).date()
        self.price_change = price_change
        self.selection = selection
        self.fmpKey = credentials['fmp_api_key']
        self.batch_size = batchsize
        self.linregdays = linregdays

    async def fetch_data(self, element: str):
        logging.info(f'element is:{element},start_date={self.start_date}, end_date={self.end_date}')

        ticks = element.split(',')
        all_records = []
        for tick in ticks:
            params = dict(symbol=tick, start_date=self.start_date,
                          end_date=self.end_date)
            # sleeping for 10 seconds
            time.sleep(3)
            # logging.info(f'xxxttempting to retrieve data for {t}')
            try:
                # 1. We need to get the close price of the day by just querying for 1d interval
                # 2. then we get the pre-post market. group by day and get latest of yesterday and latest of
                #    today
                # 3. we aggregate and store in bq
                # 4 .send email for everything that increased over 10% overnight
                # 5 . also restrict only for US. drop every ticker which has a .<Exchange>

                data = await self.fetcher.fetch_data(params, self.credentials)
                result = [d.model_dump(exclude_none=True) for d in data]
                if result:
                    # logging.info(f'StartDate:{self.start_date} {t} Result is :{result[-1]}. Looking for latest close @{self.start_date}')
                    latest = result[-1]
                    logging.info(f'Latest\n{latest}')
                    increase = latest.get('last_price', 0) / latest.get('prev_close', 1)

                    checker_negative = lambda x: x < (1 + self.price_change)
                    checker_positive = lambda x: x > (1 + self.price_change)

                    func_checker = checker_negative if self.price_change < 0 else checker_positive
                    logging.info(f'Increase for {tick}={increase} vs {1+self.price_change}')
                    if func_checker(increase):
                        slope = self.calculate_slope(tick)
                        logging.info(f'Adding ({tick}):{latest}')
                        latest['ticker'] = tick
                        latest['symbol'] = tick
                        latest['prev_date'] = ''
                        latest['prev_close'] =  latest.get('prev_close', 1)
                        latest['change'] = increase
                        latest['selection'] = self.selection
                        latest['slope'] = slope
                        tech_dict = self.get_adx_and_rsi(tick)
                        profile = self.get_profile(tick)
                        latest.update(profile)
                        pandas_indic_dict = self.get_pandas_ta_indicators(tick)
                        latest.update(pandas_indic_dict)
                        # logging.info(f'{t} getting SMAS')
                        smas = self.calculate_smas(tick)
                        latest.update(tech_dict)
                        latest.update(smas)
                        if latest.get('last_price', 0) > latest.get('SMA20', 0):
                            latest['highlight'] = 'True'

                        all_records.append(latest)

                    else:
                        logging.info(
                            f"{tick} change ({increase}) missed tolerance:{1 + self.price_change}.Latest:{latest['last_price']}.Last:{latest.get('prev_close', 1)}")
                        continue
            except Exception as e:
                logging.info(f' x Failed to fetch data for {tick}:{str(e)}')
        logging.info(f'Returningn records with :{len(all_records)}')
        return all_records

    def process(self, element: str):
        # logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))








