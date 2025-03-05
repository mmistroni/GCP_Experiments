import logging
from datetime import datetime
from openbb_yfinance.models.equity_historical import YFinanceEquityHistoricalFetcher
import apache_beam as beam
from datetime import date
from pandas.tseries.offsets import BDay
import asyncio
from openbb_finviz.models.equity_screener import FinvizEquityScreenerFetcher
import pandas as pd
from openbb_multpl.models.sp500_multiples import MultplSP500MultiplesFetcher

class AsyncProcessFinvizTester(beam.DoFn):

    def __init__(self):
        self.fetcher = FinvizEquityScreenerFetcher
        

    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')
        try:
            descriptive_filters = {
                    'Price' : 'Over $10',
                    'Average Volume' : 'Over 200K',
                    'Relative Volume' : 'Over 1.5'
                }
            high = await self.fetcher.fetch_data(descriptive_filters, {})
            high_result = [d.model_dump(exclude_none=True) for d in high]

            if high_result:
                high_ticks = ','.join([d['symbol'] for d in high_result])
                logging.info(f' adv declie for  successfully retrieved')
                return [{'ADVANCE': len(high_result), 
                        'ADVANCING_TICKERS': high_ticks}]
            else:
                return[{}]
        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return  [str(e)]

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))

