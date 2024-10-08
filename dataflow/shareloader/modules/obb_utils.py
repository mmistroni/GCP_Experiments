import requests
import logging
from datetime import datetime
from openbb_yfinance.models.equity_historical import YFinanceEquityHistoricalFetcher
import apache_beam as beam
from pandas.tseries.offsets import BDay
from datetime import date
from pandas.tseries.offsets import BDay
import asyncio

def create_bigquery_ppln(p):
    plus500_sql = """SELECT *  FROM `datascience-projects.gcp_shareloader.plus500`"""
    logging.info('executing SQL :{}'.format(plus500_sql))
    return (p | 'Reading-plus500}' >> beam.io.Read(
        beam.io.BigQuerySource(query=plus500_sql, use_standard_sql=True))

            )

class AsyncProcess(beam.DoFn):

    def __init__(self, credentials, start_date, price_change=0.07):
        self.credentials = credentials
        self.fetcher = YFinanceEquityHistoricalFetcher
        self.end_date = start_date
        self.start_date = (self.end_date - BDay(1)).date()
        self.price_change = price_change

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









