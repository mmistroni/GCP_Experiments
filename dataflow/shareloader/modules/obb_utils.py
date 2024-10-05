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
    plus500_sql = """SELECT TICKER  FROM `datascience-projects.gcp_shareloader.plus500`"""
    logging.info('executing SQL :{}'.format(plus500_sql))
    return (p | 'Reading-plus500}' >> beam.io.Read(
        beam.io.BigQuerySource(query=plus500_sql, use_standard_sql=True))

            )

class AsyncProcess(beam.DoFn):

    def __init__(self, credentials, start_date):
        self.credentials = credentials
        self.fetcher = YFinanceEquityHistoricalFetcher
        self.start_date = start_date
        self.end_date = (start_date - BDay(1)).date()

    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')

        ticks = element.split(',')
        for t in ticks:
            params = dict(symbol=t, interval='1h', extended_hours=True, start_date=self.start_date,
                            end_date=self.end_date)
            try:
                data = await self.fetcher.fetch_data(params, {})
                all_records=  [d.model_dump(exclude_none=True) for d in data]
                filtered =  [r for r in all_records if r['date'] < datetime(
                                                                    self.start_date.year,
                                                                    self.start_date.month,
                                                                    self.start_date.day,
                                                                    9, 0, 0)]
                if all_records:
                    return all_records
                else:
                    return []
            except Exception as e:
                logging.info('Failed to fetch data for {t}:{str(e)}')
        return all_records

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))









