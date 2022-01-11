from __future__ import absolute_import

import argparse
import logging
import re

from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import re, requests
from datetime import datetime, date
from collections import OrderedDict
import requests
import pandas_datareader.data as dr
import logging
import apache_beam as beam
from datetime import date
import apache_beam.io.gcp.gcsfilesystem as gcs
from apache_beam.options.pipeline_options import PipelineOptions
from .superperf_metrics import get_all_data, get_fundamental_parameters, get_descriptive_and_technical,\
                                            get_financial_ratios, get_fundamental_parameters_qtr, get_analyst_estimates,\
                                            get_quote_benchmark, get_financial_ratios_benchmark, get_key_metrics_benchmark, \
                                            get_income_benchmark, get_balancesheet_benchmark
from apache_beam.io.gcp.internal.clients import bigquery


class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')
        parser.add_argument('--iistocks')

def get_descriptive_and_techincal_filter(input_dict):
    if not input_dict:
        return False
    return (input_dict.get('marketCap') is not None and input_dict.get('marketCap') > 300000000) and \
        (input_dict.get('avgVolume') is not None and input_dict.get('avgVolume') > 200000) \
                and (input_dict.get('price') is not None and input_dict.get('price') > 10) \
                and (input_dict.get('priceAvg50') is not None) and (input_dict.get('priceAvg200') is not None) \
                and (input_dict.get('priceAvg20') is not None) \
                and (input_dict.get('price') > input_dict.get('priceAvg20')) \
                and (input_dict.get('price') > input_dict.get('priceAvg50')) \
                and (input_dict.get('price') > input_dict.get('priceAvg200'))

def get_fundamental_filter(input_dict):
    if not input_dict:
        return False
    # \
    return (input_dict.get('net_sales_qtr_over_qtr', 0) > 0.2) and (input_dict.get('returnOnEquity', 0) > 0) \
             and (input_dict.get('grossProfitMargin', 0) > 0) and  (input_dict.get('eps_growth_this_year', 0) > 0.2) \
             and (input_dict.get('eps_growth_next_year', 0) > 0) and (input_dict.get('eps_growth_qtr_over_qtr', 0) > 0.2)    


def get_universe_filter(input_dict):
    logging.info('WE got data:{}'.format(input_dict))
    
    res = (input_dict.get('marketCap', 0) > 300000000) and (input_dict.get('avgVolume', 0) > 200000) \
        and (input_dict.get('price', 0) > 10) and (input_dict.get('eps_growth_this_year', 0) > 0.2) \
        and (input_dict.get('grossProfitMargin', 0) > 0) \
        and  (input_dict.get('price', 0) > input_dict.get('priceAvg20', 0))\
        and (input_dict.get('price', 0) > input_dict.get('priceAvg50', 0)) \
        and (input_dict.get('price', 0) > input_dict.get('priceAvg200', 0))  \
        and (input_dict.get('net_sales_qtr_over_qtr', 0) > 0.2) and (input_dict.get('returnOnEquity', 0) > 0) \
        and (input_dict.get('eps_growth_next_year', 0) > 0) and (input_dict.get('eps_growth_qtr_over_qtr', 0) > 0.2)
    
    if res:
        logging.info('Found one that passes filter:{}'.format(input_dict))
        return True
    else:
        return False        

class BaseLoader(beam.DoFn):
    def __init__(self, key) :
        self.key = key
    def process(self, elements):
        logging.info('Attepmting to get fundamental data for all elements {}'.format(len(elements.split(','))))
        all_dt = []
        for ticker in elements.split(','):
            ticker_data = get_descriptive_and_technical(ticker, self.key)
            all_dt.append(ticker_data)
        return all_dt


class FundamentalLoader(beam.DoFn):
    def __init__(self, key):
        self.key = key

    def process(self, elements):
        logging.info('Attepmting to get fundamental data for all elements {}'.format(len(elements)))
        logging.info('All data is\n{}'.format(elements))
        all_dt = []
        for ticker in elements.split(','):
            fundamental_data = get_fundamental_parameters(ticker, self.key)
            if fundamental_data:
                fundamental_qtr = get_fundamental_parameters_qtr(ticker, self.key)
                if fundamental_qtr:
                    fundamental_data.update(fundamental_qtr)
                    financial_ratios = get_financial_ratios(ticker, self.key)
                    if financial_ratios:
                        fundamental_data.update(financial_ratios)

                updated_dict = get_analyst_estimates(ticker, self.key, fundamental_data)
                descr_and_tech = get_descriptive_and_technical(ticker, self.key)
                updated_dict.update(descr_and_tech)
                all_dt.append(updated_dict)
        return all_dt



class BenchmarkLoader(beam.DoFn):
    def __init__(self, key):
        self.key = key

    def process(self, elements):
        logging.info('Attepmting to get fundamental data for all elements {}'.format(len(elements)))
        logging.info('All data is\n{}'.format(elements))
        all_dt = []
        for ticker in elements.split(','):
            quotes_data = get_quote_benchmark(ticker, self.key)
            if quotes_data:
                income_data = get_income_benchmark(ticker, self.key)
                if income_data:
                    quotes_data.update(income_data)
                    balance_sheet_data = get_balancesheet_benchmark(ticker, self.key)
                    if balance_sheet_data:
                        quotes_data.update(balance_sheet_data)
                        financial_ratios_data = get_financial_ratios_benchmark(ticker, self.key)
                        if financial_ratios_data:
                            quotes_data.update(financial_ratios_data)
                            key_metrics_dta = get_key_metrics_benchmark(ticker, self.key)
                            if key_metrics_dta:
                                quotes_data.update(key_metrics_dta)
                        all_dt.append(quotes_data)
        return all_dt


def write_to_bucket(lines, sink):
    return (
            lines | 'Writing to bucket' >> sink
    )

def combine_tickers(input):
    return ','.join(input)


def combine_dict(input):
    print('Combining {}'.format(input))
    return [d for d in input]

def load_fundamental_data(source,fmpkey):
    return (source
            | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(FundamentalLoader(fmpkey))
            | 'Filtering out none fundamentals' >> beam.Filter(lambda item: item is not None)
            | 'filtering on descr and technical' >> beam.Filter(get_descriptive_and_techincal_filter)
            | 'Using fundamental filters' >> beam.Filter(get_fundamental_filter)
            )

def load_benchmark_data(source,fmpkey):
    return (source
            | 'Combine all at fundamentals bench' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals bench' >> beam.ParDo(BenchmarkLoader(fmpkey))
            )

def filter_universe(data):
    return (data
             | 'Filtering on Universe' >> beam.Filter(get_universe_filter)
            )

def extract_data_pipeline(p, input_file):
    logging.info('r?eadign from:{}'.format(input_file))
    return (p
            | 'Reading Tickers' >> beam.io.textio.ReadFromText(input_file)
            | 'Converting to Tuple' >> beam.Map(lambda row: row.split(','))
            | 'Extracting only ticker and Industry' >> beam.Map(lambda item:(item[0]))
            
    )

def canslim_filter(input_dict):
    return (input_dict.get('avgVolume',0) > 200000) and (input_dict.get('eps_growth_this_year',0) > 0.2)\
         and (input_dict.get('eps_growth_next_year', 0) > 0.2) \
    and (input_dict.get('eps_growth_qtr_over_qtr', 0) > 0.2) and (input_dict.get('net_sales_qtr_over_qtr',0) > 0.2) \
    and (input_dict.get('eps_growth_past_5yrs',0) > 0.2) and (input_dict.get('returnOnEquity',0) > 0) \
    and (input_dict.get('grossProfitMargin', 0) > 0) and (input_dict.get('institutionalHoldingsPercentage', 0) > 0.3) \
    and (input_dict.get('price',0) > input_dict.get('priceAvg20', 0)) and (input_dict.get('price',0) > input_dict.get('priceAvg50',0)) \
    and (input_dict.get('price', 0) > input_dict.get('priceAvg200',0)) and (input_dict.get('sharesOutstanding',0) > 50000000)

def stocks_under_10m_filter(input_dict):
    return (input_dict['marketCap'] < 10000000000) and (input_dict['avgVolume'] > 100000) \
                            and (input_dict['eps_growth_this_year'] > 0) and (input_dict['eps_growth_next_year'] > 0.25) \
                            and (input_dict['eps_growth_qtr_over_qtr'] > 0.2) and  (input_dict['net_sales_qtr_over_qtr'] > 0.25) \
                            and (input_dict['returnOnEquity'] > 0.15) and (input_dict['price'] > input_dict['priceAvg200'])

def new_high_filter(input_dict):
    return (input_dict['eps_growth_this_year'] > 0) and (input_dict['eps_growth_next_year'] > 0) \
                    and (input_dict['eps_growth_qtr_over_qtr'] > 0) and  (input_dict['net_sales_qtr_over_qtr'] > 0) \
                    and (input_dict.get('price') is not None) \
                    and (input_dict['returnOnEquity'] > 0) and (input_dict['price'] > input_dict['priceAvg20']) \
                    and (input_dict['price'] > input_dict['priceAvg50']) \
                    and (input_dict['price'] > input_dict['priceAvg200']) and (input_dict['change'] > 0) \
                    and (input_dict.get('changeFromOpen') is not None and  input_dict.get('changeFromOpen') > 0) \
                    and (input_dict.get('allTimeHigh') is not None) and (input_dict['price'] >= input_dict.get('allTimeHigh'))

def benchmark_filter(input_dict):
    fields_to_check = ['debtOverCapital', 'enterpriseDebt', 'epsGrowth',
                       'epsGrowth5yrs', 'positiveEps', 'positiveEpsLast5Yrs',
                       'tangibleBookValuePerShare', 'netCurrentAssetValue',
                       'dividendPaid', 'peRatio', 'currentRatio', 'priceToBookRatio',
                       'marketCap' ,'sharesOutstanding', 'institutionalOwnershipPercentage',
                       'price', 'dividendPaidEnterprise', 'dividendPaid', 'symbol']

    result = [input_dict.get(field) is not None for field in fields_to_check]
    return all(result)

def defensive_stocks_filter(input_dict):
    return  (input_dict['marketCap'] > 2000000000) and (input_dict['currentRatio'] >= 2) \
                   and (input_dict['debtOverCapital'] < 0) \
                   and (input_dict['dividendPaid'] == True)\
                   and (input_dict['epsGrowth'] >= 0.33) \
                   and (input_dict['positiveEps'] > 0 ) \
                   and (input_dict['peRatio'] > 0) and  (input_dict['peRatio'] <= 15) \
                   and (input_dict['priceToBookRatio'] > 0) and (input_dict['priceToBookRatio'] < 1.5) \
                   and (input_dict['institutionalOwnershipPercentage'] < 0.6)

def enterprise_stock_filter(input_dict):
    return (input_dict['currentRatio'] >= 1.5) \
                   and (input_dict['enterpriseDebt'] <= 1.2) \
                   and (input_dict['dividendPaidEnterprise'] == True)\
                   and (input_dict['epsGrowth5yrs'] > 0) \
                   and (input_dict['positiveEpsLast5Yrs'] == 5   ) \
                   and (input_dict['peRatio'] > 0) and  (input_dict['peRatio'] <= 10) \
                   and (input_dict['priceToBookRatio'] > 0) and (input_dict['priceToBookRatio'] < 1.5) \
                   and (input_dict['institutionalOwnershipPercentage'] < 0.6)



def find_leaf(p):
    pass

def find_stocks_under10m(p):
    pass

def find_stocks_alltime_high(p):
    pass

def write_to_bigquery(p, bq_sink, status):
    return (p | 'Mapping Tuple {}'.format(status) >> beam.Map(lambda d: (datetime.today().strftime('%Y-%m-%d'), d['ticker'], status))
              | 'Mapping to BQ Dict {}'.format(status) >> beam.Map(lambda tpl: dict(AS_OF_DATE=tpl[0], TICKER=tpl[1], STATUS=tpl[2]))
              | 'Writing to Sink for :{}'.format(status) >> bq_sink 
              )


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    input_file = 'gs://mm_dataflow_bucket/inputs/shares_dataset.csv-00000-of-00001'
    destination = 'gs://mm_dataflow_bucket/outputs/superperformers_benchmark_{}'.format(date.today().strftime('%Y-%m-%d %H:%M'))
    sink = beam.io.WriteToText(destination, num_shards=1)
    bq_sink = beam.io.WriteToBigQuery(
             bigquery.TableReference(
                projectId="datascience-projects",
                datasetId='gcp_shareloader',
                tableId='stock_selection'),
            schema='AS_OF_DATE:DATE,TICKER:STRING,LABEL:STRING,PRICE:FLOAT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    pipeline_options = XyzOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        tickers = extract_data_pipeline(p, input_file)

        if (pipeline_options.iistocks):
            benchmark_data = load_benchmark_data(tickers, pipeline_options.fmprepkey)

            benchmark_data | 'Sending to Sink' >> sink


            (benchmark_data | 'Filtering for all fields d ' >> beam.Filter(benchmark_filter)

                            | 'Filtering for defensive' >> beam.Filter(defensive_stocks_filter)
                            | 'Mapping only Relevant fields d' >> beam.Map(lambda d: dict(AS_OF_DATE=date.today(),
                                                                                          TICKER=d['symbol'],
                                                                                          LABEL='DEFENSIVE',
                                                                                          PRICE=d['price']))
                            | 'Writing to sink d' >> bq_sink)

            (benchmark_data | 'Filtering for all fields e ' >> beam.Filter(benchmark_filter)
            
                            | 'Filtering for enterprise' >> beam.Filter(enterprise_stock_filter)
                            | 'Mapping only Relevant fields ent' >> beam.Map(lambda d: dict(AS_OF_DATE=date.today(),
                                                                                         TICKER=d['symbol'],
                                                                                         LABEL='ENTERPRISE',
                                                                                         PRICE=d['price']))
                            | 'Writing to sink e' >> bq_sink)
        else:

            fundamental_data = load_fundamental_data(tickers, pipeline_options.fmprepkey)

            fundamental_data  |'Sendig to sink' >> sink

            (fundamental_data | 'Mapping only Relevant fields' >> beam.Map(lambda d: dict(AS_OF_DATE=date.today(),
                                                                                        TICKER=d['symbol'],
                                                                                        LABEL='STOCK_UNIVERSE',
                                                                                        PRICE=d['price']))
                             | 'Writing to stock selection' >> bq_sink)

            (fundamental_data | 'Canslimm filter' >> beam.Filter(canslim_filter)
                              |'Mapping only Relevant canslim fields' >> beam.Map(lambda d: dict(AS_OF_DATE=date.today(),
                                                                                          TICKER=d['symbol'],
                                                                                          LABEL='CANSLIM',
                                                                                        PRICE=d['price']))
                                | 'Writing to stock selection C' >> bq_sink)

            (fundamental_data | 'stock under 10m filter' >> beam.Filter(stocks_under_10m_filter)
             | 'Mapping only Relevant xm fields' >> beam.Map(lambda d: dict(AS_OF_DATE=date.today(),
                                                                                 TICKER=d['symbol'],
                                                                                 LABEL='UNDER10M',
                                                                                 PRICE=d['price']))
             | 'Writing to stock selection 10' >> bq_sink)

            (fundamental_data | 'stock NEW HIGHGS' >> beam.Filter(new_high_filter)
             | 'Mapping only Relevant nh fields' >> beam.Map(lambda d: dict(AS_OF_DATE=date.today(),
                                                                            TICKER=d['symbol'],
                                                                            LABEL='NEWHIGHS',
                                                                                PRICE=d['price']))
             | 'Writing to stock selection nh' >> bq_sink)




