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
import pandas as pd
from datetime import date
import apache_beam.io.gcp.gcsfilesystem as gcs
from apache_beam.options.pipeline_options import PipelineOptions
from .superperf_metrics import get_all_data, get_fundamental_parameters, get_descriptive_and_technical,\
                                            get_financial_ratios, get_fundamental_parameters_qtr, get_analyst_estimates,\
                                            get_quote_benchmark, get_financial_ratios_benchmark, get_key_metrics_benchmark, \
                                            get_income_benchmark, get_balancesheet_benchmark, get_asset_play_parameters,\
                                            calculate_piotrosky_score, compute_rsi, get_price_change
from apache_beam.io.gcp.internal.clients import bigquery

'''
Further source of infos
https://medium.com/@mancuso34/building-all-in-one-stock-economic-data-repository-6246dde5ce02
'''
class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')
        parser.add_argument('--iistocks')

def asset_play_filter(input_dict):
    if not input_dict:
        return False
    shares_outstanding = input_dict.get('sharesOutstanding', 0)
    marketCap = input_dict.get('marketCap', 0)
    bookValuePerShare = input_dict.get('bookValuePerShare', 0)
    assetValue = shares_outstanding * bookValuePerShare

    return assetValue > marketCap



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
                asset_play_dict = get_asset_play_parameters(ticker, self.key)
                updated_dict.update(asset_play_dict)

                piotrosky_score = calculate_piotrosky_score(self.key, ticker)
                latest_rsi = compute_rsi(ticker, self.key)
                updated_dict['piotroskyScore'] = piotrosky_score
                updated_dict['rsi'] = latest_rsi

                priceChangeDict = get_price_change(ticker, self.key)
                updated_dict.update(priceChangeDict)

                all_dt.append(updated_dict)
        return all_dt

def load_bennchmark_data(ticker, key):
    quotes_data = get_quote_benchmark(ticker, key)
    if quotes_data:
        income_data = get_income_benchmark(ticker, key)
        if income_data:
            quotes_data.update(income_data)
            balance_sheet_data = get_balancesheet_benchmark(ticker, key)
            if balance_sheet_data:
                quotes_data.update(balance_sheet_data)
                financial_ratios_data = get_financial_ratios_benchmark(ticker, key)
                if financial_ratios_data:
                    quotes_data.update(financial_ratios_data)
                    key_metrics_dta = get_key_metrics_benchmark(ticker, key)
                    if key_metrics_dta:
                        quotes_data.update(key_metrics_dta)
                        asset_play_dict = get_asset_play_parameters(ticker, key)
                        quotes_data.update(asset_play_dict)
                        # CHecking if assets > stocks outstanding
                        currentCompanyValue = quotes_data['sharesOutstanding'] * quotes_data['price']
                        # current assets
                        quotes_data['canBuyAllItsStock'] = quotes_data['totalAssets'] - currentCompanyValue
                        quotes_data['netQuickAssetPerShare'] = (quotes_data['totalCurrentAssets'] - \
                                                                quotes_data['totalCurrentLiabilities'] - \
                                                                quotes_data['inventory']) / quotes_data[
                                                                   'sharesOutstanding']

                        piotrosky_score = calculate_piotrosky_score(key, ticker)
                        latest_rsi = compute_rsi(ticker, key)
                        quotes_data['rsi'] = latest_rsi
                        quotes_data['piotroskyScore'] = piotrosky_score
                    priceChangeDict = get_price_change(ticker, key)
                    quotes_data.update(priceChangeDict)

    return quotes_data

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
                                asset_play_dict = get_asset_play_parameters(ticker, self.key)
                                quotes_data.update(asset_play_dict)
                                # CHecking if assets > stocks outstanding
                                currentCompanyValue = quotes_data['sharesOutstanding'] * quotes_data['price']
                                # current assets
                                quotes_data['canBuyAllItsStock'] = quotes_data['totalAssets'] - currentCompanyValue
                                quotes_data['netQuickAssetPerShare'] = (quotes_data['totalCurrentAssets'] -  \
                                                                        quotes_data['totalCurrentLiabilities'] - \
                                                                         quotes_data['inventory']) / quotes_data['sharesOutstanding']

                                piotrosky_score = calculate_piotrosky_score(self.key, ticker)
                                latest_rsi = compute_rsi(ticker, self.key)
                                quotes_data['rsi'] = latest_rsi
                                quotes_data['piotroskyScore'] = piotrosky_score

                            priceChangeDict = get_price_change(ticker, self.key)
                            quotes_data.update(priceChangeDict)
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
def load_microcap_data(source,fmpkey):
    return (source
            | 'Combine all at fundamentals microcap' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(FundamentalLoader(fmpkey))
            | 'Filtering out none fundamentals' >> beam.Filter(lambda item: item is not None)
            )

def load_benchmark_data(source,fmpkey):
    return (source
            | 'Combine all at fundamentals bench' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals bench' >> beam.ParDo(BenchmarkLoader(fmpkey))
            | 'Filtering  benchmark by price' >>  beam.Filter(lambda d: d.get('price', 0) > 10)
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

def microcap_filter(input_dict):
    return (input_dict['marketCap'] <= 200000000) and (input_dict['dividendPaid'] == 0) and \
           (input_dict.get('grossProfitMargin', 0) >= 0.5) and (input_dict['netProfitMargin'] >= 0.1)  and \
           (input_dict.get('currentRatio') >= 2) and (input_dict['avgVolume'] >= 10000) and \
           (input_dict.get('52weekChange') > 0) and  (input_dict.get('52weekChange') < 0.5)


def benchmark_filter(input_dict):
    fields_to_check = ['debtOverCapital', 'enterpriseDebt', 'epsGrowth',
                       'epsGrowth5yrs', 'positiveEps', 'positiveEpsLast5Yrs',
                       'tangibleBookValuePerShare', 'netCurrentAssetValue',
                       'dividendPaid', 'peRatio', 'currentRatio', 'priceToBookRatio',
                       'marketCap' ,'sharesOutstanding', 'institutionalOwnershipPercentage',
                       'price', 'dividendPaidEnterprise', 'dividendPaid', 'symbol',
                       'freeCashFlowPerShare']

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


def get_defensive_filter_df():
    filters = { 'marketCap' : 'marketCap > 2000000000',
                 'currentRatio' : 'currentRatio >= 2',
                  'debtOverCapital' : 'debtOverCapital < 0',
                  'dividendPaid' : 'dividendPaid == True',
                  'epsGrowth' : 'epsGrowth >= 0.33',
                   'positiveEps' : 'positiveEps > 0',
                   'peRatio' : 'peRatio <= 15',
                   'priceToBookRatio' : 'priceToBookRatio < 1.5',
                   'institutionalOwnershipPercentage': 'institutionalOwnershipPercentage < 0.6'}
    return pd.DataFrame(list(filters.items()), columns=['key', 'function'])


def enterprise_stock_filter(input_dict):
    return (input_dict['currentRatio'] >= 1.5) \
                   and (input_dict['enterpriseDebt'] <= 1.2) \
                   and (input_dict['dividendPaidEnterprise'] == True)\
                   and (input_dict['epsGrowth5yrs'] > 0) \
                   and (input_dict['positiveEpsLast5Yrs'] == 5   ) \
                   and (input_dict['peRatio'] > 0) and  (input_dict['peRatio'] <= 10) \
                   and (input_dict['priceToBookRatio'] > 0) and (input_dict['priceToBookRatio'] < 1.5) \
                   and (input_dict['institutionalOwnershipPercentage'] < 0.6)

def get_enterprise_filter_df():
    filters = {  'currentRatio' : 'currentRatio >= 1.5',
                  'enterpriseDebt' : 'enterpriseDebt <= 1.2',
                  'dividendPaidEnterprise' : 'dividendPaidEnterprise == True',
                  'epsGrowth5yrs' : 'epsGrowth5yrs  > 0',
                   'positiveEpsLast5Yrs' : 'positiveEpsLast5Yrs == 5',
                   'peRatio' : 'peRatio <= 10',
                   'priceToBookRatio' : 'priceToBookRatio < 1.5',
                   'institutionalOwnershipPercentage': 'institutionalOwnershipPercentage < 0.6'}
    return pd.DataFrame(list(filters.items()), columns=['key', 'function'])




def out_of_favour_filter(input_dict):
    return (input_dict['price'] <=  input_dict['priceAvg200'])

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

def map_to_bq_dict(input_dict, label):
    return dict(AS_OF_DATE=date.today(), TICKER=input_dict['symbol'], LABEL=label, PRICE=input_dict['price'],
                YEARHIGH=input_dict.get('yearHigh', 0.0), YEARLOW=input_dict.get('yearLow', 0.0),
                PRICEAVG50=input_dict.get('priceAvg50', 0.0), PRICEAVG200=input_dict.get('priceAvg200', 0.0),
                BOOKVALUEPERSHARE=input_dict.get('bookValuePerShare', 0.0),
                TANGIBLEBOOKVALUEPERSHARE=input_dict.get('tangibleBookValuePerShare', 0.0),
                CASHFLOWPERSHARE=input_dict.get('freeCashFlowPerShare',0.0), MARKETCAP=input_dict.get('marketCap', 0.0),
                ASSET_VALUE=input_dict.get('bookValuePerShare', 0.0) * input_dict.get('sharesOutstanding', 0.0),
                EXCESS_MARKETCAP=( input_dict.get('bookValuePerShare', 0.0) * input_dict.get('sharesOutstanding', 0.0)  ) - input_dict.get('marketCap', 0.0),
                DIVIDENDRATIO=input_dict.get('dividendPaidRatio', 0.0),
                PERATIO=input_dict.get('pe', 0.0),
                INCOME_STMNT_DATE=input_dict['income_statement_date'],
                INCOME_STMNT_DATE_QTR=input_dict.get('income_statement_qtr_date'),
                RSI=input_dict.get('rsi',-1),
                PIOTROSKY_SCORE=input_dict.get('piotroskyScore', -1),
                RETURN_ON_CAPITAL=input_dict.get('returnOnCapital', -1),
                NET_INCOME=input_dict.get('netIncome', -1),

                )


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    input_file = 'gs://mm_dataflow_bucket/inputs/shares_dataset.csv-00000-of-00001'
    destination = 'gs://mm_dataflow_bucket/outputs/superperformers_benchmark_{}'.format(date.today().strftime('%Y-%m-%d %H:%M'))
    bq_sink = beam.io.WriteToBigQuery(
             bigquery.TableReference(
                projectId="datascience-projects",
                datasetId='gcp_shareloader',
                tableId='stock_selection'),
            schema='AS_OF_DATE:DATE,TICKER:STRING,LABEL:STRING,PRICE:FLOAT,YEARHIGH:FLOAT,YEARLOW:FLOAT,PRICEAVG50:FLOAT,PRICEAVG200:FLOAT,BOOKVALUEPERSHARE:FLOAT,TANGIBLEBOOKVALUEPERSHARE:FLOAT,CASHFLOWPERSHARE:FLOAT,MARKETCAP:FLOAT,ASSET_VALUE:FLOAT,EXCESS_MARKETCAP:FLOAT,DIVIDENDRATIO:FLOAT,PERATIO:FLOAT,INCOME_STMNT_DATE:STRING,INCOME_STMNT_DATE_QTR:STRING,RSI:FLOAT,PIOTROSKY_SCORE:FLOAT,NET_INCOME:FLOAT,RETURN_ON_CAPITAL:FLOAT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    pipeline_options = XyzOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        tickers = extract_data_pipeline(p, input_file)

        if (pipeline_options.iistocks):
            benchmark_data = load_benchmark_data(tickers, pipeline_options.fmprepkey)

            destination = 'gs://mm_dataflow_bucket/outputs/superperformers_stockbenchmarks_{}'.format(
                date.today().strftime('%Y-%m-%d %H:%M'))

            sink = beam.io.WriteToText(destination, num_shards=1)
            benchmark_data | 'Sendig to bqsink' >> sink

            (benchmark_data | 'Filtering for all fields d ' >> beam.Filter(benchmark_filter)

                            | 'Filtering for defensive' >> beam.Filter(defensive_stocks_filter)
                            | 'Mapping only Relevant fields d' >> beam.Map(lambda d:
                                                                           map_to_bq_dict(d, 'DEFENSIVE'))
                            | 'Writing to sink d' >> bq_sink)

            (benchmark_data | 'Filtering for all fields e ' >> beam.Filter(benchmark_filter)
            
                            | 'Filtering for enterprise' >> beam.Filter(enterprise_stock_filter)
                            | 'Mapping only Relevant fields ent' >> beam.Map(lambda d:
                                                                             map_to_bq_dict(d, 'ENTERPRISE'))
                            | 'Writing to sink e' >> bq_sink)

            (benchmark_data | 'Filtering for all fields AP ' >> beam.Filter(benchmark_filter)
             | 'Filtering for defensive ap' >> beam.Filter(defensive_stocks_filter)
             | 'Filtering for asset play defensive' >> beam.Filter(asset_play_filter)
             | 'Mapping only Relevant fields AP' >> beam.Map(lambda d:
                                                                map_to_bq_dict(d, 'ASSET_PLAY_DEFENSIVE'))
             | 'Writing to sink ap' >> bq_sink)

            (benchmark_data | 'Filtering for all fields ENT ' >> beam.Filter(benchmark_filter)
             | 'Filtering for ent ap' >> beam.Filter(enterprise_stock_filter)
             | 'Filtering for asset play enterprise' >> beam.Filter(asset_play_filter)
             | 'Mapping only Relevant fields ENT' >> beam.Map(lambda d:
                                                             map_to_bq_dict(d, 'ASSET_PLAY_ENTERPRISE'))
             | 'Writing to sink ENT' >> bq_sink)

            (benchmark_data | 'Filtering for all fields AP2 ' >> beam.Filter(benchmark_filter)
             | 'Filtering for defensive ap2' >> beam.Filter(defensive_stocks_filter)
             | 'Filtering for out of favour ' >> beam.Filter(out_of_favour_filter)
             | 'Mapping only Relevant fields AP2' >> beam.Map(lambda d:
                                                             map_to_bq_dict(d, 'OUT_OF_FAVOUR_DEFENSIVE'))
             | 'Writing to sink ap2' >> bq_sink)

            (benchmark_data | 'Filtering for all fields ENT2 ' >> beam.Filter(benchmark_filter)
             | 'Filtering for ent ap2' >> beam.Filter(enterprise_stock_filter)
             | 'Filtering for out of favour enterprise2' >> beam.Filter(out_of_favour_filter)
             | 'Mapping only Relevant fields ENT2' >> beam.Map(lambda d:
                                                              map_to_bq_dict(d, 'OUT_OF_FAVOUR_ENTERPRISE'))
             | 'Writing to sink ENT2' >> bq_sink)



        else:

            fundamental_data = load_fundamental_data(tickers, pipeline_options.fmprepkey)

            microcap_data = load_microcap_data(tickers, pipeline_options.fmprepkey)



            destination = 'gs://mm_dataflow_bucket/outputs/superperformers_stockuniverse_{}'.format(
                                                    date.today().strftime('%Y-%m-%d %H:%M'))

            sink = beam.io.WriteToText(destination, num_shards=1)

            fundamental_data | 'Sendig to sink' >> sink

            (fundamental_data | 'fund unvierse' >> beam.Filter(get_universe_filter)
                            |'Mapping only Relevant fields' >> beam.Map(lambda d:
                                                                             map_to_bq_dict(d, 'STOCK_UNIVERSE'))
                             | 'Writing to stock selection' >> bq_sink)

            (fundamental_data | 'Canslimm filter' >> beam.Filter(canslim_filter)
                              |'Mapping only Relevant canslim fields' >> beam.Map(lambda d:
                                                                                  map_to_bq_dict(d, 'CANSLIM'))
                                | 'Writing to stock selection C' >> bq_sink)

            (fundamental_data | 'stock under 10m filter' >> beam.Filter(stocks_under_10m_filter)
             | 'Mapping only Relevant xm fields' >> beam.Map(lambda d: map_to_bq_dict(d, 'UNDER10M'))
             | 'Writing to stock selection 10' >> bq_sink)

            (fundamental_data | 'stock NEW HIGHGS' >> beam.Filter(new_high_filter)
             | 'Mapping only Relevant nh fields' >> beam.Map(lambda d:
                                                             map_to_bq_dict(d, 'NEWHIGHS'))
             | 'Writing to stock selection nh' >> bq_sink)

            (fundamental_data | 'asset universe filter' >> beam.Filter(get_universe_filter)
             | 'Asset PLays' >> beam.Filter(asset_play_filter)
             | 'Universe filter' >> beam.Filter(get_universe_filter)
             | 'Mapping only Relevant ASSET play fields' >> beam.Map(lambda d:
                                                                     map_to_bq_dict(d, 'ASSET_PLAY_WEEKLY'))
             | 'Writing to stock selection ap' >> bq_sink)

            (fundamental_data | 'Filtering for all fields weekly ' >> beam.Filter(get_universe_filter)
             | 'Filtering for out of favour weekly' >> beam.Filter(out_of_favour_filter)
             | 'Mapping only Relevant fields weekly' >> beam.Map(lambda d:
                                                               map_to_bq_dict(d, 'OUT_OF_FAVOUR_WEEKLY'))
             | 'Writing to sink weekly' >> bq_sink)


            (microcap_data | 'Filtering microcap' >> beam.Filter(microcap_filter)
                            | 'Mapping only relevan microcap' >> beam.Map(lambda d:
                                                               map_to_bq_dict(d, 'MICROCAPY'))
                            | 'wRITING TO SINK microcap' >> bq_sink)




