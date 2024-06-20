from __future__ import absolute_import

from apache_beam.options.pipeline_options import SetupOptions, DebugOptions
from datetime import datetime, date
import logging
import apache_beam as beam
import pandas as pd
from datetime import date
from apache_beam.options.pipeline_options import PipelineOptions
from .superperf_metrics import get_all_data, get_fundamental_parameters, get_descriptive_and_technical,\
                                            get_financial_ratios, get_fundamental_parameters_qtr, get_analyst_estimates,\
                                            get_quote_benchmark, get_financial_ratios_benchmark, get_key_metrics_benchmark, \
                                            get_income_benchmark, get_balancesheet_benchmark, get_asset_play_parameters,\
                                            calculate_piotrosky_score, compute_rsi, get_price_change, get_dividend_paid,\
                                            get_peter_lynch_ratio
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
        parser.add_argument('--microcap')
        parser.add_argument('--probe')
        parser.add_argument('--split')

def asset_play_filter(input_dict):
    if not input_dict:
        return False
    shares_outstanding = input_dict.get('sharesOutstanding', 0)
    marketCap = input_dict.get('marketCap', 0)
    bookValuePerShare = input_dict.get('bookValuePerShare', 0)
    assetValue = shares_outstanding * bookValuePerShare

    return assetValue > marketCap

def canslim_filter(input_dict):
    return (input_dict.get('avgVolume',0) > 200000) and (input_dict.get('eps_growth_this_year',0) > 0.2)\
         and (input_dict.get('eps_growth_next_year', 0) > 0.2) \
    and (input_dict.get('eps_growth_qtr_over_qtr', 0) > 0.2) and (input_dict.get('net_sales_qtr_over_qtr',0) > 0.2) \
    and (input_dict.get('eps_growth_past_5yrs',0) > 0.2) and (input_dict.get('returnOnEquity',0) > 0) \
    and (input_dict.get('grossProfitMargin', 0) > 0) and (input_dict.get('institutionalOwnershipPercentage', 0) > 0.3) \
    and (input_dict.get('price',0) > input_dict.get('priceAvg20', 0)) and (input_dict.get('price',0) > input_dict.get('priceAvg50',0)) \
    and (input_dict.get('price', 0) > input_dict.get('priceAvg200',0)) and (input_dict.get('sharesOutstanding',0) > 50000000)

def canslim_potential_filter(input_dict):
    return (input_dict.get('avgVolume',0) > 200000) and (input_dict.get('eps_growth_this_year',0) > 0.2)\
         and (input_dict.get('eps_growth_next_year', 0) > 0.2) \
    and (input_dict.get('eps_growth_qtr_over_qtr', 0) > 0.2) and (input_dict.get('net_sales_qtr_over_qtr',0) > 0.2) \
    and (input_dict.get('eps_growth_past_5yrs',0) > 0.2) and (input_dict.get('returnOnEquity',0) > 0) \
    and (input_dict.get('grossProfitMargin', 0) > 0) and (input_dict.get('institutionalOwnershipPercentage', 0) > 0.3) \
    and (input_dict.get('sharesOutstanding',0) > 50000000)


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
    return (input_dict['marketCap'] <= 200000000) and (input_dict.get('numOfDividendsPaid', 0) == 0) and \
           (input_dict.get('grossProfitMargin', 0) >= 0.5) and (input_dict['netProfitMargin'] >= 0.1)  and \
           (input_dict.get('currentRatio') >= 2) and (input_dict['avgVolume'] >= 10000) and \
           (input_dict.get('52weekChange') > 0) and  (input_dict.get('52weekChange') < 0.5)




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

class MicrocapLoader(beam.DoFn):
    def __init__(self, key, microcap_flag=True, split=''):
        self.key = key
        self.microcap_flag = microcap_flag
        self.split = split


    def process(self, elements):
        all_dt = []
        logging.info('fRunning with split of:{split}')
        tickers_to_process = elements.split(',')
        num_to_process = len(tickers_to_process) // 3

        if 'first' in self.split.lower():
            tickers_to_process = tickers_to_process[0:num_to_process]
        elif 'second' in self.split.lower():
            tickers_to_process = tickers_to_process[num_to_process:num_to_process * 2]
        else:
            tickers_to_process = tickers_to_process[-num_to_process:]

        logging.info('Ticker to process:{len(tickers_to_process}')

        excMsg = ''
        isException = False
        for idx, ticker in enumerate(tickers_to_process):
            # Not good. filter out data at the beginning to reduce stress load for rest of data
            try:
                descr_and_tech = get_descriptive_and_technical(ticker, self.key)
                if descr_and_tech.get('marketCap', 0) is not None \
                        and descr_and_tech.get('marketCap', 0) <= 200000000 \
                        and descr_and_tech.get('avgVolume', 0) is not None \
                        and descr_and_tech.get('avgVolume', 0) >= 10000  \
                        and descr_and_tech.get('price', 9) > 10:
                    logging.info(f'Checks proceed for {ticker}')
                    # checking pct change
                    priceChangeDict = get_price_change(ticker, self.key)
                    if priceChangeDict:
                        chng = priceChangeDict.get('52weekChange', 0)
                        if chng > 0 and chng < .5:
                            descr_and_tech.update(priceChangeDict)
                            logging.info(f'Change check green for {ticker}')
                            fundamental_data = get_fundamental_parameters(ticker, self.key)
                            if fundamental_data:
                                descr_and_tech.update(fundamental_data)
                                financial_ratios = get_financial_ratios(ticker, self.key)
                                if financial_ratios:
                                    descr_and_tech.update(financial_ratios)
                                    logging.info('Calculating divis..')
                                    dividendDict = get_dividend_paid(ticker, self.key)
                                    descr_and_tech.update(dividendDict)
                                    all_dt.append(descr_and_tech)
            except Exception as e:
                excMsg = f"{idx/len(tickers_to_process)}Failed to process fundamental loader for {ticker}:{str(e)}"
                isException = True
                break
        if isException:
            raise Exception(excMsg)
        return all_dt


class FundamentalLoader(beam.DoFn):
    def __init__(self, key, microcap_flag=False, split_flag=None):
        self.key = key
        self.microcap_flag = microcap_flag
        self.split = split_flag

    def process(self, elements):
        all_dt = []
        isException = False
        excMsg = ''
        tickers_to_process = elements.split(',')
        if self.split:
            num_to_process = len(tickers_to_process) // 4
            if 'first' in self.split.lower():
                tickers_to_process = tickers_to_process[0:num_to_process]
            elif 'second' in self.split.lower():
                tickers_to_process = tickers_to_process[num_to_process:num_to_process * 2]
            elif 'third' in self.split.lower():
                tickers_to_process = tickers_to_process[num_to_process * 2:num_to_process * 3]
            else:
                tickers_to_process = tickers_to_process[-num_to_process:]
        for idx, ticker in enumerate(tickers_to_process):

            try:
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
                    logging.info(f'Getting Key Metrics Benchmark for {ticker}')
                    keyMetrics = get_key_metrics_benchmark(ticker, self.key)
                    updated_dict.update(keyMetrics)
                    logging.info(f'Getting lynchratio for {ticker}')
                    updated_dict['lynchRatio'] = get_peter_lynch_ratio(self.key, ticker, updated_dict)
                    all_dt.append(updated_dict)
            except Exception as e:
                logging.info(f"Failed to process fundamental loader for {ticker}:{str(e)}")
                isException = True
                excMsg = f"{idx/len(tickers_to_process)}Failed to process fundamental loader for {ticker}:{str(e)}"
                break
        if isException:
            raise Exception(excMsg)
        return all_dt

class BenchmarkLoader(beam.DoFn):
    def __init__(self, key, split=None):
        self.key = key
        self.split = split

    def process(self, elements):
        logging.info('All data is\n{}'.format(elements))
        all_dt = []
        tickers = elements.split(',')

        if self.split:
            if 'first' in self.split.lower():
                tickers = tickers[0: len(tickers) //2]
            else:
                tickers = tickers[len(tickers) //2:]

        logging.info('Attepmting to get fundamental data for all elements {}'.format(len(tickers)))

        isException = False
        excMsg = ''


        exceptionCount = 0
        for idx, ticker in enumerate(tickers):
            try:
                quotes_data = get_quote_benchmark(ticker, self.key)
                if quotes_data:
                    if quotes_data['institutionalOwnershipPercentage'] > 0.6 or \
                        quotes_data['institutionalOwnershipPercentage'] ==  0.55555:
                        continue
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
                                    if quotes_data.get('sharesOutstanding',1) is not None \
                                        and quotes_data.get('price',1) is not None:
                                        currentCompanyValue = quotes_data['sharesOutstanding'] * quotes_data['price']
                                    else:
                                        currentCompanyValue = 1
                                    # current assets
                                    quotes_data['canBuyAllItsStock'] = quotes_data['totalAssets'] - currentCompanyValue

                                    if quotes_data.get('totalCurrentAssets') is not None and \
                                            quotes_data.get('totalCurrentLiabilities') is not None and \
                                            quotes_data.get('inventory') is not None and \
                                            quotes_data.get('sharesOutstanding') is not None and \
                                            quotes_data.get('sharesOutstanding') > 0:
                                        quotes_data['netQuickAssetPerShare'] = (quotes_data['totalCurrentAssets'] -  \
                                                                                    quotes_data['totalCurrentLiabilities'] - \
                                                                                     quotes_data['inventory']) / quotes_data['sharesOutstanding']
                                    else:
                                        quotes_data['netQuickAssetPerShare'] = -1

                                    #piotrosky_score = calculate_piotrosky_score(self.key, ticker)
                                    #latest_rsi = compute_rsi(ticker, self.key)
                                    quotes_data['rsi'] = 0
                                    quotes_data['piotroskyScore'] = 0
                                    quotes_data['lynchRatio'] = get_peter_lynch_ratio(self.key, ticker, quotes_data)

                                #priceChangeDict = get_price_change(ticker, self.key)
                                #quotes_data.update(priceChangeDict)
                                all_dt.append(quotes_data)
            except Exception as e:
                logging.info(f'Unexpected exception in fundamental data for:{ticker}:{str(e)}')
                excMsg = f'{idx/len(tickers)}Unexpected exception in fundamental data for:{ticker}:{str(e)}'
                isException = True
                break

        if isException:
            raise Exception(excMsg)
        return all_dt

def write_to_bucket(lines, sink):
    return (
            lines | 'Writing to bucket' >> sink
    )

def combine_tickers(input):
    return ','.join(input)

def combine_dict(input):
    return [d for d in input]

def load_fundamental_data(source,fmpkey, split=''):
    return (source
            | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(FundamentalLoader(fmpkey, split_flag=split))
            | 'Filtering out none fundamentals' >> beam.Filter(lambda item: item is not None)
            | 'filtering on descr and technical' >> beam.Filter(get_descriptive_and_techincal_filter)
            | 'Using fundamental filters' >> beam.Filter(get_fundamental_filter)
            )
def load_microcap_data(source,fmpkey):
    return (source
            | 'Combine all at fundamentals microcap' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals microcap' >> beam.ParDo(MicrocapLoader(fmpkey, microcap_flag=True))
            | 'Filtering out none fundamentals microcap' >> beam.Filter(lambda item: item is not None)
            | 'MicroCap Sanity Check' >> beam.Filter(microcap_sanity_check)
            | 'Filtering microcap' >> beam.Filter(microcap_filter)
            )

def load_benchmark_data(source,fmpkey, split=None):
    return (source
            | 'Combine all at fundamentals bench' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals bench' >> beam.ParDo(BenchmarkLoader(fmpkey, split))
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


def microcap_sanity_check(input_dict):
    fields_to_check = ['marketCap', 'numOfDividendsPaid', 'grossProfitMargin',
                       'netProfitMargin', 'currentRatio', 'avgVolume',
                       '52weekChange']
    result = [input_dict.get(field) is not None for field in fields_to_check]
    return all(result)


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
                DIVIDENDRATIO=input_dict.get('dividendPayoutRatio', 0.0),
                NUM_OF_DIVIDENDS=input_dict.get('numOfDividendsPaid', 0.0),
                PERATIO=input_dict.get('pe', 0.0),
                INCOME_STMNT_DATE=input_dict['income_statement_date'],
                INCOME_STMNT_DATE_QTR=input_dict.get('income_statement_qtr_date'),
                RSI=input_dict.get('rsi',-1),
                PIOTROSKY_SCORE=input_dict.get('piotroskyScore', -1),
                RETURN_ON_CAPITAL=input_dict.get('returnOnCapital', -1),
                NET_INCOME=input_dict.get('netIncome', -1),
                LYNCH_RATIO=input_dict.get('lynchRatio', -1)

                )

def run_probe(fund_data):
    logging.info('Running probe..,')
    logging.info('Returning')
    destination = 'gs://mm_dataflow_bucket/outputs/superperformers_probe_{}'.format(
        date.today().strftime('%Y-%m-%d %H:%M'))
    sink = beam.io.WriteToText(destination, num_shards=1)
    (fund_data | 'Writing to text sink' >> sink)

def store_microcap(data, sink):
    (data
     | 'Mapping only relevan microcap' >> beam.Map(lambda d:
                                                   map_to_bq_dict(d, 'MICROCAP'))
     | 'wRITING TO SINK microcap' >> sink)

def store_superperformers(data, bq_sink, test_sink):
    #data | 'Sendig to sink' >> test_sink

    (data | 'fund unvierse' >> beam.Filter(get_universe_filter)
     | 'Mapping only Relevant fields' >> beam.Map(lambda d:
                                                  map_to_bq_dict(d, 'STOCK_UNIVERSE'))
     | 'Writing to stock selection' >> bq_sink)

    (data | 'Canslimm filter' >> beam.Filter(canslim_filter)
     | 'Mapping only Relevant canslim fields' >> beam.Map(lambda d:
                                                          map_to_bq_dict(d, 'CANSLIM'))
     | 'Writing to stock selection C' >> bq_sink)

    (data | 'stock under 10m filter' >> beam.Filter(stocks_under_10m_filter)
     | 'Mapping only Relevant xm fields' >> beam.Map(lambda d: map_to_bq_dict(d, 'UNDER10M'))
     | 'Writing to stock selection 10' >> bq_sink)

    (data | 'stock NEW HIGHGS' >> beam.Filter(new_high_filter)
     | 'Mapping only Relevant nh fields' >> beam.Map(lambda d:
                                                     map_to_bq_dict(d, 'NEWHIGHS'))
     | 'Writing to stock selection nh' >> bq_sink)

    (data | 'asset universe filter' >> beam.Filter(get_universe_filter)
     | 'Asset PLays' >> beam.Filter(asset_play_filter)
     | 'Universe filter' >> beam.Filter(get_universe_filter)
     | 'Mapping only Relevant ASSET play fields' >> beam.Map(lambda d:
                                                             map_to_bq_dict(d, 'ASSET_PLAY_WEEKLY'))
     | 'Writing to stock selection ap' >> bq_sink)

    (data | 'Filtering for all fields weekly ' >> beam.Filter(get_universe_filter)
     | 'Filtering for out of favour weekly' >> beam.Filter(out_of_favour_filter)
     | 'Mapping only Relevant fields weekly' >> beam.Map(lambda d:
                                                         map_to_bq_dict(d, 'OUT_OF_FAVOUR_WEEKLY'))
     | 'Writing to sink weekly' >> bq_sink)


def store_superperformers_benchmark(benchmark_data, bq_sink):
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
            schema='AS_OF_DATE:DATE,TICKER:STRING,LABEL:STRING,PRICE:FLOAT,YEARHIGH:FLOAT,YEARLOW:FLOAT,PRICEAVG50:FLOAT,PRICEAVG200:FLOAT,BOOKVALUEPERSHARE:FLOAT,TANGIBLEBOOKVALUEPERSHARE:FLOAT,CASHFLOWPERSHARE:FLOAT,MARKETCAP:FLOAT,ASSET_VALUE:FLOAT,EXCESS_MARKETCAP:FLOAT,DIVIDENDRATIO:FLOAT,PERATIO:FLOAT,INCOME_STMNT_DATE:STRING,INCOME_STMNT_DATE_QTR:STRING,RSI:FLOAT,PIOTROSKY_SCORE:FLOAT,NET_INCOME:FLOAT,RETURN_ON_CAPITAL:FLOAT,NUM_OF_DIVIDENDS:FLOAT,LYNCH_RATIO:FLOAT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    pipeline_options = XyzOptions()

    timeout_secs = 18400
    experiment_value = f"max_workflow_runtime_walltime_seconds={timeout_secs}"
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(DebugOptions).add_experiment(experiment_value)

    test_sink =beam.io.WriteToText(destination, num_shards=1)

    with beam.Pipeline(options=pipeline_options) as p:
        tickers = extract_data_pipeline(p, input_file)

        if (pipeline_options.iistocks):
            benchmark_data = load_benchmark_data(tickers, pipeline_options.fmprepkey,
                                                 pipeline_options.split)

            if (pipeline_options.probe):
                run_probe(benchmark_data)
                return
            else:
                store_superperformers_benchmark(benchmark_data, bq_sink)

        elif (pipeline_options.microcap):
            microcap_data = load_microcap_data(tickers, pipeline_options.fmprepkey)
            store_microcap(microcap_data, bq_sink)
        else:
            fundamental_data = load_fundamental_data(tickers, pipeline_options.fmprepkey,
                                                    pipeline_options.split)
            if (pipeline_options.probe):
                run_probe(fundamental_data)
                return
            else:
                store_superperformers(fundamental_data, bq_sink, test_sink)






