### SuperPerformers Pipelines
import logging
import apache_beam as beam
from datetime import date
from shareloader.modules.finviz_utils import FinvizLoader
from shareloader.modules.obb_utils import AsyncProcess, create_bigquery_ppln, ProcessHistorical
from shareloader.modules.superperformers import combine_tickers
from shareloader.modules.finviz_utils import get_extra_watchlist, get_leaps, get_universe_stocks, get_canslim, get_buffett_six, \
                                                get_graham_enterprise, get_graham_defensive, get_new_highs
from datetime import datetime
from shareloader.modules.obb_processes import AsyncProcessFinvizTester
from shareloader.modules.sectors_utils import get_finviz_performance
from .superperf_metrics import get_all_data, get_fundamental_parameters, get_descriptive_and_technical,\
                                            get_financial_ratios, get_fundamental_parameters_qtr, get_analyst_estimates,\
                                            get_quote_benchmark, get_financial_ratios_benchmark, get_key_metrics_benchmark, \
                                            get_income_benchmark, get_balancesheet_benchmark, get_asset_play_parameters,\
                                            calculate_piotrosky_score, compute_rsi, get_price_change, get_dividend_paid,\
                                            get_peter_lynch_ratio
import itertools
from .mail_utils import NEW_STOCK_EMAIL_TEMPLATE, NEW_ROW_TEMPLATE
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization

import requests


def combine_data(input):
    return ','.join(input)



def update_dict_beam(element, label):
  """Updates a dictionary within a Beam pipeline element.

  Args:
    element: A dictionary.
    key: The key to update or add.
    value: The value to associate with the key.

  Returns:
    A new dictionary with the updated key-value pair.
  """
  return {**element, 'label': label}


def run_universe(p):
    return (p | 'Starting universe' >> beam.Create(get_universe_stocks())
            | 'adding Universe Label' >> beam.Map(lambda d: update_dict_beam(d,  'UNIVERSE'))
            )

def run_graham_defensive(p):
    return (p | 'Starting gd' >> beam.Create(get_graham_defensive())
            | 'adding gd Label' >> beam.Map(lambda d: update_dict_beam(d,  'DEFENSIVE'))
            )

def run_graham_enterprise(p):
    return (p | 'Starting ge' >> beam.Create(get_graham_enterprise())
            | 'adding ge' >> beam.Map(lambda d: update_dict_beam(d,  'ENTEPRISE'))
            )
def run_leaps(p):
    return (p | 'Starting leaps' >> beam.Create(get_leaps())
            | 'adding leaps' >> beam.Map(lambda d: update_dict_beam(d,  'LEAPS'))
            )

def run_canslim(p):
    return (p | 'Starting cs' >> beam.Create(get_canslim())
            | 'adding cs' >> beam.Map(lambda d: update_dict_beam(d,  'CANSLIM'))
            )

def run_newhighs(p):
    return (p | 'Starting ns' >> beam.Create(get_new_highs())
            | 'adding nh' >> beam.Map(lambda d: update_dict_beam(d,  'NEWHIGHS'))
            )

def run_buffetsix(p):
    return (p | 'Starting bs' >> beam.Create(get_buffett_six())
            | 'adding bs' >> beam.Map(lambda d: update_dict_beam(d,  'BUFFET_SIX'))
            )

def run_extrawl(p):
    return (p | 'Starting ewl' >> beam.Create(get_buffett_six())
            | 'adding ewl' >> beam.Map(lambda d: update_dict_beam(d,  'WATCHLIST'))
            )

'''

We  need to combine and see differences betweeen the loaders so that we all retrieve the same data

- Fundamental Loader calls
    - get_fundamental_parameters
    - get_financial_ratios
    - get_analyst_estimates
    - get_descriptive_and_technical
    - get_asset_play_parameter
    - calculate_piotrosky_score
    - compute_rsi
    - get_key_metrics_benchmark
    - get_peter_lynch_ratio

- MicroCap Loader
    - get_descriptive_and_technical F
    - get_price_change (priceChangeDict.get('52weekChange', 0)
    - get_fundamental_parameter  F
    - get_financial_ratios       F
    - get_dividend_paid
    

- Benchmark Loader
    - get_quote_benchmark (to retrieve institutional ownership. can be replaced by finviz)
    - get_income_benchmark - should be same as get_fundamental_params
    - get_balancesheet_benchmark
    - get_financial_ratios_benchmark F
    - get_key_metrics_benchmark   F
    - get_asset_play_parameters  F
    - get_peter_lynch_ratio      F
        


def load_fundamental_data(source,fmpkey, split=''):
    # fundamental works for everything minus microcap, 
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
'''


def combine_fund1(p):
    extrawl = run_extrawl(p)
    buffetsix = run_buffetsix(p)
    universe = run_universe(p)
    # this will go to standard
    return ((extrawl, buffetsix, universe)
                | 'FlattenCombine all f1' >> beam.Flatten()
            )


def combine_fund2(p):
    newhighs = run_newhighs(p)
    canslim = run_canslim(p)
    leaps = run_leaps(p)
    # this will go to stndard
    return ((newhighs, canslim, leaps)
                | 'FlattenCombine all f2' >> beam.Flatten()
            )


def combine_benchmarks(p):
    ge = run_graham_enterprise(p)
    gd = run_graham_defensive(p)
    # this will go all to benchmark
    return (
            (ge, gd)
            | 'FlattenCombine all' >> beam.Flatten()

    )

def run_standard_pipeline1(p, fmpKey):
    combined  = combine_fund1(p)
    return (combined
            | 'Getting fundamentals' >> beam.ParDo(EnhancedFundamentalLoader(fmpKey))
            )




class EnhancedFundamentalLoader(beam.DoFn):
    def __init__(self, key, microcap_flag=False):
        self.key = key
        self.microcap_flag = microcap_flag
        
    def process(self, elements):
        all_dt = []
        isException = False
        excMsg = ''
        for tickerDict in elements:
            ticker = tickerDict['ticker']
            company_dict = tickerDict.copy()
            try:
                fundamental_data = get_fundamental_parameters(ticker, self.key)
                if fundamental_data:
                    fundamental_data.update(company_dict)
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
                
        if isException:
            raise Exception(excMsg)
        return all_dt


class EnhancedBenchmarkLoader(beam.DoFn):
    def __init__(self, key, split=None):
        self.key = key
        self.split = split

    def process(self, elements):
        logging.info('All data is\n{}'.format(elements))
        all_dt = []
        isException = False
        excMsg = ''

        idx = 0
        for tickerDict in elements:
            ticker = tickerDict['ticker']
            company_dict = tickerDict.copy()
            idx +=1
            try:
                quotes_data = get_quote_benchmark(ticker, self.key)
                quotes_data.update(company_dict)
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

                                    quotes_data['rsi'] = 0
                                    quotes_data['piotroskyScore'] = 0
                                    quotes_data['lynchRatio'] = get_peter_lynch_ratio(self.key, ticker, quotes_data)

                                all_dt.append(quotes_data)
            except Exception as e:
                logging.info(f'Unexpected exception in fundamental data for:{ticker}:{str(e)}')
                excMsg = f'{idx/len(tickers)}Unexpected exception in fundamental data for:{ticker}:{str(e)}'
                isException = True
                break

        if isException:
            raise Exception(excMsg)
        return all_dt


class PipelineCombinerFn(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, accumulator, input):
        accumulator.append(input)
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = []
        for acc in accumulators:
            merged.extend(acc)
        return merged

    def extract_output(self, accumulator):
        return accumulator


class StockSelectionCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    row_acc = accumulator
    row_acc.append(NEW_ROW_TEMPLATE.format(*[input['ticker'], input['label'], input['price']]))
    return row_acc

  def merge_accumulators(self, accumulators):
    return list(itertools.chain(*accumulators))

  def extract_output(self, sum_count):
    return ''.join(sum_count)

class EmailSender(beam.DoFn):
    def __init__(self, recipients, key):
        self.recipients = recipients.split(';')
        self.key = key


    def _build_personalization(self, recipients):
        personalizations = []
        for recipient in recipients:
            logging.info('Adding personalization for {}'.format(recipient))
            person1 = Personalization()
            person1.add_to(Email(recipient))
            personalizations.append(person1)
        return personalizations


    def process(self, element):
        logging.info('Attepmting to send emamil to:{}, using key:{}'.format(self.recipients, self.key))
        template = NEW_STOCK_EMAIL_TEMPLATE
        asOfDateStr = date.today().strftime('%d %b %Y')
        content = template.format(asOfDate=asOfDateStr, tableOfData=element)
        sender = 'gcp_cloud_mm@outlook.com'
        logging.info(f'Sending mail from:{sender} ')
        logging.info('Sending \n {}'.format(content))
        message = Mail(
            from_email=sender,
            to_emails=self.recipients,
            subject=f'New Stock selection ideas for {asOfDateStr}',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        print(response.status_code, response.body, response.headers)

def send_email(pipeline, sendgridkey):
    return (pipeline | 'SendEmail' >> beam.ParDo(EmailSender('mmistron@gmail.com', sendgridkey))
             )





