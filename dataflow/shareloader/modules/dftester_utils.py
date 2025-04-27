import requests
import logging
from datetime import datetime
import apache_beam as beam
from openai import OpenAI






def get_fields():
    return ["ticker", "date", "currentRatio", "quickRatio", "cashRatio", "calendarYear",
            "returnOnAssets", "returnOnEquity", "returnOnCapitalEmployed",
            "priceToBookRatio", "priceToSalesRatio",
            "priceEarningsRatio", "priceToFreeCashFlowsRatio",
            "priceEarningsToGrowthRatio", "dividendYield", "priceFairValue"
            ] + \
            [
                "revenuePerShare", "earningsYield", "debtToEquity",
                "debtToAssets", "capexToRevenue", "grahamNumber",
                "lynchRatio"
            ]


class DfTesterLoader(beam.DoFn):
    def __init__(self, key, period='annual', limit=10):
        self.key = key
        self.period = period
        self.limit = limit

    def to_list_of_vals(self, data_dict):
        return ','.join([str(data_dict[field]) for field in get_fields()])

    def process(self, elements):
        all_dt = []
        logging.info('fRunning with split of:{split}')
        tickers_to_process = elements.split(',')
        num_to_process = len(tickers_to_process) // 3

        excMsg = ''
        isException = False

        all_dt = []

        for idx, ticker in enumerate(tickers_to_process):
            try:
                data  =get_fundamental_data(ticker, self.key, self.period, self.limit)
                vals_only = [self.to_list_of_vals(d) for d in data]
                all_dt += vals_only
            except Exception as e:
                excMsg = f"{idx/len(tickers_to_process)}Failed to process fundamental loader for {ticker}:{str(e)}"
                isException = True
                break
        if isException:
            raise Exception(excMsg)
        return all_dt


def combine_tickers(input):
    return ','.join(input)

def calculate_peter_lynch_ratio(key, ticker, asOfDateStr, dataDict):
    if dataDict['dividendYield']  is None or  dataDict['priceEarningsRatio'] is None:
        return -99
    try:
        divYield = dataDict['dividendYield'] * 100
        peRatio = dataDict['priceEarningsRatio']
        cob = datetime.strptime(asOfDateStr, '%Y-%m-%d')
        baseUrl = f'https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker}?apikey={key}'
        all_estimates = requests.get(baseUrl).json()
        estimatesList = []
        for estimate in all_estimates:
            estimatesList.append( ( datetime.strptime(estimate['date'], '%Y-%m-%d'), estimate['estimatedEpsAvg']) )
        valid = [tpl for tpl  in estimatesList if tpl[0] > cob]

        if valid:
            epsGrowth = float(valid[0][1])
            return (divYield + epsGrowth) / peRatio
        return -99
    except Exception as e:
        return -99

def get_financial_ratios(ticker, key, period='annual', limit=10):
    keys = ["currentRatio", "quickRatio", "cashRatio", "date", "calendarYear",
            "returnOnAssets", "returnOnEquity", "returnOnCapitalEmployed",
            "priceToBookRatio", "priceToSalesRatio",
            "priceEarningsRatio", "priceToFreeCashFlowsRatio",
            "priceEarningsToGrowthRatio", "dividendYield", "priceFairValue"
            ]
    global_dict = dict()
    try:
        financial_ratios = requests.get(
            f'https://financialmodelingprep.com/api/v3/ratios/{ticker}?period={period}&limit={limit}&apikey={key}').json()

        for data_dict in financial_ratios:
            tmp_dict = dict((k, data_dict.get(k, None)) for k in keys)
            global_dict[data_dict['date']] = tmp_dict
    except Exception as e:
        logging.info(f'Unable to get data for {ticker}:{str(e)}')
    return global_dict

def get_key_metrics(ticker, key, period='annual', limit=10):
    keys = [
            "date", "calendarYear", "revenuePerShare", "earningsYield", "debtToEquity",
            "debtToAssets", "capexToRevenue", "grahamNumber"
    ]

    globalDict = dict()

    try:
        keyMetrics = requests.get(
           f'https://financialmodelingprep.com/api/v3/key-metrics/{ticker}?period={period}&limit={limit}&apikey={key}').json()
        for dataDict in keyMetrics:
            tmpDict = dict((k, dataDict.get(k, None)) for k in keys)
            globalDict[dataDict['date']] = tmpDict

    except Exception as e:
        logging.info(f'Unable to get data for {ticker}:{str(e)}')

    return globalDict

def get_fundamental_data(ticker, key, period='annual', limit=10): # we do separately , for each ticker we get perhaps the last
    ## we get ROC, divYield and pl ratio. we need to get historicals on a quarterly and annually
    # then we build a dataframe with all the information
    metrics = get_key_metrics(ticker, key, period, limit)
    ratios = get_financial_ratios(ticker, key, period, limit)

    mergedDicts = []
    for asOfDate, metricsDict in metrics.items():
        ratiosDict = ratios.get(asOfDate)
        if ratiosDict:
            ratiosDict.update(metricsDict)
            peterLynch = calculate_peter_lynch_ratio(key, ticker, asOfDate, ratiosDict)
            ratiosDict['lynchRatio'] = peterLynch
            ratiosDict['ticker'] = ticker
            mergedDicts.append(ratiosDict)
    return mergedDicts

def get_tickers_for_sectors(sector, key):
    ''' Sectors
    Consumer Cyclical, Energy, Technology, Industrials, Financial Services,
    Basic Materials, Communication Services, Consumer Defensive,
    Healthcare, Real Estate, Utilities, Industrial Goods, Financial, Services, Conglomerates

    Industry
    '''

    url = f'https://financialmodelingprep.com/api/v3/stock-screener?sector={sector}&apikey={key}'
    return requests.get(url).json()

def get_tickers_for_industry(industry, key):
    ''' Sectors
    Consumer Cyclical, Energy, Technology, Industrials, Financial Services,
    Basic Materials, Communication Services, Consumer Defensive,
    Healthcare, Real Estate, Utilities, Industrial Goods, Financial, Services, Conglomerates

    Industry




    '''

    url = f'https://financialmodelingprep.com/api/v3/stock-screener?industry={industry}&apikey={key}'
    return requests.get(url).json()


def get_sectors():
    return ['Consumer Cyclical', 'Energy', 'Technology', 'Industrials',
            'Financial Services', 'Basic Materials', 'Communication Services',
            'Consumer Defensive', 'Healthcare', 'Real Estate', 'Utilities',
            'Industrial Goods', 'Financial', 'Services', 'Conglomerates']


def get_industries(key):
    inds = []
    for sector in get_sectors():
        companies = get_tickers_for_sectors(sector, key)
        sector_inds = [d['industry'] for d in companies]
        inds += sector_inds

    return set(inds)


