import requests
import logging
from datetime import datetime
## TODO: identify how to store the data. mayb we just return  a list of dicts and let the sink do the stuff

def calculate_peter_lynch_ratio(key, ticker, asOfDateStr, dataDict):
    divYield = dataDict['dividendYield'] * 100
    peRatio = dataDict['priceEarningsRatio']

    cob = datetime.strptime(asOfDateStr, '%Y-%m-%d')

    try:
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

def get_financial_ratios(ticker, key, period='annual'):
    keys = ["currentRatio", "quickRatio", "cashRatio", "date", "calendarYear",
            "returnOnAssets", "returnOnEquity", "returnOnCapitalEmployed",
            "priceToBookRatio", "priceToSalesRatio",
            "priceEarningsRatio", "priceToFreeCashFlowsRatio",
            "priceEarningsToGrowthRatio", "dividendYield", "priceFairValue"
            ]
    global_dict = dict()
    try:
        financial_ratios = requests.get(
            f'https://financialmodelingprep.com/api/v3/ratios/{ticker}?period={period}&limit=5&apikey={key}').json()

        for data_dict in financial_ratios:
            tmp_dict = dict((k, data_dict.get(k, None)) for k in keys)
            global_dict[data_dict['date']] = tmp_dict
    except Exception as e:
        logging.info(f'Unable to get data for {ticker}:{str(e)}')
    return global_dict

def get_key_metrics(ticker, key, period='annual'):
    keys = [
            "date", "calendarYear", "revenuePerShare", "earningsYield", "debtToEquity",
            "debtToAssets", "capexToRevenue", "grahamNumber"
    ]

    globalDict = dict()

    try:
        keyMetrics = requests.get(
           f'https://financialmodelingprep.com/api/v3/key-metrics/{ticker}?period={period}&limit=5&apikey={key}').json()
        for dataDict in keyMetrics:
            tmpDict = dict((k, dataDict.get(k, None)) for k in keys)
            globalDict[dataDict['date']] = tmpDict

    except Exception as e:
        logging.info(f'Unable to get data for {ticker}:{str(e)}')

    return globalDict

def get_fundamental_data(ticker, key, period='annual'): # we do separately , for each ticker we get perhaps the last
    ## we get ROC, divYield and pl ratio. we need to get historicals on a quarterly and annually
    # then we build a dataframe with all the information
    metrics = get_key_metrics(ticker, key, period)
    ratios = get_financial_ratios(ticker, key, period)

    mergedDicts = []
    for asOfDate, metricsDict in metrics.items():
        ratiosDict = ratios.get(asOfDate)
        if ratiosDict:
            ratiosDict.update(metricsDict)
            peterLynch = calculate_peter_lynch_ratio(key, ticker, asOfDate, ratiosDict)
            ratiosDict['lynchRatio'] = peterLynch
            mergedDicts.append(ratiosDict)
    return mergedDicts

def get_tickers_for_sectors(sector, key):
    ''' Sectors
    Consumer Cyclical, Energy, Technology, Industrials, Financial Services,
    Basic Materials, Communication Services, Consumer Defensive,
    Healthcare, Real Estate, Utilities, Industrial Goods, Financial, Services, Conglomerates

    Industry




    '''



    url = 'https://financialmodelingprep.com/api/v3/stock-screener?sector={sector}&apikey={key}'
    return requests.get(url).json()

