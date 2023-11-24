import requests
import logging
from datetime import datetime

def calculate_peter_lynch_ratio(key, ticker, asOfDateStr, dataDict):
    divYield = dataDict['dividendYield'] * 100
    peRatio = dataDict['peRatio']

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
            "returnOnAssets", "returnOnEquity", "returnOnCapitalEmployed"
                                                "priceToBookRatio", "priceToSalesRatio",
            "priceEarningsRatio", "priceToFreeCashFlowsRatio",
            "priceEarningsToGrowthRatio", "dividendYield", "priceFairValue"
            ]
    globalDict = dict()
    try:
        financial_ratios = requests.get(
            f'https://financialmodelingprep.com/api/v3/ratios/{ticker}?period={period}&limit=5&apikey={key}').json()

        for dataDict in financial_ratios:
            tmpDict = dict((k, dataDict.get(key, None)) for k in keys)
            globalDict[dataDict['date']] = tmpDict
    except Exception as e:
        logging.info(f'Unable to get data for {ticker}:{str(e)}')
    return globalDict

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
            tmpDict = dict((k, dataDict.get(key, None)) for k in keys)
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
    for asOfDate, dataDict in metrics.items():
        ratioDict = ratios.get(asOfDate)
        if ratioDict:
            dataDict.merge(ratioDict)
            peterLynch = calculate_peter_lynch_ratio(key, ticker, asOfDate, ratios)
            dataDict['lynchRatio'] = peterLynch
            mergedDicts.append(dataDict)
    return mergedDicts
