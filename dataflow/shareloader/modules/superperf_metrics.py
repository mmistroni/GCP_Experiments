from datetime import datetime, date
import requests
import logging
import statistics
import pandas as pd
from pandas.tseries.offsets import BDay
# Criteria #1
# ==== WATCH LIST ===
# Market Cap > 2bln (mid)
import statistics

### Add info in this https://medium.com/@oleg.kazanskyi/trading-analytics-with-financial-kpis-cb35fe5c020d
def get_historical_ttm(ticker, key, asOfDate):
    all_incomes = requests.get(
        'https://financialmodelingprep.com/api/v3/income-statement/{}?period=quarter&limit=40&apikey={}'.format(ticker,
                                                                                                                key)).json()
    all_bsheet = requests.get(
        'https://financialmodelingprep.com/api/v3/balance-sheet-statement/{}?period=quarter&limit=40&apikey={}'.format(
            ticker, key)).json()
    all_incomes = filter_historical(all_incomes, asOfDate)[0:4]
    all_bsheet = filter_historical(all_bsheet, asOfDate)[0:4]

    ttm_netIncome = sum(d['netIncome'] for d in all_incomes)
    ttm_stockholdersEquity = sum(d['totalStockholdersEquity'] for d in all_bsheet) / 4
    print(
        f'NetIncome:{ttm_netIncome}, stockholderEquity:{ttm_stockholdersEquity}, expectedROE:{ttm_netIncome / ttm_stockholdersEquity}')

    #fundamental_dict['financial_ratios_date'] = latest['date']
    # https://financialmodelingprep.com/developer/docs/formula
    # fundamental_dict['grossProfitMargin'] = latest.get('grossProfitMargin', 0)  #grossProfit/ revenue
    # fundamental_dict['returnOnEquity'] = latest.get('returnOnEquity', 0)          #Net Income / stockHOlderEquity
    # fundamental_dict['dividendPayoutRatio']= latest.get('dividendPayoutRatio', 0.0)
    # fundamental_dict['dividendYield']= latest.get('dividendYield', 0.0)    # (dividendPaid / shareNumber) / price
    # fundamental_dict['returnOnCapital'] = latest.get('returnOnCapitalEmployedTTM', 0) #ebit / (totalAsset - totalCurrentLiabilities)

def calculate_piotrosky_score(key, ticker):
    '''
    Profitability Criteria Include
       Positive net income (1 point)
       Positive return on assets (ROA) in the current year (1 point)
       Positive operating cash flow in the current year (1 point)
       Cash flow from operations being greater than net Income (quality of earnings) (1 point)
    Leverage, Liquidity, and Source of Funds Criteria Include:
       Lower amount of long term debt in the current period, compared to the previous year (decreased leverage) (1 point)
       Higher current ratio this year compared to the previous year (more liquidity) (1 point)
       No new shares were issued in the last year (lack of dilution) (1 point).
    Operating Efficiency Criteria Include:
       A higher gross margin compared to the previous year (1 point)
       A higher asset turnover ratio compared to the previous year (1 point)


    :param input_dict:
    :return:
    '''

    score = 0

    try:
        cashflow_statements = requests.get(
            'https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}?period=quarter&limit=5&apikey={key}'.format(
                ticker=ticker, key=key)).json()

        income_statements = requests.get(
            'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=5&apikey={key}'.format(
                ticker=ticker, key=key)).json()
        balance_sheets = requests.get(
            'https://financialmodelingprep.com/api/v3/balance-sheet-statement/{ticker}?period=quarter&limit=5&apikey={key}'.format(
                ticker=ticker, key=key)).json()
        ratios = requests.get(
            'https://financialmodelingprep.com/api/v3/ratios/{ticker}?period=quarter&limit=5&apikey={key}'.format(
                ticker='MSFT', key=key)).json()
        current_ratio_this_year = ratios[0]['currentRatio']
        current_ratio_last_year = ratios[4]['currentRatio']
        gross_margin_quarter = ratios[0]['grossProfitMargin']
        gross_margin_last_year_quarter = ratios[4]['grossProfitMargin']
        long_term_debt_this_year = balance_sheets[0]['longTermDebt']
        long_term_debt_last_year = balance_sheets[4]['longTermDebt']
        net_income = cashflow_statements[0]['netIncome']
        operating_cashflow = cashflow_statements[0]['operatingCashFlow']
        outstanding_shares_this_year = income_statements[0]['weightedAverageShsOutDil']
        outstanding_shares_last_year = income_statements[4]['weightedAverageShsOutDil']
        asset_turnover_ttm = ratios[0]['assetTurnover']
        asset_turnover_last_year = ratios[4]['assetTurnover']
        roa = ratios[0]['returnOnAssets']

        if net_income > 0:
            score += 1
        if operating_cashflow > 0:
            score += 1
        if operating_cashflow > net_income:
            score += 1
        if roa > 0:
            score += 1

        if long_term_debt_this_year < long_term_debt_last_year:
            score += 1
        ###
        if current_ratio_this_year > current_ratio_last_year:
            score += 1
        ### shares outstanding
        if outstanding_shares_this_year < outstanding_shares_last_year:
            score += 1
        ##A higher gross margin compared to the previous year (1 point)
        ##A higher asset turnover ratio compared to the previous year (1 point)

        # assturnvr

        if asset_turnover_ttm > asset_turnover_last_year:
            score += 1

        if gross_margin_quarter > gross_margin_last_year_quarter:
            score += 1

    except Exception as e:
        logging.debug('Exception in calculating piotroski score:' + str(e))

    return score

def evaluate_progression(input):
    if len(input) < 2:
        return False
    start = input[0:-1]
    end = input[1:]
    zipped = zip(start, end)
    res = [(tpl[1] > tpl[0]) for tpl in zipped]
    return all(res)


def get_fmprep_historical(ticker, key, numdays=20, colname='adjClose'):
    hist_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/{}?apikey={}'.format(ticker, key)
    data = requests.get(hist_url).json().get('historical')
    if data:
        if colname:
            return [d[colname] for d in data[:numdays]]
        else:
            return [d for d in data[:numdays]]
    return [100000] * numdays


def get_common_shares_outstanding(ticker, key):
    res2 = requests.get(
        'https://financialmodelingprep.com/api/v3/balance-sheet-statement-as-reported/{}?limit=10&apikey={}'.format(
            ticker, key)).json()
    return [(d['date'], d['commonstocksharesoutstanding']) for d in res2]


def get_latest_stock_news(ticker, key):
    stock_news = []
    try:
        all_data = requests.get(f'https://financialmodelingprep.com/api/v3/stock_news?tickers={ticker}&limit=50&apikey={key}').json()
        yesterday = (date.today() - BDay(2)).date()
        stock_news = [news for news in all_data if news['publishedDate'] is not None and \
                    datetime.strptime(news['publishedDate'], '%Y-%m-%d %H:%M:%S').date() > yesterday]

    except Exception as e:
        logging.info(f'Exception in finding news for {ticker}:{str(e)}')
        stock_news =  []
    return {f"NumberOfNewsSince{yesterday.strftime('%Y%m%d')}" : len(stock_news)}

def get_mm_trend_template(ticker, key, numdays=55*5):
    #https: // medium.datadriveninvestor.com / how - to - easily - code - the - mark - minervini - trend - template - in -python - d1f40647fdbc
    return get_fmprep_historical(ticker, key, numdays=numdays, colname=[])[::-1]


def get_descriptive_and_technical(ticker, key, asOfDate=None):
    keys = ['marketCap', 'price', 'avgVolume', 'priceAvg50', 'priceAvg200', 'eps', 'pe', 'sharesOutstanding',
            'yearHigh', 'yearLow', 'exchange', 'change', 'open', 'symbol', 'volume', 'previousClose']

    try:
        res = requests.get(
            'https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={key}'.format(ticker=ticker,
                                                                                          key=key)).json()
        if res:
            hist_prices = get_fmprep_historical(ticker, key, numdays=1500)
            priceAvg20 = statistics.mean(hist_prices[0:20]) if len(hist_prices) > 0 else  0
            descriptive_dict =  dict( (k,v) for k,v in res[0].items() if k in keys)
            descriptive_dict['priceAvg20'] = priceAvg20
            descriptive_dict['changeFromOpen'] = descriptive_dict['price'] - descriptive_dict['open']
            descriptive_dict['allTimeHigh'] = max(hist_prices) if hist_prices else 0
            descriptive_dict['allTimeLow'] = min(hist_prices) if hist_prices else 0

            descriptive_dict['institutionalOwnershipPercentage'] = get_institutional_holders_quote(ticker, key)['institutionalHoldings']

            return descriptive_dict
        else:
            d=  dict((k, -1) for k in keys )
            d['priceAvg20'] = 0
            d['changeFromOpen'] = 0
            d['allTimeHigh'] =  0
            d['allTimeLow'] =  0
            d['institutionalOwnershipPercentage'] = 1

            return d
    except Exception as e:
        return dict((k, -1) for k in keys)


def compute_rsi(ticker, key):
    #https://www.roelpeters.be/many-ways-to-calculate-the-rsi-in-python-pandas/
    #https://tcoil.info/compute-rsi-for-stocks-with-python-relative-strength-index/#:~:text=Here%20we%20will%20describe%20how%20to%20calculate%20RSI,100%20%E2%88%92%20100%201%20%2B%20r%20s%20n
    def _compute(data, time_window):
        diff = data.diff(1).dropna()  # diff in one field(one day)
        # this preservers dimensions off diff values
        up_chg = 0 * diff
        down_chg = 0 * diff
        # up change is equal to the positive difference, otherwise equal to zero
        up_chg[diff > 0] = diff[diff > 0]
        # down change is equal to negative deifference, otherwise equal to zero
        down_chg[diff < 0] = diff[diff < 0]
        # check pandas documentation for ewm
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.ewm.html
        # values are related to exponential decay
        # we set com=time_window-1 so we get decay alpha=1/time_window
        up_chg_avg = up_chg.ewm(com=time_window - 1, min_periods=time_window).mean()
        down_chg_avg = down_chg.ewm(com=time_window - 1, min_periods=time_window).mean()

        rs = abs(up_chg_avg / down_chg_avg)
        rsi = 100 - 100 / (1 + rs)
        return rsi

    url = f'https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from=2022-01-01&to=2022-07-15&apikey={key}'

    historical = requests.get(url).json().get('historical')
    if historical:
        data = pd.DataFrame(data=historical[::-1])
        data['asOfDate'] = pd.to_datetime(data['date'])
        data['RSI'] = _compute(data['adjClose'], 20)
        return data.tail(1).RSI.values[0]
    return 0

def get_asset_play_parameters(ticker, key):
    dataDict = {}
    try:
        data = requests.get('https://financialmodelingprep.com/api/v3/key-metrics-ttm/{}?limit=40&apikey={}'.format(ticker, key)).json()
        if data:
            current = data[0]
            dataDict['bookValuePerShare'] = current.get('bookValuePerShareTTM') or 0
            dataDict['tangibleBookValuePerShare'] = current.get('tangibleBookValuePerShareTTM') or 0
            dataDict['freeCashFlowPerShare'] = current.get('freeCashFlowPerShareTTM') or 0

    except Exception as e:
        pass
    return dataDict



def get_yearly_financial_ratios(ticker, key):
    base_url = 'https://financialmodelingprep.com/api/v3/ratios-ttm/{}?apikey={}'.format(ticker.upper(), key)
    return requests.get(base_url).json()[0]


def filter_historical(statements, asOfDate):
    checkDate = asOfDate or date.today()
    res = [stmnt for stmnt in statements if datetime.strptime(stmnt['dateReported'], '%Y-%m-%d').date() <= checkDate]
    ## massaging data
    mapped_data = map(lambda d: (datetime.strptime(d['dateReported'], '%Y-%m-%d').date(), d['shares']), res)
    from collections import defaultdict
    holdersDict = defaultdict(list)
    for asOfDate, shares in mapped_data:
        holdersDict[asOfDate].append(shares)
    return holdersDict

def get_fundamental_parameters_qtr(ticker,key):
    try :
        income_stmnt = requests.get(
            'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=5&apikey={key}'.format(
                ticker=ticker, key=key)).json()
        qtr_fundamental_dict = {}
        if income_stmnt and len(income_stmnt) > 1:
            # these measures are for the last quarter. so we need most recent data

            eps_thisqtr = income_stmnt[0].get('eps', 0)  # EPS Growt qtr over qtr: > 20%
            eps_lastqtr = income_stmnt[1].get('eps',
                                              0)  # else income_stmnt_as_reported[-1].get('earningspersharebasicanddiluted', 0)    #EPS Growt qtr over qtr: > 20%
            net_sales_thisqtr = income_stmnt[0]['revenue']  # sakes   EPS Growt qtr over qtr: > 20%
            net_sales_lastqtr = income_stmnt[1]['revenue']  # EPS Growt qtr over qtr: > 20%
            qtr_fundamental_dict['income_statement_qtr_date'] = income_stmnt[0]['date']
            qtr_fundamental_dict['income_statement_qtr_date_prev'] = income_stmnt[1]['date']
            sales = [stmnt.get('revenue', 0)/ 1000 for stmnt in income_stmnt[::-1]]
            eps_qtr = [stmnt.get('eps', 0) for stmnt in income_stmnt[::-1]]  # EPS Growt qtr over qtr: > 20%
            randD = [stmnt.get('researchAndDevelopmentExpenses', 0) / 1000 for stmnt in income_stmnt[::-1]]

            qtr_fundamental_dict['researchAndDevelopmentExpenses_qtr'] = income_stmnt[0].get('researchAndDevelopmentExpenses')
            qtr_fundamental_dict['researchAndDevelopmentExpenses_qtr_prev'] = income_stmnt[1].get(
                'researchAndDevelopmentExpenses')
            qtr_fundamental_dict['researchAndDevelopmentExpenses_over_revenues'] = income_stmnt[0].get(
                'researchAndDevelopmentExpenses') / net_sales_thisqtr if net_sales_thisqtr > 0 else 0
            qtr_fundamental_dict['net_sales_progression'] = evaluate_progression(sales)
            qtr_fundamental_dict['net_sales_progression_detail'] = ','.join([str(s) for s in sales])
            qtr_fundamental_dict['eps_progression_last4_qtrs'] = evaluate_progression(eps_qtr)
            qtr_fundamental_dict['eps_progression_last4_qtrs_detail'] = ','.join([str(s) for s in eps_qtr])
            qtr_fundamental_dict['researchAndDevelopmentProgression'] = evaluate_progression(randD)
            qtr_fundamental_dict['researchAndDevelopmentProgression_detail'] = ','.join([str(s) for s in randD])

            op_incomes = [d['operatingIncome'] for d in income_stmnt]
            op_income_cagr = compute_cagr(op_incomes)
            qtr_fundamental_dict['OPERATING_INCOME_CAGR_QTR'] = op_income_cagr

            if eps_lastqtr != 0 and net_sales_lastqtr != 0:
                qtr_fundamental_dict['eps_growth_qtr_over_qtr'] = (eps_thisqtr - eps_lastqtr) / eps_lastqtr
                qtr_fundamental_dict['net_sales_qtr_over_qtr'] = (net_sales_thisqtr - net_sales_lastqtr) / net_sales_lastqtr
            else:
                qtr_fundamental_dict['eps_growth_qtr_over_qtr'] = 0
                qtr_fundamental_dict['net_sales_qtr_over_qtr'] = 0
        return qtr_fundamental_dict
    except Exception as e:
        return {}


def get_analyst_estimates(ticker, key,  fundamental_dict):
    try:
        analyst_estimates = requests.get('https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker}?apikey={key}'.format(ticker=ticker, key=key)).json()
        # achievable. we just need to sort the date
        income_statement_date = fundamental_dict.get('income_statement_date', date.today())
        year = datetime.strptime(income_statement_date, '%Y-%m-%d').date().year
        if analyst_estimates:
            estimateeps_next = [data for data in analyst_estimates if str(year+1) in data['date']]
            if estimateeps_next:
                fundamental_dict['eps_growth_next_year'] = estimateeps_next[0]['estimatedEpsAvg'] 
            else:
                fundamental_dict['eps_growth_next_year'] = 0
        else:
            fundamental_dict['eps_growth_next_year'] = 0
    except Exception as e:
        fundamental_dict['eps_growth_next_year'] = 0
    return fundamental_dict

def get_fundamental_parameters(ticker, key, asOfDate=None):
    fundamental_dict = {}
    try:
        income_statement = requests.get(
            'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=5&apikey={key}'.format(ticker=ticker,
                                                                                                             key=key)).json()
        # THESE ARE MEASURED FOR TRAILING TWELWEMONTHS. EPS = Total Earnings / Total Common Shares Outstanding (trailing twelve months) So we need a ttm for current..

        if len(income_statement) > 2:
            latest = income_statement[0]
            fundamental_dict['cost_of_research_and_dev'] = latest['researchAndDevelopmentExpenses']
            fundamental_dict['income_statement_date'] = latest['date']
            previous = income_statement[1]
            fundamental_dict['income_statement_prev_date'] = previous['date']
            earnings = [stmnt['eps'] for stmnt in income_statement[0:3][::-1]]
            fundamental_dict['eps_progression'] = evaluate_progression(earnings)
            fundamental_dict['eps_progression_detail'] = ','.join([str(e) for e in earnings])


            ## Operating income CAGR ,annually and quarterly
            op_incomes = [d['operatingIncome'] for d in income_statement]
            op_income_cagr = compute_cagr(op_incomes)
            fundamental_dict['OPERATING_INCOME_CAGR'] = op_income_cagr
            fundamental_dict['netIncome'] = latest['netIncome']


            data_5yrs_ago = income_statement[-1]
            eps_thisyear = latest['eps']  # EPS Growth this year: 20%
            eps_prevyear = previous['eps']  # EPS Growth this year: 20%
            eps_5yrs_ago = data_5yrs_ago['eps']
            if eps_prevyear > 0:
                fundamental_dict['eps_growth_this_year'] = (eps_thisyear - eps_prevyear) / eps_prevyear
            else:
                fundamental_dict['eps_growth_this_year'] = -1
            if eps_5yrs_ago > 0:
                complex_n = pow(eps_thisyear / eps_5yrs_ago, 1 / 5) - 1
                fundamental_dict['eps_growth_past_5yrs'] = float(complex_n.real)
                fundamental_dict['epsGrowth5yrs'] = (eps_thisyear - income_statement[4].get('eps', -99)) / income_statement[4].get('eps', -99)
            else:
                fundamental_dict['eps_growth_past_5yrs'] = -1
            # Now we get the quarterl stats
            qtrly_fundamental_dict = get_fundamental_parameters_qtr(ticker, key)
            fundamental_dict.update(qtrly_fundamental_dict)
            return fundamental_dict
    except Exception as e:
        fundamental_dict['cost_of_research_and_dev'] = 0
        fundamental_dict['income_statement_prev_date'] = 0
        fundamental_dict['eps_progression'] = []
        fundamental_dict['eps_progression_detail'] = []

        fundamental_dict['eps_growth_this_year'] = -1
        fundamental_dict['eps_growth_past_5yrs'] = -1
        fundamental_dict['epsGrowth5yrs'] = fundamental_dict['eps_growth_past_5yrs']
        # Now we get the quarterl stats
        qtrly_fundamental_dict = get_fundamental_parameters_qtr(ticker, key)
        fundamental_dict.update(qtrly_fundamental_dict)
        return fundamental_dict


def get_dividend_paid(ticker, key):
    dataDict = {}
    try:

        diviUrl = f"https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{ticker}?apikey={key}"
        divis = requests.get(diviUrl).json()['historical']
        currentDate = date.today()
        hist_date = date(currentDate.year - 20, currentDate.month, currentDate.day)
        all_divis = [d.get('adjDividend', 0) for d in divis if
                     datetime.strptime(d.get('date', date(2000, 1, 1)), '%Y-%m-%d').date() > hist_date]
        dataDict['dividendPaid'] = all([d > 0 for d in all_divis])
        dataDict['dividendPaidEnterprise'] = any([d > 0 for d in all_divis])
        dataDict['dividendPaidRatio'] = dataDict['dividendPaid'] / len(all_divis) if len(all_divis) > 0 else 0
        dataDict['numOfDividendsPaid'] = len([d > 0 for d in all_divis])


    except Exception as e:
        dataDict['dividendPaid'] = False
    return dataDict


def get_financial_ratios(ticker, key):
    financial_ratios = requests.get(
        'https://financialmodelingprep.com/api/v3/ratios-ttm/{ticker}?limit=5&apikey={key}'.format(ticker=ticker,
                                                                                                   key=key)).json()
    ratioDict = {}

    if financial_ratios:
        try:
            latest = financial_ratios[0]

            dataDict = dict(grossProfitMargin=0 if latest.get('grossProfitMarginTTM') is None else latest.get('grossProfitMarginTTM'),
                    returnOnEquity= 0 if latest.get('returnOnEquityTTM') is None else latest.get('returnOnEquityTTM'),
                    dividendPayoutRatio= 0 if latest.get('payoutRatioTTM') is None else latest.get('payoutRatioTTM'),
                    dividendYield=0 if latest.get('dividendYielTTM') is None else latest.get('dividendYielTTM'),
                    returnOnCapital = 0 if latest.get('returnOnCapitalEmployedTTM', 0) is None else latest.get('returnOnCapitalEmployedTTM'),
                    netProfitMargin = 0 if latest.get('netProfitMarginTTM') is None else latest.get('netProfitMarginTTM'),
                    currentRatio = 0 if latest.get('currentRatioTTM') is None else latest.get('currentRatioTTM'),
                    peRatio = 0 if latest.get('peRatioTTM') is None else latest.get('peRatioTTM'))
            ratioDict.update(dataDict)
        except Exception as e:
            return {}
        dividendDict = {}
        ratioDict.update(dividendDict)
    return ratioDict



def get_shares_float(ticker, key):
    # we might not have it. but for canslim it does not matter
    res = requests.get(
        'https://financialmodelingprep.com/api/v4/shares_float?symbol={}&apikey={}'.format(ticker, key)).json()
    return res[0]['floatShares'] if res else 0



def get_instutional_holders_percentage_yahoo(ticker):
    from bs4 import BeautifulSoup  # Move to aJob
    import requests

    try:
        link = f'https://finance.yahoo.com/quote/{ticker}/holders?p={ticker}'
        r = requests.get(link, headers={'user-agent': 'my-app/0.0.1'})
        bs = BeautifulSoup(r.content, 'html.parser')
        spans = bs.find_all('span')

        inst_span = [s for s in spans if 'of Shares Held by Institutions' in s.text][0]
        row = inst_span.parent.parent

        pcnt_text = row.find_all('td')[0].text
        return float(pcnt_text[:-1]) / 100
    except Exception as e:
        return 0.55555  # returning an allowed value

def get_institutional_holders_quote(ticker, key, asOfDate=None):
    # we need to be smarter here. only filter for results whose date is lessorequal the current date.

    pcnt = get_instutional_holders_percentage_yahoo(ticker)
    return {'institutionalHoldings': pcnt}

def get_institutional_holders_percentage(ticker, exchange):
    import requests
    import re
    str1 = requests.get(
        'https://www.marketbeat.com/stocks/{}/{}/institutional-ownership/'.format(exchange, ticker.upper())).text
    string_pattern = r"Institutional Ownership Percentage.*[\d]+\.[\d]+%<\/div>"
    # compile string pattern to re.Pattern object
    regex_pattern = re.compile(string_pattern)
    res = regex_pattern.findall(str1)[0]
    return float(res[res.find('strong>') + 7: res.rfind('%')])


def get_all_data(ticker, key):
  try:
    desc_tech_dict = get_descriptive_and_technical(ticker, key)
    return desc_tech_dict
  except Exception as e:
    print('Exception:Could not fetch data for :{}:{}'.format(ticker, str(e)))

def get_fundamentals(input_dict, key):
    ticker = input_dict['symbol']
    try:
        # need to think different strategy for historical. perhaps we get it at the  bottom so that we only have few hundreds to fetch
        #
        fund_dict = get_fundamental_parameters(ticker, key)
        newd = input_dict.copy()
        newd.update(fund_dict)
        return newd
    except Exception as e:
        return None


def get_balancesheet_benchmark(ticker, key):
    try:
        dataDict = {}
        balance_sheet = requests.get(
            'https://financialmodelingprep.com/api/v3/balance-sheet-statement/{ticker}?limit=1&apikey={key}'.format(
                ticker=ticker, key=key)).json()
        debtOverCapital = 1
        if balance_sheet and len(balance_sheet) > 0:
            bs = balance_sheet[0]
            totalAssets = bs.get('totalCurrentAssets') or 0
            totalLiabilities = bs.get('totalCurrentLiabilities') or 0
            longTermDebt = bs.get('longTermDebt') or 0
            debtOverCapital = longTermDebt - (totalAssets - totalLiabilities)
            dataDict['debtOverCapital'] = debtOverCapital
            dataDict['enterpriseDebt'] = longTermDebt / (totalAssets - totalLiabilities)
            dataDict['totalAssets'] = bs.get('totalAssets') or 0
            dataDict['inventory'] = bs.get('inventory') or 0
            dataDict['totalCurrentAssets'] = totalAssets
            dataDict['totalCurrentLiabilities'] = totalLiabilities

            return dataDict
    except Exception as e:
        return None


def compute_cagr(input_list):
    ''' CAGR = (Last amount / starting amo) ^ (1 / number of years) '''
    starter = input_list[0]

    if input_list[-1] < 0:
        return '-0.0'
    if any([item < 0 for item in input_list]):
        return '-0.0'
    if all([item != 0 for item in input_list]):
        return ','.join ([ "%.2f" %  ((current_amount / starter) ** (1 / (idx+1))) for idx, current_amount in enumerate(input_list[1:])])
    return '-0.0'


def get_income_benchmark(ticker, key):
    # some eps in last 10 yrs
    try:
        dataDict = {}
        income_statement = requests.get(
            'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=10&apikey={key}'.format(
                ticker=ticker, key=key)).json()
        all_eps = [d['eps'] for d in income_statement]

        if len(all_eps) >= 6:

            latest_three = statistics.mean(all_eps[0:3])
            first_eps = statistics.mean(all_eps[-3:])
            if first_eps > 0:
                dataDict['epsGrowth'] = (latest_three - first_eps) / first_eps

            else:
                dataDict['epsGrowth'] = 0

            if all_eps[4] > 0:
                dataDict['epsGrowth5yrs'] = (all_eps[0] - all_eps[4]) / all_eps[4]

            op_incomes = [d['operatingIncome'] for d in income_statement]
            op_income_cagr = compute_cagr(op_incomes)
            dataDict['OPERATING_INCOME_CAGR'] = op_income_cagr

            positive_eps = [e > 0 for e in all_eps]
            dataDict['positiveEps'] = len(positive_eps)
            dataDict['positiveEpsLast5Yrs'] = len([e > 0 for e in all_eps[0:5]])
            latest = income_statement[0]
            dataDict['netIncome'] = latest['netIncome']
            dataDict['income_statement_date'] = latest['date']

            return dataDict
    except Exception as e:
        return None

def get_eps_growth(ticker, key, field='estimatedEpsAvg'):
    try:
        baseUrl = f'https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker}?apikey={key}'
        return requests.get(baseUrl).json()[0].get(field)
    except Exception as e:
        return -99



def get_key_metrics_benchmark(ticker, key):
    # some eps in last 10 yrs
    dataDict = {}

    try:
        keyMetrics = requests.get(
            'https://financialmodelingprep.com/api/v3/key-metrics-ttm/{}?limit=2&apikey={}'.format(ticker, key)).json()

        if keyMetrics:

            dataDict['tangibleBookValuePerShare'] = keyMetrics[0].get('tangibleBookValuePerShareTTM') or 0
            dataDict['netCurrentAssetValue'] = keyMetrics[0].get('netCurrentAssetValueTTM') or 0
            dataDict['freeCashFlowPerShare'] = keyMetrics[0].get('freeCashFlowPerShareTTM') or 0
            dataDict['earningsYield'] = keyMetrics[0].get('earningsYieldTTM') or 0
    except Exception as e:
        logging.info(f'Exception in gettng keymeetrics for {ticker}:{str(e)}')
    return dataDict

def get_peter_lynch_ratio(key, ticker, inputDict):
    # need growth ratio, div yield and pe/ratio
    try:
        divYield = inputDict['dividendYield'] * 100
        peRatio = inputDict['peRatio']
        growth =  get_eps_growth(ticker, key)

        if peRatio and peRatio != 0:
            return (divYield + growth) / peRatio
    except Exception as e:
        logging.info(f'Unable to calculate plynch for {ticker}')
    return -1



def get_financial_ratios_benchmark(ticker, key):
    # some eps in last 10 yrs
    try:
        dataDict = {}
        financial_ratios = requests.get(
            'https://financialmodelingprep.com/api/v3/ratios-ttm/{ticker}?limit=5&apikey={key}'.format(ticker=ticker,
                                                                                                       key=key)).json()
        if isinstance(financial_ratios, list) and financial_ratios:
            try:
                latest = financial_ratios[0]
            except Exception as e:
                return None

            try:
                divis = requests.get(
                    'https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{}?apikey={}'.format(
                        ticker, key)).json()['historical']
                currentDate = date.today()
                hist_date = date(currentDate.year - 20, currentDate.month, currentDate.day)
                all_divis = [d.get('adjDividend', 0) for d in divis if
                             datetime.strptime(d.get('date', date(2000, 1, 1)), '%Y-%m-%d').date() > hist_date]
                dataDict['dividendPaid'] = all([d > 0 for d in all_divis])
                dataDict['dividendPaidEnterprise'] = any([d > 0 for d in all_divis])

                dataDict['dividendPayoutRatio'] = 0 if latest.get('payoutRatioTTM') is None else latest.get('payoutRatioTTM')
                dataDict['numOfDividendsPaid'] = len([d for d in all_divis if d > 0])
                dataDict['returnOnCapital'] = 0 if latest.get('returnOnCapitalEmployedTTM', 0) is None else \
                    latest.get('returnOnCapitalEmployedTTM')

            except Exception as e:
                pass

            dataDict['pe'] = latest.get('priceEarningsRatioTTM') or 0
            dataDict['peRatio'] = dataDict['pe']
            dataDict['netProfitMargin'] = 0 if latest.get('netProfitMarginTTM') is None else latest.get(
                'netProfitMarginTTM')
            # price to book ratio no more than 1.5
            dataDict['currentRatio'] = latest.get('currentRatioTTM') or 0
            dataDict['priceToBookRatio'] = latest.get('priceToBookRatioTTM') or 0

            dataDict['grossProfitMargin'] = latest.get('grossProfitMarginTTM') or 0
            dataDict['returnOnEquity'] = latest.get('returnOnEquityTTM')  or 0
            dataDict['dividendPayoutRatio'] = latest.get('payoutRatioTTM') or 0
            dataDict['dividendYield'] = latest.get('dividendYielTTM') or 0
            dataDict['returnOnCapital'] =  latest.get('returnOnCapitalEmployedTTM') or 0
            return dataDict
    except Exception as e:
        return dataDict

def get_price_change(ticker, key):
    resUrl = f'https://financialmodelingprep.com/api/v3/stock-price-change/{ticker}?apikey={key}'
    dataDict = {}
    dataDict['ticker'] = ticker

    try:
        res = requests.get(resUrl).json()[ 0]
        dataDict['52weekChange'] = res.get('1Y') or 0
    except Exception as e:
        dataDict['52weekChange'] = -1
    return dataDict

def get_quote_benchmark(ticker, key):
    resUrl = 'https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={key}'.format(ticker=ticker, key=key)
    keys = ['marketCap', 'price', 'avgVolume', 'priceAvg50', 'priceAvg200', 'eps', 'pe', 'sharesOutstanding',
            'yearHigh', 'yearLow', 'exchange', 'change', 'open', 'symbol']
    dataDict = {}

    try:
        dataDict['ticker'] = ticker
        res = requests.get(resUrl).json()[ 0]
        dataDict = dict((k,v) for k, v in res.items() if k in keys)
        # then check ownership < 60% fund ownership
        dataDict['institutionalOwnershipPercentage'] = get_institutional_holders_quote(ticker, key)['institutionalHoldings']

        return dataDict
    except Exception as e:
        logging.info(f'Exception in getting quote daqta for :{ticker}:{str(e)}')

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
                    if priceChangeDict:
                        quotes_data.update(priceChangeDict)

    return quotes_data

