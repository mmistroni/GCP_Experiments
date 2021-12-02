from datetime import datetime, date
import requests
import logging

# Criteria #1
# ==== WATCH LIST ===
# Market Cap > 2bln (mid)
import statistics


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


def evaluate_progression(input):
    if len(input) < 2:
        return False
    start = input[0:-1]
    end = input[1:]
    zipped = zip(start, end)
    res = [(tpl[1] > tpl[0]) for tpl in zipped]
    return all(res)




def get_fmprep_historical(ticker, key):
    hist_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/{}?apikey={}'.format(ticker, key)
    data = requests.get(hist_url).json()['historical']
    return data


def get_common_shares_outstanding(ticker, key):
    res2 = requests.get(
        'https://financialmodelingprep.com/api/v3/balance-sheet-statement-as-reported/{}?limit=10&apikey={}'.format(
            ticker, key)).json()
    return [(d['date'], d['commonstocksharesoutstanding']) for d in res2]


def get_descriptive_and_technical(ticker, key, asOfDate=None):
    res = requests.get(
        'https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={key}'.format(ticker=ticker, key=key)).json()
    keys = ['marketCap', 'price', 'avgVolume', 'priceAvg50', 'priceAvg200', 'eps', 'pe', 'sharesOutstanding',
            'yearHigh', 'yearLow', 'exchange', 'change', 'open', 'symbol']
    if res:
        return dict( (k,v) for k,v in res[0].items() if k in keys)


def get_yearly_financial_ratios(ticker, key):
    base_url = 'https://financialmodelingprep.com/api/v3/ratios-ttm/{}?apikey={}'.format(ticker.upper(), key)
    return requests.get(base_url).json()[0]


def filter_historical(statements, asOfDate):
    print('Filtering for date <= {}'.format(asOfDate))
    res = [stmnt for stmnt in statements if datetime.strptime(stmnt['date'], '%Y-%m-%d').date() <= asOfDate]
    return res

def get_fundamental_parameters_qtr(ticker,key):
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
        if eps_lastqtr != 0 and net_sales_lastqtr != 0:
            qtr_fundamental_dict['eps_growth_qtr_over_qtr'] = (eps_thisqtr - eps_lastqtr) / eps_lastqtr
            qtr_fundamental_dict['net_sales_qtr_over_qtr'] = (net_sales_thisqtr - net_sales_lastqtr) / net_sales_lastqtr
        else:
            qtr_fundamental_dict['eps_growth_qtr_over_qtr'] = 0
            qtr_fundamental_dict['net_sales_qtr_over_qtr'] = 0
    return qtr_fundamental_dict


def get_analyst_estimates(ticker, key,  fundamental_dict):
    analyst_estimates = requests.get('https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker}?apikey={key}'.format(ticker=ticker, key=key)).json()
    # achievable. we just need to sort the date
    income_statement_date = fundamental_dict.get('income_statement_date', date.today())
    year = datetime.strptime(income_statement_date, '%Y-%m-%d').date().year
    logging.info('getting estimates for {}@{}'.format(ticker, income_statement_date))
    if analyst_estimates:
        estimateeps_next = [data for data in analyst_estimates if str(year+1) in data['date']]
        if estimateeps_next:
            fundamental_dict['eps_growth_next_year'] = estimateeps_next['estimatedEpsAvg'] 
        else:
            fundamental_dict['eps_growth_next_year'] = 0
    else:
        fundamental_dict['eps_growth_next_year'] = 0

    return fundamental_dict



def get_fundamental_parameters(ticker, key, asOfDate=None):
    fundamental_dict = {}

    income_statement = requests.get(
        'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=5&apikey={key}'.format(ticker=ticker,
                                                                                                         key=key)).json()
    logging.info('Income statement is:{}'.format(income_statement))

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
        else:
            fundamental_dict['eps_growth_past_5yrs'] = -1
        # Now we get the quarterl stats
        qtrly_fundamental_dict = get_fundamental_parameters_qtr(ticker, key)
        fundamental_dict.update(qtrly_fundamental_dict)
        return fundamental_dict

def get_financial_ratios(ticker, key):
    financial_ratios = requests.get(
        'https://financialmodelingprep.com/api/v3/ratios-ttm/{ticker}?limit=5&apikey={key}'.format(ticker=ticker,
                                                                                                   key=key)).json()
    if financial_ratios:
        try:
            latest = financial_ratios[0]

            return dict(grossProfitMargin=0 if latest.get('grossProfitMarginTTM') is None else latest.get('grossProfitMarginTTM'),
                    returnOnEquity= 0 if latest.get('returnOnEquityTTM') is None else latest.get('returnOnEquityTTM'),
                    dividendPayoutRatio= 0 if latest.get('payoutRatioTTM') is None else latest.get('payoutRatioTTM'),
                    dividendYield=0 if latest.get('dividendYielTTM') is None else latest.get('dividendYielTTM'),
                    returnOnCapital = 0 if latest.get('returnOnCapitalEmployedTTM', 0) is None else latest.get('dividendYielTTM'))
        except Exception as e:
            logging.info('Could not find ratios for {}:{}={}'.format(ticker, financial_ratios, str(e)))
            return {}



def get_shares_float(ticker, key):
    # we might not have it. but for canslim it does not matter
    res = requests.get(
        'https://financialmodelingprep.com/api/v4/shares_float?symbol={}&apikey={}'.format(ticker, key)).json()
    return res[0]['floatShares'] if res else 0


def get_institutional_holders_quote(ticker, key, asOfDate=None):
    # we need to be smarter here. only filter for results whose date is lessorequal the current date.
    res = requests.get(
        'https://financialmodelingprep.com/api/v3/institutional-holder/{}?apikey={}'.format(ticker, key)).json()
    if asOfDate:
        iholders = filter_historical(res, asOfDate)
    else:
        iholders = res
    return {'institutionalHoldings': sum(d['shares'] for d in iholders)}


def get_institutional_holders_percentage(ticker, exchange):
    import requests
    import re
    print('GEttign ihp for exchange:{}'.format(exchange))

    str1 = requests.get(
        'https://www.marketbeat.com/stocks/{}/{}/institutional-ownership/'.format(exchange, ticker.upper())).text
    string_pattern = r"Institutional Ownership Percentage.*[\d]+\.[\d]+%<\/div>"
    # compile string pattern to re.Pattern object
    regex_pattern = re.compile(string_pattern)
    res = regex_pattern.findall(str1)[0]
    return float(res[res.find('strong>') + 7: res.rfind('%')])


def get_all_data(ticker, key):
  try:
    logging.info('Get al data.t icker is:{}'.format(ticker))
    desc_tech_dict = get_descriptive_and_technical(ticker, key)
    #desc_tech_dict = get_fundamental_parameters(ticker, key)
    #desc_tech_dict.update(fund_dict)
    #inst_holders_dict = get_institutional_holders_quote(ticker, key)
    #desc_tech_dict.update(fund_dict)
    #desc_tech_dict.update(inst_holders_dict)
    #desc_tech_dict['institutionalHoldingsPercentage'] = desc_tech_dict['institutionalHoldings'] / desc_tech_dict['sharesOutstanding']
    #desc_tech_dict['sharesFloat'] = get_shares_float(ticker, key)
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
        logging.info('Failed to retrieve data for {}:{}'.format(ticker, str(e)))
