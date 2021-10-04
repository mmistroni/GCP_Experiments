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


def get_form4_filings(ticker):
    qry = "SELECT COB, TICKER, COUNT, PRICE  FROM `datascience-projects.gcp_edgar.form_4_daily_enhanced` WHERE TICKER = '{}' ".format(
        ticker)
    return get_bigquery_as_dataframe(qry)


def get_form13_filings(ticker):
    qry = "SELECT PERIODOFREPORT, SUM(COUNT) FROM `datascience-projects.gcp_edgar.form_13hf_daily_enhanced` WHERE TICKER = '{}' GROUP BY PERIODOFREPORT".format(
        ticker)
    return get_bigquery_as_dataframe(qry)


def get_fmprep_historical(ticker, key):
    hist_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/{}?apikey={}'.format(ticker, key)
    data = requests.get(hist_url).json()['historical']
    return data


def get_common_shares_outstanding(ticker):
    res2 = requests.get(
        'https://financialmodelingprep.com/api/v3/balance-sheet-statement-as-reported/{}?limit=10&apikey={}'.format(
            ticker, key)).json()
    return [(d['date'], d['commonstocksharesoutstanding']) for d in res2]


def get_descriptive_and_technical(ticker, key, asOfDate=None):
    res = requests.get(
        'https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={key}'.format(ticker=ticker, key=key)).json()
    keys = ['marketCap', 'price', 'avgVolume', 'priceAvg50', 'priceAvg200', 'eps', 'pe', 'sharesOutstanding',
            'yearHigh', 'yearLow', 'exchange', 'change', 'open']
    print(res)
    return [dict( (k,v) for k,v in d.items() if k in keys) for d in res   ]


def get_yearly_financial_ratios(ticker, key):
    base_url = 'https://financialmodelingprep.com/api/v3/ratios-ttm/{}?apikey={}'.format(ticker.upper(), key)
    return requests.get(base_url).json()[0]


def filter_historical(statements, asOfDate):
    print('Filtering for date <= {}'.format(asOfDate))
    res = [stmnt for stmnt in statements if datetime.strptime(stmnt['date'], '%Y-%m-%d').date() <= asOfDate]
    return res


def get_fundamental_parameters(ticker, key, asOfDate=None):
    print('Getting data for:{}, offset={}'.format(ticker, asOfDate))
    fundamental_dict = {}

    income_statement = requests.get(
            'https://financialmodelingprep.com/api/v3/income-statement/{}?period=quarter&limit=4&apikey={}'.format(ticker, key)).json()

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
        fundamental_dict['eps_growth_this_year'] = (eps_thisyear - eps_prevyear) / eps_prevyear
        fundamental_dict['eps_growth_past_5yrs'] = pow(eps_thisyear / eps_5yrs_ago, 1 / 5) - 1


    else:
        print('No income stmpt for :{}'.format(asOfDate))
        fundamental_dict['eps_growth_this_year'] = 0
        fundamental_dict['eps_growth_past_5yrs'] = 0
        fundamental_dict['eps_progression'] = False
        fundamental_dict['eps_progression_detail'] = 'NA'

    # THis depends on dates.
    analyst_estimates = requests.get(
        'https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker}?apikey={key}'.format(ticker=ticker,
                                                                                                  key=key)).json()
    # achievable. we just need to sort the date
    year = date.today().year if not asOfDate else datetime.strptime(fundamental_dict['income_statement_date'],
                                                                    '%Y-%m-%d').year
    if analyst_estimates:
        estimateeps_next = [data for data in analyst_estimates if str(year + 1) in data['date']][0]
        # EPS Growth next year: >20%

        fundamental_dict['eps_growth_next_year'] = estimateeps_next['estimatedEpsAvg']
    else:
        fundamental_dict['eps_growth_next_year'] = 0

    # also add previous 3ys pe . previous quarters pe

    '''
    else:
      # https://groww.in/blog/how-to-assess-a-companys-growth-potential/
      # Sustainable Growth Rate = Return on Equity x (1 – Dividend Payout Ratio)
      print('Fetching historical...')
      all_income_statement = requests.get('https://financialmodelingprep.com/api/v3/ratios/{ticker}?limit=20&apikey={key}'.format(ticker=ticker, key=key)).json()
      filtered = filter_historical(all_income_statement, asOfDate)[0]
      payoutRatio = filtered.get('dividendPayoutRatio', 0)
      roe = filtered.get('returnOnEquity', 0)
      fundamental_dict['eps_growth_next_year'] = roe * (1 - payoutRatio)
    '''

    if asOfDate:
        all_income_stmnt = requests.get(
            'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=40&apikey={key}'.format(
                ticker=ticker, key=key)).json()
        income_stmnt = filter_historical(all_income_stmnt, asOfDate)[0:5]

    else:
        # this one is hard. we need to increase the limit to 5 + <n years>. then exclude the first n * 5
        income_stmnt = requests.get(
            'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=5&apikey={key}'.format(
                ticker=ticker, key=key)).json()

    if income_stmnt:
        # these measures are for the last quarter. so we need most recent data
        eps_thisqtr = income_stmnt[0].get('eps', 0)  # EPS Growt qtr over qtr: > 20%
        eps_lastqtr = income_stmnt[1].get('eps',
                                          0)  # else income_stmnt_as_reported[-1].get('earningspersharebasicanddiluted', 0)    #EPS Growt qtr over qtr: > 20%
        net_sales_thisqtr = income_stmnt[0]['revenue']  # sakes   EPS Growt qtr over qtr: > 20%
        net_sales_lastqtr = income_stmnt[1]['revenue']  # EPS Growt qtr over qtr: > 20%
        fundamental_dict['income_statement_qtr_date'] = income_stmnt[0]['date']
        fundamental_dict['income_statement_qtr_date_prev'] = income_stmnt[1]['date']
        sales = [stmnt['revenue'] / 1000 for stmnt in income_statement[::-1]]
        eps_qtr = [stmnt['eps'] for stmnt in income_statement[::-1]]  # EPS Growt qtr over qtr: > 20%
        randD = [stmnt.get('researchAndDevelopmentExpenses', 0) / 1000 for stmnt in income_statement[::-1]]

        fundamental_dict['researchAndDevelopmentExpenses_qtr'] = income_stmnt[0].get('researchAndDevelopmentExpenses')
        fundamental_dict['researchAndDevelopmentExpenses_qtr_prev'] = income_stmnt[1].get(
            'researchAndDevelopmentExpenses')

        fundamental_dict['researchAndDevelopmentExpenses_over_revenues'] = income_stmnt[0].get(
            'researchAndDevelopmentExpenses') / net_sales_thisqtr

        fundamental_dict['net_sales_progression'] = evaluate_progression(sales)
        fundamental_dict['net_sales_progression_detail'] = ','.join([str(s) for s in sales])
        fundamental_dict['eps_progression_last4_qtrs'] = evaluate_progression(eps_qtr)
        fundamental_dict['eps_progression_last4_qtrs_detail'] = ','.join([str(s) for s in eps_qtr])
        fundamental_dict['researchAndDevelopmentProgression'] = evaluate_progression(randD)
        fundamental_dict['researchAndDevelopmentProgression_detail'] = ','.join([str(s) for s in randD])

        if eps_lastqtr != 0 and net_sales_lastqtr != 0:
            print('Setting eps qtr over qtr')
            fundamental_dict['eps_growth_qtr_over_qtr'] = (eps_thisqtr - eps_lastqtr) / eps_lastqtr
            fundamental_dict['net_sales_qtr_over_qtr'] = (net_sales_thisqtr - net_sales_lastqtr) / net_sales_lastqtr
        else:
            print('Setting tozero')
            fundamental_dict['eps_growth_qtr_over_qtr'] = 0
            fundamental_dict['net_sales_qtr_over_qtr'] = 0
    # Net Sales
    # same here
    # Financial ratios, w
    if asOfDate:
        all_financial_ratios = requests.get(
            'https://financialmodelingprep.com/api/v3/ratios/{ticker}?limit=20&apikey={key}'.format(ticker=ticker,
                                                                                                    key=key)).json()
        financial_ratios = filter_historical(all_financial_ratios, asOfDate)
        latest = financial_ratios[0] if financial_ratios else {}

       


    else:
        financial_ratios = requests.get(
            'https://financialmodelingprep.com/api/v3/ratios-ttm/{ticker}?limit=5&apikey={key}'.format(ticker=ticker,
                                                                                                       key=key)).json()
        latest = financial_ratios[0] if financial_ratios else {}

        fundamental_dict['grossProfitMargin'] = latest.get('grossProfitMarginTTM', 0)
        fundamental_dict['returnOnEquity'] = latest.get('returnOnEquityTTM', 0)
        fundamental_dict['dividendPayoutRatio'] = latest.get('payoutRatioTTM', 0.0)
        fundamental_dict['dividendYield'] = latest.get('dividendYielTTM', 0.0)
        fundamental_dict['returnOnCapital'] = latest.get('returnOnCapitalEmployedTTM', 0)

    return fundamental_dict


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
    print('Get al data.t icker is:{}'.format(ticker))
    #desc_tech_dict = get_descriptive_and_technical(ticker, key)
    desc_tech_dict = get_fundamental_parameters(ticker, key)
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
    ticker = input_dict['ticker']
    try:
        # need to think different strategy for historical. perhaps we get it at the  bottom so that we only have few hundreds to fetch
        #
        fund_dict = get_fundamental_parameters(ticker, key)
        newd = input_dict.copy()
        newd.update(fund_dict)
        return newd
    except Exception as e:
        logging.info('Failed to retrieve data for {}:{}'.format(ticker, str(e)))
