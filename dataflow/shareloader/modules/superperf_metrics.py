from datetime import datetime, date
import requests
import logging

# Criteria #1
# ==== WATCH LIST ===
# Market Cap > 2bln (mid)
import statistics


def get_fmprep_historical(ticker, key):
    hist_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/{}?serietype=line&apikey={}'.format(
        ticker, key)
    data = requests.get(hist_url).json()['historical']
    return data


def get_descriptive_and_technical(ticker, key):
    res = requests.get(
        'https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={key}'.format(ticker=ticker, key=key)).json()
    keys = ['marketCap', 'price', 'avgVolume', 'priceAvg50', 'priceAvg200', 'eps', 'pe', 'sharesOutstanding',
            'yearHigh', 'yearLow', 'exchange', 'change', 'open']

    hist_prices = get_fmprep_historical(ticker, key)
    all_prices = [d['close'] for d in hist_prices]
    all_time_high = max(all_prices)
    all_time_low = min(all_prices)
    priceAvg20 = sum(all_prices[0:20]) / 20.0
    print('Price avg 20 is:{}'.format(priceAvg20))
    base_dict = dict((k, res[0][k]) for k in keys)
    base_dict['priceAvg20'] = priceAvg20
    base_dict['allTimeHigh'] = all_time_high
    base_dict['allTimeLow'] = all_time_low
    base_dict['weeks52High'] = base_dict['yearHigh']
    base_dict['ticker'] = ticker
    base_dict['changeFromOpen'] = base_dict['price'] - base_dict['open']
    return base_dict


def get_yearly_financial_ratios(ticker):
    key = getfmpkeys()
    base_url = 'https://financialmodelingprep.com/api/v3/ratios-ttm/{}?apikey={}'.format(ticker.upper(), key)
    return requests.get(base_url).json()[0]


def get_fundamental_parameters(ticker, key, offset=0):
    print('Getting data for:{}, offset={}'.format(ticker, offset))
    fundamental_dict = {}

    income_statement = requests.get(
        'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=5&apikey={key}'.format(ticker=ticker,
                                                                                                         key=key)).json()
    if len(income_statement) > 2:
        latest = income_statement[0]
        previous = income_statement[1]
        data_5yrs_ago = income_statement[-1]
        eps_thisyear = latest['eps']  # EPS Growth this year: 20%
        eps_prevyear = previous['eps']  # EPS Growth this year: 20%
        eps_5yrs_ago = data_5yrs_ago['eps']
        fundamental_dict['eps_growth_this_year'] = (eps_thisyear - eps_prevyear) / eps_prevyear
        fundamental_dict['eps_growth_past_5yrs'] = (eps_thisyear - eps_5yrs_ago) / eps_5yrs_ago
    else:
        fundamental_dict['eps_growth_this_year'] = 0
        fundamental_dict['eps_growth_past_5yrs'] = 0
    analyst_estimates = requests.get(
        'https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker}?apikey={key}'.format(ticker=ticker,
                                                                                                  key=key)).json()
    year = date.today().year
    if analyst_estimates:
        estimateeps_next = [data for data in analyst_estimates if str(year + 1) in data['date']][0][
            'estimatedEpsAvg']  # EPS Growth next year: >20%
        fundamental_dict['eps_growth_next_year'] = estimateeps_next
    else:
        fundamental_dict['eps_growth_next_year'] = 0
    income_stmnt = requests.get(
        'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=5&apikey={key}'.format(
            ticker=ticker, key=key)).json()
    income_stmnt_as_reported = requests.get(
        'https://financialmodelingprep.com/api/v3/income-statement-as-reported/{ticker}?period=quarter&limit=5&apikey={key}'.format(
            ticker=ticker, key=key)).json()

    if income_stmnt and income_stmnt_as_reported:
        eps_thisqtr = income_stmnt_as_reported[0].get('earningspersharebasic', 0)  # EPS Growt qtr over qtr: > 20%
        eps_lastqtr = income_stmnt_as_reported[-1]['earningspersharebasic'] if 'earningspersharebasic' in \
                                                                               income_stmnt_as_reported[-1].keys() \
            else income_stmnt_as_reported[-1].get('earningspersharebasicanddiluted', 0)  # EPS Growt qtr over qtr: > 20%
        net_sales_thisqtr = income_stmnt[0]['revenue']  # sakes   EPS Growt qtr over qtr: > 20%
        net_sales_lastqtr = income_stmnt[1]['revenue']  # EPS Growt qtr over qtr: > 20%

        if eps_lastqtr != 0 and net_sales_lastqtr != 0:
            fundamental_dict['eps_growth_qtr_over_qtr'] = (eps_thisqtr - eps_lastqtr) / eps_lastqtr
            fundamental_dict['net_sales_qtr_over_qtr'] = (net_sales_thisqtr - net_sales_lastqtr) / net_sales_lastqtr
        else:
            print('Setting tozero')
            fundamental_dict['eps_growth_qtr_over_qtr'] = 0
            fundamental_dict['net_sales_qtr_over_qtr'] = 0
    # Net Sales
    financial_ratios = requests.get(
        'https://financialmodelingprep.com/api/v3/ratios/{ticker}?limit=5&apikey={key}'.format(ticker=ticker,
                                                                                               key=key)).json()
    latest = financial_ratios[0] if financial_ratios else {}
    fundamental_dict['grossProfitMargin'] = latest.get('grossProfitMargin', 0)
    fundamental_dict['returnOnEquity'] = latest.get('returnOnEquity', 0)
    fundamental_dict['dividendPayoutRatio'] = latest.get('dividendPayoutRatio', 0.0)
    fundamental_dict['dividendYield'] = latest.get('dividendYield', 0.0)
    return fundamental_dict


def get_shares_float(ticker, key):
    res = requests.get(
        'https://financialmodelingprep.com/api/v4/shares_float?symbol={}&apikey={}'.format(ticker, key)).json()
    return res[0]['floatShares'] if res else 0


def get_institutional_holders_quote(ticker, key):
    res = requests.get(
        'https://financialmodelingprep.com/api/v3/institutional-holder/{}?apikey={}'.format(ticker, key)).json()
    return {'institutionalHoldings': sum(d['shares'] for d in res)}


def get_all_data(ticker, key):
    try:
        desc_tech_dict = get_descriptive_and_technical(ticker, key)
        fund_dict = get_fundamental_parameters(ticker, key)
        inst_holders_dict = get_institutional_holders_quote(ticker, key)
        desc_tech_dict.update(fund_dict)
        desc_tech_dict.update(inst_holders_dict)
        desc_tech_dict['institutionalHoldingsPercentage'] = desc_tech_dict['institutionalHoldings'] / desc_tech_dict[
            'sharesOutstanding']
        desc_tech_dict['sharesFloat'] = get_shares_float(ticker, key)
        return desc_tech_dict
    except Exception as e:
        logging.info('Could not fetch data for :{}'.format(ticker))
        return {'ticker' :ticker}
