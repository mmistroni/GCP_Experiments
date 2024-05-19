## finviz utilities
#https://pypi.org/project/finvizfinance/
# https://finvizfinance.readthedocs.io/en/latest/
#https://medium.com/the-investors-handbook/the-best-finviz-screens-for-growth-investors-72795f507b91
from finvizfinance.screener.overview import Overview

'''
res = (input_dict.get('marketCap', 0) > 300000000) and (input_dict.get('avgVolume', 0) > 200000) \
        and (input_dict.get('price', 0) > 10) and (input_dict.get('eps_growth_this_year', 0) > 0.2) \
        and (input_dict.get('grossProfitMargin', 0) > 0) \
        and  (input_dict.get('price', 0) > input_dict.get('priceAvg20', 0))\
        and (input_dict.get('price', 0) > input_dict.get('priceAvg50', 0)) \
        and (input_dict.get('price', 0) > input_dict.get('priceAvg200', 0))  \
        and (input_dict.get('net_sales_qtr_over_qtr', 0) > 0.2) and (input_dict.get('returnOnEquity', 0) > 0) \
        and (input_dict.get('eps_growth_next_year', 0) > 0) and (input_dict.get('eps_growth_qtr_over_qtr', 0) > 0.2)
'''

def _run_screener(filters):
    foverview = Overview()
    foverview.set_filter(filters_dict=filters)
    df = foverview.screener_view()
    return df.to_dict('records') if df is not None else []

def get_universe_filter():

    filters_dict = {'Market Cap.':'+Small (over $300mln)',
                    'Average Volume' : 'Over 200K',
                    'Price' : 'Over $10',
                    'EPS growththis year' :  'Over 20%',
                    'EPS growthnext year' :  'Positive (>0%)',
                    'Gross Margin' : 'Positive (>0%)',
                    'EPS growthqtr over qtr': 'Over 20%',
                    'Sales growthqtr over qtr' : 'Over 20%',
                    'Return on Equity' : 'Positive (>0%)'

                    }
    return filters_dict

def get_universe_stocks():
    filter = get_universe_filter()
    return _run_screener(filter)

def \
        get_canslim():
    '''
    Descriptive Parameters:

    Average Volume: Over 200K
    Float: Under 50M
    Stocks only (ex-Funds)
    Stocks that have above 200K average daily volume are liquid and stocks with a low float under 50 million shares are more likely to explode faster because of the lower supply. For example, low float stocks like FUTU, CELH, BLNK, GRWG, SI, and DQ are all up more than 750% from their 52-week lows.

    Fundamental Parameters:

    EPS Growth This Year: Over 20%
    EPS Growth Next Year: Over 20%
    EPS Growth qtr over qtr: Over 20%
    Sales Growth qtr over qtr: Over 20%
    EPS Growth past 5 years: Over 20%
    Return on Equity: Positive (>0%)
    Gross Margin: Positive (>0%)
    Institutional Sponsorship: Over 20%

    Technical Parameters:

    Price above SMA20
    Price above SMA50
    Price above SMA200
    0–10% below High

        :return:
        '''

    price_filters =  {
        '20-Day Simple Moving Average': 'Price above SMA20',
        '50-Day Simple Moving Average': 'Price above SMA50',
        '200-Day Simple Moving Average': 'Price above SMA200',
        '52-Week High/Low': '0-10% below High'
    }

    desc_filters = {
        'Average Volume': 'Over 200K',
        'Float' : 'Under 50M',
        #'Asset Type':'Equities (Stocks)'
    }

    fund_filters = {
        'Average Volume': 'Over 200K',
        'Float': 'Under 50M',
        # 'Asset Type':'Equities (Stocks)'
        'EPS growththis year': 'Over 20%',
        'EPS growthnext year': 'Over 20%',
        'EPS growthqtr over qtr': 'Over 20%',
        'Sales growthqtr over qtr': 'Over 20%',
        'EPS growthpast 5 years': 'Over 20%',
        'Gross Margin': 'Positive (>0%)',
        'Return on Equity': 'Positive (>0%)',
        'InstitutionalOwnership': 'Over 20%'
    }

    filters_dict = price_filters
    filters_dict.update(desc_filters)
    filters_dict.update(fund_filters)

    return _run_screener(filters_dict)

def get_leaps():
    '''
    Descriptive Parameters:

        Market Cap: +Small (over $2bln)
        Average Volume: Over 200K
        Price: Over $5
        With these descriptive parameters, I narrow the list to stocks that are above 300 million dollars in market cap and with at least 10 million dollars of daily average dollar volume. Stocks that pass these parameters are again more likely to be quality companies with some institutional sponsorship.

        Fundamental Parameters:

        EPS Growth This Year: Over 20%
        EPS Growth Next Year: Over 25%
        EPS Growth qtr over qtr: Over 20%
        Sales Growth qtr over qtr: Over 25%
        Return on Equity: Over 15%
        Gross Margin: Over 0%
        Institutional Sponsorship: Over 30%
        :return:
        '''

    price_filters =  {
        'Price': 'Over $5',

    }

    desc_filters = {
        'Average Volume': 'Over 200K',
        'Float' : 'Under 50M',
        #'Asset Type':'Equities (Stocks)'
    }

    fund_filters = {
        'EPS growththis year': 'Over 20%',
        'EPS growthnext year': 'Over 25%',
        'EPS growthqtr over qtr': 'Over 20%',
        'Sales growthqtr over qtr': 'Over 25%',
        'Gross Margin': 'Positive (>0%)',
        'Return on Equity': 'Over +15%',
        'InstitutionalOwnership': 'Over 20%'
    }

    filters_dict = price_filters
    filters_dict.update(desc_filters)
    filters_dict.update(fund_filters)

    return _run_screener(filters_dict)


def get_graham_defensive():
    pass







