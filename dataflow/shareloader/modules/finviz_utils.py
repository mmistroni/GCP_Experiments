## finviz utilities
#https://pypi.org/project/finvizfinance/
# https://finvizfinance.readthedocs.io/en/latest/
#https://medium.com/the-investors-handbook/the-best-finviz-screens-for-growth-investors-72795f507b91

#https://www.justetf.com/uk/etf-profile.html?isin=IE000NDWFGA5
#https://www.justetf.com/uk/etf-profile.html?isin=IE000M7V94E1#chart URANIUM ETF
import apache_beam as beam
from finvizfinance.screener.overview import Overview
from .superperf_metrics import  load_bennchmark_data
import numpy as np
import logging
from datetime import date
import requests
import asyncio



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
    from numpy import nan
    foverview = Overview()
    foverview.set_filter(filters_dict=filters)
    df = foverview.screener_view()
    if df is not None and df.shape[0] > 0:
        return df.convert_dtypes().replace({nan: None}).to_dict(orient="records")
    return []
    #return df.to_dict('records') if df is not None else []

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
    
    res = _run_screener(filter)
    logging.info(f'Found  {len(res)}')

    return res

def  get_canslim():
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
    }

    fund_filters = {
        'Average Volume': 'Over 200K',
        'Float': 'Under 50M',
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

def get_graham_enterprise():
    '''
        Current superperf filter
        return (input_dict['currentRatio'] >= 1.5) \
                   and (input_dict['enterpriseDebt'] <= 1.2) \
                   and (input_dict['dividendPaidEnterprise'] == True)\
                   and (input_dict['epsGrowth5yrs'] > 0) \
                   and (input_dict['positiveEpsLast5Yrs'] == 5   ) \
                   and (input_dict['peRatio'] > 0) and  (input_dict['peRatio'] <= 10) \
                   and (input_dict['priceToBookRatio'] > 0) and (input_dict['priceToBookRatio'] < 1.5) \
                   and (input_dict['institutionalOwnershipPercentage'] < 0.6)
    '''''
    filters_dict = {'Market Cap.': '+Mid (over $2bln)',
                    'Current Ratio': 'Over 1',
                    'Debt/Equity': 'Under 1',
                    'P/E': 'Under 10',
                    'InstitutionalOwnership': 'Under 60%'
                    }

    return _run_screener(filters_dict)

def get_magic_formula(fmpKey):
    # Greenblatt magic formula plus institutional ownership
    # Div Yield > 0 and ROC  > 0
    # we take the stock universe filter, which skims the best, and apply roc > 0 and div yield > 0
    universe_filter = get_universe_filter()

    universe_filter['Dividend Yield'] = 'High (>5%)'

    # From this we need FMP to filter out
    return _run_screener(universe_filter)


def get_graham_defensive():

    # https://groww.in/blog/benjamin-grahams-7-stock-criteria
    # at least MidCap  finviz
    # Current Ratio > 2 finviz
    # Profitable earnings in last 10 years
    # Consistent Dividend Payment over last 20 years dividend
    # > 33% increase in earnings over last 10 years income
    # Price To Book < 11.5  financial ratio
    # PE Ratio < 15  finviz
    ''' our current criteria from superperfs are

    filters = { 'marketCap' : 'marketCap > 2000000000',
                 'currentRatio' : 'currentRatio >= 2',
                  'debtOverCapital' : 'debtOverCapital < 0',
                  'dividendPaid' : 'dividendPaid == True',
                  'epsGrowth' : 'epsGrowth >= 0.33',
                   'positiveEps' : 'positiveEps > 0',
                   'peRatio' : 'peRatio <= 15',
                   'priceToBookRatio' : 'priceToBookRatio < 1.5',
                   'institutionalOwnershipPercentage': 'institutionalOwnershipPercentage < 0.6'}
    '''

    filters_dict = {'Market Cap.': '+Mid (over $2bln)',
                    'Current Ratio' : 'Over 2',
                    'Debt/Equity': 'Under 1',
                    'P/E': 'Low (<15)',
                    'InstitutionalOwnership': 'Under 60%',
                    'EPS growthpast 5 years' : 'Positive (>0%)'
                    }
    return  _run_screener(filters_dict)


def get_extra_watchlist():
    '''
    Descriptive Parameters:
            Market Cap: +Mid (over $2bln)
            Average Volume: Over 200K
            Price: Over $10
            Stocks only (ex-Funds)
            With these descriptive parameters, I narrow the list to stocks that are relatively large companies with a respectable amount of liquidity. Stocks that pass these parameters are more likely to be quality companies.

            Fundamental Parameters:

            EPS Growth This Year: Over 20%
            EPS Growth Next Year: Over 20%
            EPS Growth qtr over qtr: Over 20%
            Sales Growth qtr over qtr: Over 20%
            Return on Equity: Over 20%
            Gross Margin: Over 20%
            With this scan, I keep the fundamental parameters to show only the fastest-growing companies with the best financial numbers. Stocks with greater than 20% revenue and earnings growth can lead to explosive movements in price as large institutions rapidly take positions in them. High return on equity and gross margin also show that the company is doing a great job managing the growth in revenue and sales.

            Technical Parameters:

            Price above SMA20
            Price above SMA50
            Price above SMA200

    :return:
    '''

    price_filters = {
        'Price': 'Over $10',
        '20-Day Simple Moving Average': 'Price above SMA20',
        '50-Day Simple Moving Average': 'Price above SMA50',
        '200-Day Simple Moving Average': 'Price above SMA200',
    }

    desc_filters = {
        'Market Cap.': '+Mid (over $2bln)',
        'Average Volume': 'Over 200K',
        #'Float': 'Under 50M',
        # 'Asset Type':'Equities (Stocks)'
    }

    fund_filters = {
        'EPS growththis year': 'Over 20%',
        'EPS growthnext year': 'Over 20%',
        'EPS growthqtr over qtr': 'Over 20%',
        'Sales growthqtr over qtr': 'Over 20%',
        'Gross Margin': 'Over 20%',
        'Return on Equity': 'Over +20%',
        'InstitutionalOwnership': 'Under 60%'
    }

    filters_dict = price_filters
    filters_dict.update(desc_filters)
    filters_dict.update(fund_filters)

    extras  =  _run_screener(filters_dict)
    logging.info(f'Fromo extra search we found:{len(extras)}')
    return extras

def get_peter_lynch():
    '''
    Market cap >Rs 500 crore 100m
    Debt-to-equity 1 for financial prudence
    ROE >15 per cent for capital efficiency
    Five-year EPS growth between 15-30 per cent
    Institutional holding 30 per cent to spot off-the-radar stocks
    P/E 15
    Inventory growth 1.2 times revenue growth for operational discipline
    '''
    filters_dict = {'Market Cap.':'+Small (over $300mln)',
                    'Average Volume' : 'Over 200K',
                    'Price' : 'Over $10',
                    'Current Ratio': 'Over 1',
                    'Debt/Equity': 'Under 1',
                    'P/E': 'Low (<15)',
                    'Return on Equity': 'Over +20%',
                    'EPS growthpast 5 years' : 'Over 20%'

                    }
  
    return _run_screener(filters_dict)



def get_new_highs():
    # Categories > Money, Banking, & Finance > Interest Rates > Corporate Bonds
    # https://fred.stlouisfed.org/series/BAMLH0A0HYM2
    '''

    Descriptive Parameters:

Market Cap: +Small (over $300mln)
Average Volume: Over 200K
Relative Volume: Over 1
Price: Over $10

Fundamental Parameters:

EPS Growth This Year: Positive (>0%)
EPS Growth Next Year: Positive (>0%)
EPS Growth qtr over qtr: Positive (>0%)
Sales Growth qtr over qtr: Positive (>0%)
Return on Equity: Positive (>0%)

Today Up
Price above SMA20
Price above SMA50
Price above SMA200
Change: Up
Change from Open: Up
52-Week High/Low: New High

    :return:
    '''

    desc_filters = {
        'Market Cap.': '+Small (over $300mln)',
        'Average Volume': 'Over 200K',
        'Relative Volume': 'Over 1',
    }
    fund_filters = {
        'EPS growththis year': 'Positive (>0%)',
        'EPS growthnext year': 'Positive (>0%)',
        'EPS growthqtr over qtr': 'Positive (>0%)',
        'Sales growthqtr over qtr': 'Positive (>0%)',
        'Return on Equity': 'Positive (>0%)',
        'InstitutionalOwnership': 'Under 60%'
    }

    price_filters = {
        'Price': 'Over $10',
        '20-Day Simple Moving Average': 'Price above SMA20',
        '50-Day Simple Moving Average': 'Price above SMA50',
        '200-Day Simple Moving Average': 'Price above SMA200',
        'Change' : 'Up',
        'Change from Open' : 'Up',
        '52-Week High/Low' : 'New High'
    }

    filters_dict = price_filters
    filters_dict.update(desc_filters)
    filters_dict.update(fund_filters)

    dt = _run_screener(filters_dict)
    logging.info(f' New high: {len(dt)}')
    return dt
def get_high_low():
    high_filter = 'New High'
    low_filter = 'New Low'

    high_filter_dict = {'52-Week High/Low' : high_filter}
    low_filter_dict = {'52-Week High/Low' : low_filter}

    highs = _run_screener(high_filter_dict)
    high_ticks = ','.join([d['Ticker'] for d in highs])
    lows = _run_screener(low_filter_dict)
    low_ticks = ','.join([d['Ticker'] for d in lows])
    return {'VALUE' : len(highs) - len(lows), 'NEW_HIGH' : len(highs), 'NEW_LOW' : len(lows),
            'HIGH_TICKERS' : high_ticks, 'LOW_TICKERS' : low_ticks}

def get_finviz_marketdown():
    # Categories > Money, Banking, & Finance > Interest Rates > Corporate Bonds
    # https://fred.stlouisfed.org/series/BAMLH0A0HYM2
    '''

    Descriptive Parameters:


    :return:
    '''

    desc_filters = {
        'Exchange' : 'NASDAQ',
        'Market Cap.': '+Mid (over $2bln)',
        'Option/Short': 'Optionable',
        'Short Float': 'High (>20%)',
    }
    fund_filters = {
        'EPS growthnext year': 'Negative (<0%)'
    }

    tech_filters = {
        'Performance': 'Quarter Down',
        'Performance 2': 'Month DownDown',
        '20-Day Simple Moving Average': 'Price below SMA20',
        
    }

    filters_dict = tech_filters
    filters_dict.update(desc_filters)
    filters_dict.update(fund_filters)

    dt = _run_screener(filters_dict)
    logging.info(f' Finviz Market Down: {len(dt)}')
    return dt



def overnight_return():
    '''
    Hello Marco,

    %change - %difference between the current close vs previous session's close
    %change_from_open - %difference between the current close vs today's open


    :return:
    '''
    # wontwo rk., let's instead use criteria for swign trading
    # revenue growing
    # eps growing
    # price  Over $5
    # Return on Asset Over +15%
    # Return On Equity Over +15%
    # Debt/Equity Under 0.5
    # EPS growth this year  Positive (>0%)
    # EPS growth qtr over qtr Positive (>0%)
    # Return on Investment Over +15%
    # EPS growth next year Positive (>0%)
     # Sales growth qtr over qtr Positive (>0%)
     # Current Ratio Over 1.5
     # Institutional Ownership  Under 60%
     #20-Day Simple Moving Average Price above SMA20
     #50-Day Simple Moving Average Price above SMA50
     #200-Day Simple Moving Average Price above SMA200
    # debt to equity  < 0.5
    # peRatio < 30
    # revenue growing
    # eps growing
    # price  Over $5
    # future growth
    fund_filters = {
        'EPS growththis year': 'Positive (>0%)',
        'EPS growthnext year': 'Positive (>0%)',
        'EPS growthqtr over qtr': 'Positive (>0%)',
        'Sales growthqtr over qtr': 'Positive (>0%)',
        'Return on Equity': 'Over +15%',
        'InstitutionalOwnership': 'Under 60%',
        #'Current Ratio' :  'Over 1.5'
    }

    price_filters = {
        'Price': 'Over $10',
        '20-Day Simple Moving Average': 'Price above SMA20',
        '50-Day Simple Moving Average': 'Price above SMA50',
        '200-Day Simple Moving Average': 'Price above SMA200',
        'Change': 'Up',

    }
    overnight_filter_dict = fund_filters
    overnight_filter_dict.update(price_filters)

    res =  _run_screener(overnight_filter_dict)
    logging.info(f'Swing Trader finviz has {len(res)}')
    return res

class FinvizLoader(beam.DoFn):
    def __init__(self, key, runtype='all'):
        self.key = key
        self.runtype = runtype



    def _get_data(self, ticker, key, label, prevClose=False):
        try :
            result = load_bennchmark_data(ticker, key, prevClose=prevClose)
            if result:
                for k, v in result.items():
                    if isinstance(v, float):
                        if np.isnan(v) or np.isinf(v):
                            result[k] = 0
                result['label'] = label
                return result
            return []
        except Exception as e:
            logging.info(f'Failed to get data for {ticker}:{str(e)}')
            return []


    def get_adx_and_rsi(self, ticker):
        adx_url = f'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=adx&period=14&apikey={self.key}'
        rsi_url = f'https://financialmodelingprep.com/api/v3/technical_indicator/1day/{ticker}?type=rsi&period=10&apikey={self.key}'

        try:
            adx = requests.get(adx_url).json()
            latest = adx[0]
            rsi = requests.get(rsi_url).json()
            latest_rsi= rsi[0]
            return  {'ADX' : latest['adx'],'RSI' : latest_rsi['rsi']}
        except Exception as e:
            logging.info(f'Failed tor etrieve data for {ticker}:{str(e)}')
            return {'ADX': 0, 'RSI': 0}




    def run_premarket(self):
        holder = []
        overnight_watchlist = [(d['Ticker'], d['Country']) for d in overnight_return()]
        for ticker, country in overnight_watchlist:
            logging.info(f' Checkiing {ticker}@{country}')
            try:
                data = self._get_data(ticker, self.key, 'OVERNIGHT_RETURN', prevClose=True)
                logging.info(f'Obtianed:{len(data)} itesm')
                if data and country == 'USA':
                    data['country'] = country

                    #rsi = self.get_adx_and_rsi(ticker)
                    # we are only interested in selected fields
                    interested_fields =  ['symbol', 'price', 'open', "previousClose", 'change', 'exchange' , 'country']
                    reduced_dict = dict((k, data.get(k)) for k in interested_fields )
                    reduced_dict['asodate'] = date.today()
                    reduced_dict['marketCap'] = float(data['marketCap'])
                    #reduced_dict.update(rsi)
                    holder.append(reduced_dict)
            except Exception as e:
                logging.info(f'Unable to get data for {ticker}:{str(e)}')

        return holder

    def process(self, elements):
        holder = []
        logging.info('Getting XXXLEAPS')
        leaps_tickers = [d['Ticker'] for d in get_leaps()]
        logging.info(f'Running nwtih mode {self.runtype}')
        if self.runtype == 'premarket':
            return self.run_premarket()
        else:
            try:
                for ticker in leaps_tickers:
                    data = self._get_data(ticker, self.key, 'LEAPS')
                    if data:
                        holder.append(data)
                logging.info('Getting CANSLIM')
                canslim_tickers = [d['Ticker'] for d in get_canslim()]
                for ticker in canslim_tickers:
                    data = self._get_data(ticker, self.key, 'CANSLIM')
                    if data:
                        holder.append(data)

                logging.info('Getting newhigh')

                newhighm_tickers = [d['Ticker'] for d in get_new_highs()]
                for ticker in newhighm_tickers:
                    data = self._get_data(ticker, self.key, 'NEWHIGH')
                    if data:
                        holder.append(data)

                extra_watchlist = [d['Ticker'] for d in get_extra_watchlist()]
                for ticker in extra_watchlist:
                    data = self._get_data(ticker, self.key, 'EXTRA_WATCHLIST')
                    if data:
                        holder.append(data)

                overnight_watchlist = [(d['Ticker'], d['Country']) for d in overnight_return()]
                for ticker, country in overnight_watchlist:
                    data = self._get_data(ticker, self.key, 'OVERNIGHT_RETURN')
                    data['country'] = country
                    if data:
                        holder.append(data)

                return holder
            except Exception as e :
                logging.info(f'Exception in running:{str(e)}')


def get_advance_decline2(exchange):
    try:
        up_filter = 'Up'
        down_filter = 'Down'

        high_filter_dict = {'Change' : up_filter,
                            'Exchange' :exchange}
        low_filter_dict = {'Change' : down_filter,
                        'Exchange' : exchange}

        highs = get_finviz_obb_data({}, high_filter_dict)
        high_ticks = ','.join([d['symbol'] for d in highs])
        lows = get_finviz_obb_data({}, low_filter_dict)
        low_ticks = ','.join([d['symbol'] for d in lows])
        logging.info(f' adv declie for {exchange} successfully retrieved')
        return {'VALUE' : str(len(highs) / len(lows)), 'ADVANCE' : len(highs), 'DECLINE' : len(lows),
                'ADVANCING_TICKERS' : high_ticks, 'DECLINING_TICKERS' : low_ticks}
    except Exception as e :
        logging.info('Exception in getting advv delcine:{str(e)}')
        return {'VALUE' : 'N/A', 'ADVANCE' : 'N/A', 'DECLINE' : 'N/A',
                'ADVANCING_TICKERS' : 'N/A', 'DECLINING_TICKERS' : 'N/A'}


def get_advance_decline_sma(exchange, numDays):
    try:
        up_filter = f'Price above SMA{numDays}'
        down_filter = f'Price below SMA{numDays}'
        key =  f'{numDays}-Day Simple Moving Average'
        
        high_filter_dict = { key : up_filter,
                            'Exchange' :exchange}
        low_filter_dict = {key : down_filter,
                        'Exchange' : exchange}

        logging.info(f'Querying wtih {high_filter_dict}')

        highs = _run_screener(high_filter_dict)
        high_ticks = ','.join([d['Ticker'] for d in highs])
        lows = _run_screener(low_filter_dict)
        low_ticks = ','.join([d['Ticker'] for d in lows])
        logging.info(f' adv declie for {exchange} successfully retrieved')
        return {'VALUE' : str(len(highs) / len(lows)), 'ADVANCE' : len(highs), 'DECLINE' : len(lows),
                'ADVANCING_TICKERS' : high_ticks, 'DECLINING_TICKERS' : low_ticks}
    except Exception as e :
        logging.info(f'Exception in getting advv delcine:{str(e)}')
        return {'VALUE' : f'N/A-{str(e)} ', 'ADVANCE' : 'N/A', 'DECLINE' : 'N/A',
                'ADVANCING_TICKERS' : 'N/A', 'DECLINING_TICKERS' : 'N/A'}

def get_advance_decline(exchange):
    try:
        up_filter = 'Up'
        down_filter = 'Down'

        high_filter_dict = {'Change' : up_filter,
                            'Exchange' :exchange}
        low_filter_dict = {'Change' : down_filter,
                        'Exchange' : exchange}

        highs = _run_screener(high_filter_dict)
        high_ticks = ','.join([d['Ticker'] for d in highs])
        lows = _run_screener(low_filter_dict)
        low_ticks = ','.join([d['Ticker'] for d in lows])
        logging.info(f' adv declie for {exchange} successfully retrieved')
        return {'VALUE' : str(len(highs) / len(lows)), 'ADVANCE' : len(highs), 'DECLINE' : len(lows),
                'ADVANCING_TICKERS' : high_ticks, 'DECLINING_TICKERS' : low_ticks}
    except Exception as e :
        logging.info('Exception in getting advv delcine:{str(e)}')
        return {'VALUE' : f'N/A {str(e)}', 'ADVANCE' : 'N/A', 'DECLINE' : 'N/A',
                'ADVANCING_TICKERS' : 'N/A', 'DECLINING_TICKERS' : 'N/A'}




def get_buffett_six():
    filter_dict = {
                   'Return on Assets' : 'Over +10%',#
                   'Return on Equity' : 'Over +10%',#
                   'Debt/Equity' : 'Under 0.5', #  debt over capital
                   'Current Ratio' : 'Over 1.5',#
                   'Average Volume' : 'Over 100K',
                   'Price': 'Over $10',#
                   'InstitutionalOwnership': 'Under 60%',#
                   'EPS growthpast 5 years': 'Positive (>0%)',
                   'P/E': 'Low (<15)',
                   'P/B' : 'Under 2', #
                   }

    return _run_screener(filter_dict)

def get_swing_trader_growth():
    pass


def get_swing_trader_value():
    pass

def get_swing_trader_technical():
    pass


class AsyncProcessFinviz(beam.DoFn):

    def __init__(self, high_filter, low_filter):
        self.fetcher = None
        self.high_filter = high_filter
        self.low_filter = low_filter

    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')
        try:
            high = await self.fetcher.fetch_data(self.high_filter, {})
            high_result = [d.model_dump(exclude_none=True) for d in high]

            low = await self.fetcher.fetch_data(self.low_filter, {})
            low_result = [d.model_dump(exclude_none=True) for d in low]

            if high_result and low_result:
                high_ticks = ','.join([d['symbol'] for d in high_result])
                low_ticks = ','.join([d['symbol'] for d in low_result])
                logging.info(f' adv declie for  successfully retrieved')
                return [{'VALUE': str(len(high_result) / len(low_result)), 'ADVANCE': len(high_result), 'DECLINE': len(low_result),
                        'ADVANCING_TICKERS': high_ticks, 'DECLINING_TICKERS': low_ticks}]
            else:
                return[{}]
        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return  [str(e)]

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))



def get_finviz_obb_data(creds, params):

    async def fetch_data(fetcher, creds : dict, params :dict) -> list[dict]:
        try:
            # 1. We need to get the close price of the day by just querying for 1d interval
            # 2. then we get the pre-post market. group by day and get latest of yesterday and latest of
            #    today
            # 3. we aggregate and store in bq
            # 4 .send email for everything that increased over 10% overnight
            # 5 . also restrict only for US. drop every ticker which has a .<Exchange>
            logging.info('Attempting to fetch OBB FINVIZZ......')
            data = await fetcher.fetch_data(params, creds)
            return [d.model_dump(exclude_none=True) for d in data]
        except Exception as e:
            logging.info(f'Failed to fetch data for {params}:{str(e)}')
            return []

    with asyncio.Runner() as runner:
        return runner.run(fetch_data(None, creds, params))


def get_eod_screener():
    '''
    Hello Marco,

    %change - %difference between the current close vs previous session's close
    %change_from_open - %difference between the current close vs today's open


    :return:
    '''
    # wontwo rk., let's instead use criteria for swign trading
    # tryinbg some other criterias
    descriptive_filters = {
        'Price' : 'Over $10',
        'Average Volume' : 'Over 200K',
        'Relative Volume' : 'Over 1.5'
    }

    fund_filters = {
        'EPS growththis year': 'Positive (>0%)',
        'EPS growthnext year': 'Positive (>0%)',
        'EPS growthqtr over qtr': 'Positive (>0%)',
        'Sales growthqtr over qtr': 'Positive (>0%)',
        'Return on Equity': 'Positive (>0%)',
        'InstitutionalOwnership': 'Under 60%',
        'Debt/Equity' : 'Under 1',
        'Current Ratio' :  'Over 1'
    }

    price_filters = {
        'Performance': 'Month Up',
        #'Performance2': 'Week Up',
        '20-Day Simple Moving Average': 'Price above SMA20',
        '50-Day Simple Moving Average': 'Price above SMA50',
        '200-Day Simple Moving Average': 'Price above SMA200',
        '52-Week High/Low' : '5% or more below High'
    }
    eod_filter_dict = fund_filters
    eod_filter_dict.update(descriptive_filters)
    eod_filter_dict.update(price_filters)

    return _run_screener(eod_filter_dict)

def get_companies_for_industry(industry : str) -> list:
    descriptive_filters = {
        'Industry': industry,
        'Average Volume': 'Over 200K',
        'Relative Volume': 'Over 1',
        'Country' : 'USA'
    }

    perf_filters = {
        'Performance': 'Quarter Up',
        'Performance 2' : 'Month Up',
        'Relative Volume': 'Over 1.5'
    }

    price_filters = {
        '20-Day Simple Moving Average': 'Price above SMA20',
        '50-Day Simple Moving Average': 'Price above SMA50',
        '200-Day Simple Moving Average': 'Price above SMA200',
        #'52-Week High/Low': '5% or more below High'
    }

    full_filter = descriptive_filters
    full_filter.update(perf_filters)
    full_filter.update(price_filters)

    return _run_screener(full_filter)



def get_gics_to_finviz_mappings():
    ## extracted from llm
    # Helper dictionary to map GICS Industry/Sub-Industry to its top-level GICS Sector
    # This is derived from the standard GICS hierarchy and the mappings in finviz_to_gics_mapping.
    # This dictionary is used as the source for the new mapping.
    # Re-including the finviz_to_gics_mapping for completeness within this script,
    # as it's the source for the Finviz industries and their GICS industry/sub-industry equivalents.
    finviz_to_gics_mapping = {
        # Communication Services Sector
        'Advertising Agencies': 'Advertising',
        'Broadcasting': 'Broadcasting',
        'Entertainment': 'Entertainment',
        'Internet Content & Information': 'Internet Services & Infrastructure',  # Closest GICS Sub-Industry
        'Publishing': 'Publishing',
        'Telecom Services': 'Integrated Telecommunication Services',  # GICS Industry Group: Telecommunication Services

        # Consumer Discretionary Sector
        'Apparel Retail': 'Apparel Retail',
        'Auto & Truck Dealerships': 'Automotive Retail',
        'Auto Manufacturers': 'Automobiles',
        'Auto Parts': 'Automotive Parts & Equipment',
        'Consumer Electronics': 'Consumer Electronics',
        'Department Stores': 'Department Stores',
        'Discount Stores': 'Discount Stores',
        # GICS Sub-Industry: Discount Stores & Supercenters (often grouped with Food & Staples Retail)
        'Electronic Gaming & Multimedia': 'Interactive Home Entertainment',  # GICS Sub-Industry
        'Footwear & Accessories': 'Apparel, Accessories & Luxury Goods',  # GICS Industry
        'Furnishings, Fixtures & Appliances': 'Home Furnishings',  # GICS Industry
        'Gambling': 'Casinos & Gaming',  # GICS Sub-Industry
        'Home Improvement Retail': 'Home Improvement Retail',
        'Internet Retail': 'Internet & Direct Marketing Retail',
        'Leisure': 'Leisure Facilities',  # GICS Sub-Industry
        'Lodging': 'Hotels, Resorts & Cruise Lines',  # GICS Industry
        'Luxury Goods': 'Apparel, Accessories & Luxury Goods',  # GICS Industry
        'Recreational Vehicles': 'Automobiles',  # GICS Industry: Recreational Vehicles are part of Automobiles
        'Resorts & Casinos': 'Casinos & Gaming',  # GICS Sub-Industry
        'Restaurants': 'Restaurants',
        'Specialty Retail': 'Specialty Retail',  # GICS Sub-Industry

        # Consumer Staples Sector
        'Beverages - Brewers': 'Brewers',
        'Beverages - Non-Alcoholic': 'Soft Drinks',
        'Beverages - Wineries & Distilleries': 'Distillers & Vintners',
        'Confectioners': 'Packaged Foods & Meats',  # GICS Industry
        'Food Distribution': 'Food Distributors',
        'Grocery Stores': 'Hypermarkets & Super Centers',  # Closest GICS Sub-Industry
        'Household & Personal Products': 'Household Products',  # GICS Industry Group: Household & Personal Products
        'Packaged Foods': 'Packaged Foods & Meats',
        'Tobacco': 'Tobacco',

        # Energy Sector
        'Oil & Gas Drilling': 'Oil & Gas Drilling',
        'Oil & Gas E&P': 'Oil & Gas Exploration & Production',
        'Oil & Gas Equipment & Services': 'Oil & Gas Equipment & Services',
        'Oil & Gas Integrated': 'Integrated Oil & Gas',
        'Oil & Gas Midstream': 'Oil & Gas Storage & Transportation',  # GICS Sub-Industry
        'Oil & Gas Refining & Marketing': 'Oil & Gas Refining & Marketing',
        'Thermal Coal': 'Coal & Consumable Fuels',  # GICS Industry

        # Financials Sector
        'Asset Management': 'Asset Management & Custody Banks',
        'Banks - Diversified': 'Diversified Banks',
        'Banks - Regional': 'Regional Banks',
        'Capital Markets': 'Investment Banking & Brokerage',  # GICS Industry Group: Capital Markets
        'Credit Services': 'Consumer Finance',  # GICS Sub-Industry
        'Financial Conglomerates': 'Diversified Financial Services',  # GICS Industry
        'Financial Data & Stock Exchanges': 'Financial Exchanges & Data',
        'Insurance - Diversified': 'Diversified Insurance',
        'Insurance - Life': 'Life & Health Insurance',
        'Insurance - Property & Casualty': 'Property & Casualty Insurance',
        'Insurance - Reinsurance': 'Reinsurance',
        'Insurance - Specialty': 'Specialty Insurance',
        'Insurance Brokers': 'Insurance Brokers',
        'Mortgage Finance': 'Mortgage Finance',

        # Health Care Sector
        'Biotechnology': 'Biotechnology',
        'Diagnostics & Research': 'Life Sciences Tools & Services',  # GICS Industry
        'Drug Manufacturers - General': 'Pharmaceuticals',  # GICS Industry
        'Drug Manufacturers - Specialty & Generic': 'Pharmaceuticals',  # GICS Industry
        'Health Information Services': 'Healthcare Technology',  # GICS Sub-Industry
        'Healthcare Plans': 'Managed Health Care',
        'Medical Care Facilities': 'Healthcare Facilities',
        'Medical Devices': 'Medical Devices',
        'Medical Distribution': 'Healthcare Distributors',
        'Medical Instruments & Supplies': 'Medical Supplies',  # GICS Sub-Industry

        # Industrials Sector
        'Aerospace & Defense': 'Aerospace & Defense',
        'Agricultural Inputs': 'Agricultural & Farm Machinery',  # GICS Industry
        'Airports & Air Services': 'Airport Services',  # GICS Sub-Industry
        'Building Products & Equipment': 'Building Products',  # GICS Industry
        'Business Equipment & Supplies': 'Office Services & Supplies',  # GICS Sub-Industry
        'Conglomerates': 'Industrial Conglomerates',
        'Consulting Services': 'Professional Services',  # GICS Industry
        'Education & Training Services': 'Education Services',  # GICS Sub-Industry
        'Electrical Equipment & Parts': 'Electrical Components & Equipment',  # GICS Industry
        'Engineering & Construction': 'Construction & Engineering',
        'Farm & Heavy Construction Machinery': 'Construction & Farm Machinery & Heavy Trucks',  # GICS Industry
        'Industrial Distribution': 'Trading Companies & Distributors',  # GICS Industry
        'Integrated Freight & Logistics': 'Air Freight & Logistics',  # GICS Industry
        'Marine Shipping': 'Marine Transportation',  # GICS Sub-Industry
        'Metal Fabrication': 'Industrial Machinery',  # GICS Industry (broadest fit)
        'Pollution & Treatment Controls': 'Environmental & Facilities Services',  # GICS Industry
        'Railroads': 'Railroads',
        'Rental & Leasing Services': 'Trading Companies & Distributors',  # GICS Industry
        'Security & Protection Services': 'Security & Alarm Services',  # GICS Sub-Industry
        'Specialty Business Services': 'Diversified Support Services',  # GICS Sub-Industry
        'Specialty Industrial Machinery': 'Industrial Machinery',
        'Staffing & Employment Services': 'Human Resource & Employment Services',  # GICS Sub-Industry
        'Tools & Accessories': 'Industrial Machinery',  # GICS Industry (broadest fit)
        'Travel Services': 'Travel Services',
        'Trucking': 'Trucking',
        'Waste Management': 'Environmental & Facilities Services',  # GICS Industry

        # Information Technology Sector
        'Communication Equipment': 'Communications Equipment',
        'Computer Hardware': 'Technology Hardware, Storage & Peripherals',  # GICS Industry
        'Electronic Components': 'Electronic Components',
        'Electronics & Computer Distribution': 'Electronic Components',  # Closest fit, often grouped here
        'Information Technology Services': 'IT Consulting & Other Services',
        'Semiconductor Equipment & Materials': 'Semiconductor Equipment',
        'Semiconductors': 'Semiconductors',
        'Software - Application': 'Application Software',
        'Software - Infrastructure': 'Systems Software',

        # Materials Sector
        'Aluminum': 'Aluminum',
        'Building Materials': 'Construction Materials',  # GICS Industry
        'Chemicals': 'Specialty Chemicals',  # GICS Industry (broadest fit)
        'Coking Coal': 'Coal & Consumable Fuels',  # GICS Industry
        'Copper': 'Copper',
        'Gold': 'Gold',
        'Lumber & Wood Production': 'Paper & Forest Products',  # GICS Industry
        'Other Industrial Metals & Mining': 'Diversified Metals & Mining',  # GICS Industry
        'Other Precious Metals & Mining': 'Precious Metals & Minerals',  # GICS Industry
        'Packaging & Containers': 'Paper Packaging & Forest Products',  # GICS Industry
        'Paper & Paper Products': 'Paper & Forest Products',  # GICS Industry
        'Silver': 'Silver',
        'Steel': 'Steel',
        'Textile Manufacturing': 'Textiles',  # GICS Sub-Industry
        'Uranium': 'Diversified Metals & Mining',  # GICS Industry (often grouped here)

        # Real Estate Sector
        'Real Estate - Development': 'Real Estate Development',
        'Real Estate - Diversified': 'Diversified Real Estate',
        'Real Estate Services': 'Real Estate Services',
        'Residential Construction': 'Homebuilding',  # GICS Industry
        'REIT - Diversified': 'Diversified REITs',
        'REIT - Healthcare Facilities': 'Healthcare REITs',
        'REIT - Hotel & Motel': 'Hotel & Resort REITs',
        'REIT - Industrial': 'Industrial REITs',
        'REIT - Mortgage': 'Mortgage REITs',
        'REIT - Office': 'Office REITs',
        'REIT - Residential': 'Residential REITs',
        'REIT - Retail': 'Retail REITs',
        'REIT - Specialty': 'Specialty REITs',

        # Utilities Sector
        'Solar': 'Renewable Electricity',  # GICS Sub-Industry
        'Utilities - Diversified': 'Multi-Utilities',
        'Utilities - Independent Power Producers': 'Independent Power Producers & Energy Traders',
        'Utilities - Regulated Electric': 'Electric Utilities',
        'Utilities - Regulated Gas': 'Gas Utilities',
        'Utilities - Regulated Water': 'Water Utilities',
        'Utilities - Renewable': 'Renewable Electricity',  # GICS Sub-Industry

        # Other/Unclassified (or specific GICS mapping)
        'Apparel Manufacturing': 'Apparel, Accessories & Luxury Goods',  # GICS Industry
        'Farm Products': 'Agricultural Products',  # GICS Sub-Industry
        'Personal Services': 'Diversified Consumer Services',  # GICS Industry Group
        'Scientific & Technical Instruments': 'Life Sciences Tools & Services',  # GICS Industry
        'Shell Companies': 'Multi-Sector Holdings',  # GICS Industry (closest fit for general shell companies)
    }

    # Helper dictionary to map GICS Industry/Sub-Industry to its top-level GICS Sector
    # This is derived from the standard GICS hierarchy.
    gics_industry_to_sector_helper = {
        'Advertising': 'Communication Services',
        'Broadcasting': 'Communication Services',
        'Entertainment': 'Communication Services',
        'Internet Services & Infrastructure': 'Communication Services',
        'Publishing': 'Communication Services',
        'Integrated Telecommunication Services': 'Communication Services',

        'Apparel Retail': 'Consumer Discretionary',
        'Automotive Retail': 'Consumer Discretionary',
        'Automobiles': 'Consumer Discretionary',
        'Automotive Parts & Equipment': 'Consumer Discretionary',
        'Consumer Electronics': 'Consumer Discretionary',
        'Department Stores': 'Consumer Discretionary',
        'Discount Stores': 'Consumer Discretionary',
        'Interactive Home Entertainment': 'Consumer Discretionary',
        'Apparel, Accessories & Luxury Goods': 'Consumer Discretionary',
        'Home Furnishings': 'Consumer Discretionary',
        'Casinos & Gaming': 'Consumer Discretionary',
        'Home Improvement Retail': 'Consumer Discretionary',
        'Internet & Direct Marketing Retail': 'Consumer Discretionary',
        'Leisure Facilities': 'Consumer Discretionary',
        'Hotels, Resorts & Cruise Lines': 'Consumer Discretionary',
        'Restaurants': 'Consumer Discretionary',
        'Specialty Retail': 'Consumer Discretionary',
        'Travel Services': 'Consumer Discretionary',

        'Brewers': 'Consumer Staples',
        'Soft Drinks': 'Consumer Staples',
        'Distillers & Vintners': 'Consumer Staples',
        'Packaged Foods & Meats': 'Consumer Staples',
        'Food Distributors': 'Consumer Staples',
        'Hypermarkets & Super Centers': 'Consumer Staples',
        'Household Products': 'Consumer Staples',
        'Tobacco': 'Consumer Staples',
        'Agricultural Products': 'Consumer Staples',

        'Oil & Gas Drilling': 'Energy',
        'Oil & Gas Exploration & Production': 'Energy',
        'Oil & Gas Equipment & Services': 'Energy',
        'Integrated Oil & Gas': 'Energy',
        'Oil & Gas Storage & Transportation': 'Energy',
        'Oil & Gas Refining & Marketing': 'Energy',
        'Coal & Consumable Fuels': 'Energy',

        'Asset Management & Custody Banks': 'Financials',
        'Diversified Banks': 'Financials',
        'Regional Banks': 'Financials',
        'Investment Banking & Brokerage': 'Financials',
        'Consumer Finance': 'Financials',
        'Diversified Financial Services': 'Financials',
        'Financial Exchanges & Data': 'Financials',
        'Diversified Insurance': 'Financials',
        'Life & Health Insurance': 'Financials',
        'Property & Casualty Insurance': 'Financials',
        'Reinsurance': 'Financials',
        'Specialty Insurance': 'Financials',
        'Insurance Brokers': 'Financials',
        'Mortgage Finance': 'Financials',

        'Biotechnology': 'Health Care',
        'Life Sciences Tools & Services': 'Health Care',
        'Pharmaceuticals': 'Health Care',
        'Healthcare Technology': 'Health Care',
        'Managed Health Care': 'Health Care',
        'Healthcare Facilities': 'Health Care',
        'Medical Devices': 'Health Care',
        'Healthcare Distributors': 'Health Care',
        'Medical Supplies': 'Health Care',

        'Aerospace & Defense': 'Industrials',
        'Agricultural & Farm Machinery': 'Industrials',
        'Airport Services': 'Industrials',
        'Building Products': 'Industrials',
        'Office Services & Supplies': 'Industrials',
        'Industrial Conglomerates': 'Industrials',
        'Professional Services': 'Industrials',
        'Education Services': 'Industrials',
        'Electrical Components & Equipment': 'Industrials',
        'Construction & Engineering': 'Industrials',
        'Construction & Farm Machinery & Heavy Trucks': 'Industrials',
        'Trading Companies & Distributors': 'Industrials',
        'Air Freight & Logistics': 'Industrials',
        'Marine Transportation': 'Industrials',
        'Industrial Machinery': 'Industrials',
        'Environmental & Facilities Services': 'Industrials',
        'Railroads': 'Industrials',
        'Security & Alarm Services': 'Industrials',
        'Diversified Support Services': 'Industrials',
        'Human Resource & Employment Services': 'Industrials',
        'Trucking': 'Industrials',

        'Communications Equipment': 'Information Technology',
        'Technology Hardware, Storage & Peripherals': 'Information Technology',
        'Electronic Components': 'Information Technology',
        'IT Consulting & Other Services': 'Information Technology',
        'Semiconductor Equipment': 'Information Technology',
        'Semiconductors': 'Information Technology',
        'Application Software': 'Information Technology',
        'Systems Software': 'Information Technology',

        'Aluminum': 'Materials',
        'Construction Materials': 'Materials',
        'Specialty Chemicals': 'Materials',
        'Copper': 'Materials',
        'Gold': 'Materials',
        'Paper & Forest Products': 'Materials',
        'Diversified Metals & Mining': 'Materials',
        'Precious Metals & Minerals': 'Materials',
        'Paper Packaging & Forest Products': 'Materials',
        'Silver': 'Materials',
        'Steel': 'Materials',
        'Textiles': 'Materials',

        'Real Estate Development': 'Real Estate',
        'Diversified Real Estate': 'Real Estate',
        'Real Estate Services': 'Real Estate',
        'Homebuilding': 'Real Estate',
        'Diversified REITs': 'Real Estate',
        'Healthcare REITs': 'Real Estate',
        'Hotel & Resort REITs': 'Real Estate',
        'Industrial REITs': 'Real Estate',
        'Mortgage REITs': 'Real Estate',
        'Office REITs': 'Real Estate',
        'Residential REITs': 'Real Estate',
        'Retail REITs': 'Real Estate',
        'Specialty REITs': 'Real Estate',

        'Renewable Electricity': 'Utilities',
        'Multi-Utilities': 'Utilities',
        'Independent Power Producers & Energy Traders': 'Utilities',
        'Electric Utilities': 'Utilities',
        'Gas Utilities': 'Utilities',
        'Water Utilities': 'Utilities',
        'Environmental & Facilities Services': 'Utilities',
    }

    # Initialize the dictionary to store the mapping from GICS Sector to a list of Finviz Industries
    gics_sector_to_finviz_industries = {
        'Communication Services': [],
        'Consumer Discretionary': [],
        'Consumer Staples': [],
        'Energy': [],
        'Financials': [],
        'Health Care': [],
        'Industrials': [],
        'Information Technology': [],
        'Materials': [],
        'Real Estate': [],
        'Utilities': []
    }

    # Populate the dictionary
    for finviz_industry, gics_mapped_industry in finviz_to_gics_mapping.items():
        gics_sector = gics_industry_to_sector_helper.get(gics_mapped_industry)
        if gics_sector:
            gics_sector_to_finviz_industries[gics_sector].append(finviz_industry)
        else:
            print(
                f"Warning: GICS mapped industry '{gics_mapped_industry}' for Finviz industry '{finviz_industry}' does not have a corresponding GICS Sector in the helper dictionary.")

    # Sort the lists of industries within each sector for consistency
    for sector in gics_sector_to_finviz_industries:
        gics_sector_to_finviz_industries[sector].sort()

    return gics_sector_to_finviz_industries