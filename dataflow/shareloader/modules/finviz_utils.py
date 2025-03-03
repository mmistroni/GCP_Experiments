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

def get_graham_enterprise(fmpKey):
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


def get_graham_defensive(fmpKey):

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
                    'EPS growthpast5 years' : 'Positive(>0'
                    }
    data =  _run_screener(filters_dict)

    keys = [k for k in data[0].keys()] if data else []

    extra_keys = ['debtOverCapital', 'dividendPaid', 'epsGrowth',
                  'positiveEps' , 'priceToBookRatio']

    new_keys = keys +  extra_keys

    # Need to group all these params in 1-2 fmp calls

    '''

    new_data = []
    for finviz_dict in data:
        ticker = finviz_dict['Ticker']
        bench_data = get_balancesheet_benchmark(ticker, fmpKey)
        divi_data = get_dividend_paid(ticker, fmpKey)
        income_data = get_income_benchmark(ticker, fmpKey)
        financial_ratios_benchmark = get_financial_ratios_benchmark(ticker, fmpKey)
        if bench_data:
            finviz_dict.update(bench_data)
        if divi_data:
            finviz_dict.update(divi_data)
        if income_data:
            finviz_dict.update(income_data)
        if financial_ratios_benchmark:
            finviz_dict.update(financial_ratios_benchmark)

        out_dict = dict((k, v) for k, v in finviz_dict.items() if k in new_keys)
        new_data.append(out_dict)


    return new_data
    '''
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

    return _run_screener(filters_dict)

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

    return _run_screener(overnight_filter_dict)


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
                   'institutionalOwnershipPercentage': 'institutionalOwnershipPercentage < 0.6',#
                   'positiveEps' : 'positiveEps > 0',#
                   'peRatio' : 'peRatio <= 15',#
                   'priceToBookRatio' : 'priceToBookRatio < 1.5', #
                   }

    buffetts = _run_screener(filter_dict)
    return [d['Ticker'] for d in buffetts]
    
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











