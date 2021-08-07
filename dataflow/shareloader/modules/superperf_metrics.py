from datetime import datetime, date
import requests
import logging

def filter_latest(stmnts, end_date):
    res = [d for d in stmnts if datetime.strptime(d['date'].split()[0], '%Y-%m-%d').date() < end_date]
    print('Statements before {} are{}'.format(end_date, len(res)))
    return res[-1]


def get_financial_ratios(ticker, fmprepkey, end_date):
    data = requests.get(
        'https://financialmodelingprep.com/api/v3/ratios/{}?period=quarter&limit=30&apikey={}'.format(ticker,
                                                                                                      fmprepkey)).json()
    data = filter_latest(data, end_date)
    key_to_extract = ['priceToSalesRatio', 'priceToEarningRatios', 'currentRatio'
                                                                   'quickRatio', 'returnOnEquity', 'returnOnAssets',
                      'returnOnCapitalEmployed', 'debtRatio',
                      'netProfitMargin']
    return dict((k, v) for k, v in data.items() if k in key_to_extract)


def get_key_metrics(ticker, fmprepkey, end_date):
    data = requests.get(
        'https://financialmodelingprep.com/api/v3/key-metrics/{}?period=quarter&limit=30&apikey={}'.format(
            ticker.upper(), fmprepkey)).json()
    data = filter_latest(data, end_date)
    key_to_extract = ['debtToEquity', 'debtToAssets', 'revenuePerShare', 'roic', 'marketCap']
    return dict((k, v) for k, v in data.items() if k in key_to_extract)


def get_income_statement(ticker, fmprepkey, end_date):
    income_stmt = requests.get(
        'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=30&apikey={apikey}'.format(
            ticker=ticker, apikey=fmprepkey)).json()

    data = filter_latest(income_stmt, end_date)
    key_to_extract = ['netIncomeRatio', 'ebitdaratio', 'eps', 'grossProfitRatio', 'operatingIncomeRatio']

    return dict((k, v) for k, v in data.items() if k in key_to_extract)


def get_dcf(ticker, end_dt):
    key = getfmpkeys()
    url = 'https://financialmodelingprep.com/api/v3/historical-discounted-cash-flow-statement/{}?apikey={}'.format(
        ticker.upper(), key)
    res = requests.get(url).json()
    data = [(d['date'], d['dcf']) for d in res if datetime.strptime(d['date'], '%Y-%m-%d').date() <= test_dt]

    try:
        return data[0][1] if data else None
    except Exception as e:
        logging.info('Error finding dfcf for {}@{}:{}'.format(ticker, end_dt, str(e)))


def get_fmprep_metrics(ticker, end_dt, key):
    income_dict = get_income_statement(ticker.upper(), key, end_dt)
    keymetr_dict = get_key_metrics(ticker.upper(), key, end_dt)
    income_dict.update(keymetr_dict)
    frdict = get_financial_ratios(ticker.upper(), key, end_dt)
    income_dict.update(frdict)
    return income_dict


def create_metrics(ticker, df_stock):
    metrics = {}
    metrics['ticker'] = ticker
    metrics['SMA_200'] = df_stock['SMA_200'][-1]
    metrics['SMA_150'] = df_stock['SMA_150'][-1]
    metrics['SMA_50'] = df_stock['SMA_50'][-1]
    metrics['SMA_200_1mago'] = df_stock['SMA_200'][-30]
    metrics['SMA_150_1mago'] = df_stock['SMA_150'][-30]
    metrics['SMA_200_2mago'] = df_stock['SMA_200'][-60]
    metrics['SMA_150_2mago'] = df_stock['SMA_150'][-60]
    metrics['LOW_52'] = df_stock['Close'][-252:].min()
    metrics['HIGH_52'] = df_stock['Close'][-252:].max()
    metrics['price'] = df_stock['Close'][-1]
    # Current Price is at least 30% above 52 week low (1.3*low_of_52week)
    metrics['Above_30_low'] = metrics['LOW_52'] * 1.3
    metrics['Within_25_high'] = metrics['HIGH_52'] * .7
    metrics['Price_above_sma200_sma150'] = (metrics['price'] > metrics['SMA_200']) & (
                metrics['price'] > metrics['SMA_150'])
    metrics['sma150_above_sma200'] = metrics['SMA_150'] > metrics['SMA_200']
    # 3 The 200-day moving average line is trending up for 1 month
    metrics['sma200_above_sma200_1mago'] = metrics['SMA_200'] > metrics['SMA_200_1mago']
    metrics['sma50_above_sma200_sma150'] = (metrics['SMA_50'] > metrics['SMA_200']) & (
                metrics['SMA_50'] > metrics['SMA_150'])
    metrics['price_above_sma50'] = metrics['price'] > metrics['SMA_50']
    # 6 The current stock price is at least 30 percent above its 52-week low
    metrics['price_above_30_low'] = metrics['price'] > metrics['Above_30_low']
    # 7 The current stock price is within at least 25 percent of its 52-week high.
    metrics['price_within_25high'] = metrics['price'] > metrics['Within_25_high']
    return metrics


def is_superperformer(stock_metrics):
    conditions = [v for k, v in stock_metrics.items() if k in ['Price_above_sma200_sma150', 'sma150_above_sma200',
                                                               'sma200_above_sma200_1mago', 'sma50_above_sma200_sma150',
                                                               'price_above_sma50', 'price_above_30_low',
                                                               'price_within_25high']]
    return all(conditions)


def get_historical_data_yahoo(symbol, start_dt, end_dt):
    try:
        end_date = date.today()
        data = dr.get_data_yahoo(symbol, start_dt, end_dt)[['Close']]
        data['symbol'] = symbol
        return data
    except Exception as e:
        return None


def get_historical_data_yahoo_close(symbol, start_dt, end_dt):
    try:
        end_date = date.today()
        data = dr.get_data_yahoo(symbol, start_dt, end_dt)[['Close']]
        return data.Close.values[0]

    except Exception as e:
        return None

    # Moving Averages


def add_sma_ratios(all_data):
    all_data['SMA_50'] = all_data.groupby('symbol')['Close'].transform(lambda x: x.rolling(window=50).mean())
    all_data['SMA_150'] = all_data.groupby('symbol')['Close'].transform(lambda x: x.rolling(window=150).mean())
    all_data['SMA_200'] = all_data.groupby('symbol')['Close'].transform(lambda x: x.rolling(window=200).mean())
    return all_data


def find_high_and_low(all_data):
    all_data['HIGH_52'] = all_data['Close'].rolling(min_periods=1, window=252, center=False).max()
    all_data['LOW_52'] = all_data['Close'].rolling(min_periods=1, window=252, center=False).min()
    all_data['Above_30_low'] = all_data['LOW_52'] * 1.3
    all_data['Within_25_high'] = all_data['HIGH_52'] * .7
    all_data['SMA_200_1mago'] = all_data['SMA_200'].shift(30)
    all_data['SMA_150_1mago'] = all_data['SMA_150'].shift(30)
    all_data['SMA_200_2mago'] = all_data['SMA_200'].shift(60)
    all_data['SMA_150_2mago'] = all_data['SMA_150'].shift(60)
    return all_data


#def get_all_tickers():
#    read_from_bucket('outputs/all_sectors_and_industries.csv-00000-of-00001', 'mm_dataflow_bucket', to_panda=True)
#    df = pd.read_csv('data/all_sectors_and_industries.csv-00000-of-00001')
#    return df


def get_all_stock_metrics(all_data):
    smas = add_sma_ratios(all_data)
    return find_high_and_low(smas)


def get_historical_data_yahoo(symbol, start_dt, end_dt):
    try:
        end_date = date.today()
        data = dr.get_data_yahoo(symbol, start_dt, end_dt)[['Close']]
        data['symbol'] = symbol
        return data
    except Exception as e:
        return None



