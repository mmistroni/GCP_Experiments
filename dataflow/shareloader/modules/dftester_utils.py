import requests
import logging
from datetime import datetime
import apache_beam as beam
import json
import openai as openai
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference

def get_fields():
    return ["ticker", "date", "currentRatio", "quickRatio", "cashRatio", "calendarYear",
            "returnOnAssets", "returnOnEquity", "returnOnCapitalEmployed",
            "priceToBookRatio", "priceToSalesRatio",
            "priceEarningsRatio", "priceToFreeCashFlowsRatio",
            "priceEarningsToGrowthRatio", "dividendYield", "priceFairValue"
            ] + \
            [
                "revenuePerShare", "earningsYield", "debtToEquity",
                "debtToAssets", "capexToRevenue", "grahamNumber",
                "lynchRatio"
            ]


class DfTesterLoader(beam.DoFn):
    def __init__(self, key, period='annual', limit=10):
        self.key = key
        self.period = period
        self.limit = limit

    def to_list_of_vals(self, data_dict):
        return ','.join([str(data_dict[field]) for field in get_fields()])

    def process(self, elements):
        all_dt = []
        logging.info('fRunning with split of:{split}')
        tickers_to_process = elements.split(',')
        num_to_process = len(tickers_to_process) // 3

        excMsg = ''
        isException = False

        all_dt = []

        for idx, ticker in enumerate(tickers_to_process):
            try:
                data  =get_fundamental_data(ticker, self.key, self.period, self.limit)
                vals_only = [self.to_list_of_vals(d) for d in data]
                all_dt += vals_only
            except Exception as e:
                excMsg = f"{idx/len(tickers_to_process)}Failed to process fundamental loader for {ticker}:{str(e)}"
                isException = True
                break
        if isException:
            raise Exception(excMsg)
        return all_dt


def combine_tickers(input):
    return ','.join(input)

def calculate_peter_lynch_ratio(key, ticker, asOfDateStr, dataDict):
    if dataDict['dividendYield']  is None or  dataDict['priceEarningsRatio'] is None:
        return -99
    try:
        divYield = dataDict['dividendYield'] * 100
        peRatio = dataDict['priceEarningsRatio']
        cob = datetime.strptime(asOfDateStr, '%Y-%m-%d')
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

def get_financial_ratios(ticker, key, period='annual', limit=10):
    keys = ["currentRatio", "quickRatio", "cashRatio", "date", "calendarYear",
            "returnOnAssets", "returnOnEquity", "returnOnCapitalEmployed",
            "priceToBookRatio", "priceToSalesRatio",
            "priceEarningsRatio", "priceToFreeCashFlowsRatio",
            "priceEarningsToGrowthRatio", "dividendYield", "priceFairValue"
            ]
    global_dict = dict()
    try:
        financial_ratios = requests.get(
            f'https://financialmodelingprep.com/api/v3/ratios/{ticker}?period={period}&limit={limit}&apikey={key}').json()

        for data_dict in financial_ratios:
            tmp_dict = dict((k, data_dict.get(k, None)) for k in keys)
            global_dict[data_dict['date']] = tmp_dict
    except Exception as e:
        logging.info(f'Unable to get data for {ticker}:{str(e)}')
    return global_dict

def get_key_metrics(ticker, key, period='annual', limit=10):
    keys = [
            "date", "calendarYear", "revenuePerShare", "earningsYield", "debtToEquity",
            "debtToAssets", "capexToRevenue", "grahamNumber"
    ]

    globalDict = dict()

    try:
        keyMetrics = requests.get(
           f'https://financialmodelingprep.com/api/v3/key-metrics/{ticker}?period={period}&limit={limit}&apikey={key}').json()
        for dataDict in keyMetrics:
            tmpDict = dict((k, dataDict.get(k, None)) for k in keys)
            globalDict[dataDict['date']] = tmpDict

    except Exception as e:
        logging.info(f'Unable to get data for {ticker}:{str(e)}')

    return globalDict

def get_fundamental_data(ticker, key, period='annual', limit=10): # we do separately , for each ticker we get perhaps the last
    ## we get ROC, divYield and pl ratio. we need to get historicals on a quarterly and annually
    # then we build a dataframe with all the information
    metrics = get_key_metrics(ticker, key, period, limit)
    ratios = get_financial_ratios(ticker, key, period, limit)

    mergedDicts = []
    for asOfDate, metricsDict in metrics.items():
        ratiosDict = ratios.get(asOfDate)
        if ratiosDict:
            ratiosDict.update(metricsDict)
            peterLynch = calculate_peter_lynch_ratio(key, ticker, asOfDate, ratiosDict)
            ratiosDict['lynchRatio'] = peterLynch
            ratiosDict['ticker'] = ticker
            mergedDicts.append(ratiosDict)
    return mergedDicts

def get_tickers_for_sectors(sector, key):
    ''' Sectors
    Consumer Cyclical, Energy, Technology, Industrials, Financial Services,
    Basic Materials, Communication Services, Consumer Defensive,
    Healthcare, Real Estate, Utilities, Industrial Goods, Financial, Services, Conglomerates

    Industry
    '''

    url = f'https://financialmodelingprep.com/api/v3/stock-screener?sector={sector}&apikey={key}'
    return requests.get(url).json()

def get_tickers_for_industry(industry, key):
    ''' Sectors
    Consumer Cyclical, Energy, Technology, Industrials, Financial Services,
    Basic Materials, Communication Services, Consumer Defensive,
    Healthcare, Real Estate, Utilities, Industrial Goods, Financial, Services, Conglomerates

    Industry

    '''

    url = f'https://financialmodelingprep.com/api/v3/stock-screener?industry={industry}&apikey={key}'
    return requests.get(url).json()


def get_sectors():
    return ['Consumer Cyclical', 'Energy', 'Technology', 'Industrials',
            'Financial Services', 'Basic Materials', 'Communication Services',
            'Consumer Defensive', 'Healthcare', 'Real Estate', 'Utilities',
            'Industrial Goods', 'Financial', 'Services', 'Conglomerates']




def get_industries(key):
    inds = []
    for sector in get_sectors():
        companies = get_tickers_for_sectors(sector, key)
        sector_inds = [d['industry'] for d in companies]
        inds += sector_inds

    return set(inds)

def replace_dates_in_json(data, replacement_string="[REPLACED_DATE]"):
    """
    Recursively replaces all date-like strings in a JSON object with a specified string.

    This function attempts to parse each string value in the JSON object using common date formats.
    If a string matches a date format, it is replaced. This handles nested dictionaries
    and lists.

    Args:
        data (dict | list): The JSON object (dictionary or list) to process.
        replacement_string (str): The string to replace dates with.

    Returns:
        dict | list: The modified JSON object.
    """
    date_formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO 8601 with milliseconds
        '%Y-%m-%dT%H:%M:%S',     # ISO 8601 without milliseconds
        '%Y-%m-%d %H:%M:%S',     # Common database format
        '%Y-%m-%d',              # YYYY-MM-DD
        '%m/%d/%Y',              # MM/DD/YYYY
    ]

    if isinstance(data, dict):
        return {k: replace_dates_in_json(v, replacement_string) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_dates_in_json(item, replacement_string) for item in data]
    elif isinstance(data, str):
        for fmt in date_formats:
            try:
                datetime.strptime(data, fmt)
                return replacement_string
            except ValueError:
                continue
        # If no format matched, return the original string
        return data
    else:
        # Return non-string, non-dict, non-list values as-is
        return data


import json

def json_serializable_filter(obj):
    """
    Recursively filters a dictionary, list, or other object to remove
    any values that are not JSON-serializable.

    Args:
        obj: The object to filter.

    Returns:
        A new object with only JSON-serializable values.
    """
    if isinstance(obj, dict):
        return {k: json_serializable_filter(v) for k, v in obj.items() if is_json_serializable(v)}
    elif isinstance(obj, list):
        return [json_serializable_filter(item) for item in obj if is_json_serializable(item)]
    else:
        # If the object is not a dict or list, return it if it's serializable.
        return obj if is_json_serializable(obj) else None

def is_json_serializable(value):
    """
    Checks if a given value is JSON-serializable.

    Args:
        value: The value to check.

    Returns:
        True if the value can be serialized, False otherwise.
    """
    try:
        json.dumps(value)
        return True
    except (TypeError, OverflowError):
        return False


def to_json_string(element):
    def datetime_converter(o):
        if is_json_serializable(element):
            return element
        return str(element)
    jsonres =  json.dumps(element, default=datetime_converter)
    
    return jsonres

def extract_json_list(element):
    """
    This function attempts to extract a JSON list from a string.

    Args:
        element (str): The input string, which may or may not contain a JSON list.

    Returns:
        list:  A list extracted from the string, or an empty list if no valid
               JSON list is found.
    """
    try:
        # Attempt to parse the entire element as JSON.  This is the most
        # straightforward approach if the *entire* string is valid JSON.
        jsonstring = element[element.find('<JSONSTART>') + 11 : element.find('<JSONEND')]
        data = json.loads(jsonstring)
        logging.info('Extracted json string is:{data}')
        if isinstance(data, list):
            return data  # Return the list if the whole string is a list.
        else:
            return [] # if the whole string is not a list, return empty list

    except json.JSONDecodeError:
        # If the entire element is not valid JSON, try to find a JSON list *within* the string.
        try:
            start_index = element.find('[')
            end_index = element.rfind(']')
            if start_index != -1 and end_index != -1 and start_index < end_index:
                json_string = element[start_index:end_index + 1]
                data = json.loads(json_string)
                if isinstance(data, list):
                  return data
                else:
                   return []
            else:
                return []
        except json.JSONDecodeError:
            # If no valid JSON list is found, return an empty list.
            return []
    except TypeError:
        return []

class SampleOpenAIHandler(ModelHandler):
  """DoFn that accepts a batch of images as bytearray
  and sends that batch to the Cloud Vision API for remote inference"""
  def __init__(self, oai_key, llm_instructions):
      self.oai_key = oai_key
      self.llm_instructions = llm_instructions

  def load_model(self):
    """Initiate the Google Vision API client."""
    """Initiate the OAI API client."""
    client =  openai.OpenAI(
    # This is the default and can be omitted
        api_key=self.oai_key,
    )
    return client


  def run_inference(self, batch, model, inference):


    response = model.responses.create(
          model="gpt-4o",
          instructions=self.llm_instructions,
          input=batch[0],
      )
    return [response.output_text]



