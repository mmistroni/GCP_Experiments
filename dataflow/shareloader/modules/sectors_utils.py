import apache_beam as beam
import logging
import requests
from itertools import chain
from datetime import date, timedelta, datetime
from pandas.tseries.offsets import BDay
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
from functools import reduce
from collections import OrderedDict
from finvizfinance.group import Performance
from .obb_utils import fetch_historical_data
from ta.volume import OnBalanceVolumeIndicator, ChaikinMoneyFlowIndicator
import numpy as np

from ta.volume import OnBalanceVolumeIndicator, ChaikinMoneyFlowIndicator
from typing import List
from shareloader.modules.dftester_utils import to_json_string
from shareloader.modules.beam_inferences import PostProcessor, MODEL_NAME, GeminiModelHandler, GenAIClient
from google.genai import types
from apache_beam.ml.inference.base import RunInference
from typing import List, Sequence, Any, Dict
import json

SYSTEM_INSTRUCTIONS_STRING_TRIPLE_ANOMALY_ATR = (
    "You are an expert **Financial Market Anomaly Detection Agent**. "
    "Your objective is to analyze the provided 50-day time-series data, passed as a **JSON list of daily objects**, "
    "to identify all three of the following anomaly types: **Excessive Accumulation**, **Excessive Distribution**, and **Extreme Volatility Breakout**."
    "\n\n"
    "### DATA CONTEXT\n"
    "The input is a JSON array where each object contains the following fields for a trading day: "
    "`date`, `open`, `high`, `low`, `close`, `volume`, `change`, `vwap`, `obv`, and `cmf`. The data must be treated as a strict 50-day sequence. "
    "You must utilize all available data points for robust anomaly identification."
    "\n\n"
    "### SUCCESS CRITERIA (THREE ANOMALY TYPES)\n"
    "1. **Excessive Accumulation (Divergence):** A 5-day period where the **OBV increases by more than 15%** AND the **'close' price increases by less than 5%** over the same period. "
    "2. **Excessive Distribution (Divergence):** A 5-day period where the **OBV decreases by more than 15%** AND the **'close' price decreases by less than 5%** over the same period. "
    "3. **Extreme Volatility Breakout (Point Anomaly):** Any single day where the **True Range (High - Low)** is **3 times greater** than the **Average True Range (ATR)** of the preceding 10 days. This indicates a sudden, sharp, and unusual spike in intra-day volatility. "
    "\n\n"
    "### OUTPUT FORMAT\n"
    "The final output **MUST** be a structured JSON object listing **ALL** detected anomalies. For each anomaly, the output must include: `anomaly_type` (e.g., Accumulation, Distribution, or VolatilityBreakout), the exact `start_date` and `end_date` (which is the same as start_date for VolatilityBreakout), and a key metric (e.g., `OBV_change_percent` for divergences, or `TrueRange_Ratio` for volatility breakouts). **Do not include any conversational text or explanation outside of the structured tool call.**"
)

MAX_INPUT_DAYS = 20

system_instruction_string_anomaly_template = f'''
You are an expert **Financial Market Anomaly Detection Agent**. 
Your objective is to analyze the provided time-series data, passed as a **JSON list of daily objects**, 
to identify all three of the following anomaly types: **Excessive Accumulation**, **Excessive Distribution**, and **Extreme Volatility Breakout**.

### DATA CONTEXT
The input is a **subset** of the full 50-day history. Each object contains the daily fields (`date`, `open`, `high`, `low`, `close`, `obv`, `cdf`). 
CRUCIALLY, it **ALSO** includes the following pre-calculated metrics needed for anomaly checks: 
`true_range`, `atr_10_sma`, `obv_change_5d_pct`, and `close_change_5d_pct`. **The agent must not perform any rolling window calculations.**
The input size **will not exceed {MAX_INPUT_DAYS} objects** to prevent timeouts. All necessary lookback data must be pre-calculated.

### SUCCESS CRITERIA (THREE ANOMALY TYPES - BASED ON PRE-CALCULATED METRICS)
1. **Excessive Accumulation (Divergence):** Any day where the pre-calculated **'obv_change_5d_pct' is > 15%** AND the **'close_change_5d_pct' is < 5%**. 
2. **Excessive Distribution (Divergence):** Any day where the pre-calculated **'obv_change_5d_pct' is < -15%** AND the **'close_change_5d_pct' is > -5%**. 
3. **Extreme Volatility Breakout (Point Anomaly):** Any day where the **'true_range'** is **3 times greater** than the corresponding **'atr_10_sma'** value. 

### OUTPUT FORMAT
The final output **MUST** be a **single, structured JSON object** enclosed in a **Markdown code block**. This JSON object must contain a list of all detected anomalies. For each anomaly, the object must include: `anomaly_type` (e.g., Accumulation, Distribution, or VolatilityBreakout), the exact `start_date` and `end_date` (or `start_date` only for the point anomaly), and a key metric (e.g., `OBV_change_percent` or `TrueRange_Ratio`). 
**DO NOT include any conversational text, commentary, summaries, or explanation before or after the JSON code block.**

**Example Output Structure:**

```json
[
  {{
    "anomaly_type": "Accumulation",
    "start_date": "2025-01-05",
    "end_date": "2025-01-09",
    "OBV_change_percent": 18.5
  }},
  {{
    "anomaly_type": "VolatilityBreakout",
    "start_date": "2025-01-20",
    "TrueRange_Ratio": 3.1
  }}
]
'''

def to_json_string(element):
    def datetime_converter(o):
        if isinstance(o, datetime):
            return o.isoformat()  # Convert datetime to ISO 8601 string
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

    return json.dumps(element, default=datetime_converter)

def get_finviz_performance():

    # Create a Performance object
    performance = Performance()
    # Get the performance data
    return performance.screener_view().to_dict('records')


def fetch_performance(sector, ticker, key, start_date):
    end_date = date.today().strftime('%Y-%m-%d')
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={start_date}&to={end_date}&apikey={key}"
    historical = requests.get(url).json().get('historical')
    df = pd.DataFrame(data=historical[::-1])[['date', 'adjClose']]
    df['date'] = pd.to_datetime(df.date)
    df = df.rename(columns={'adjClose': sector})
    df = df.set_index('date')
    return df


def get_indicators(data: List[Dict]) -> List[Dict]:
    """
    Calculates technical indicators and lookback metrics required by the LLM agent.

    Args:
        data: List of dictionaries containing daily OHLCV and other raw data.
        num_days: The number of recent days to return (to respect the LLM's 20-day input limit).

    Returns:
        A list of dictionaries containing the processed data and indicators.
    """
    try:
        df = pd.DataFrame(data)

        # --- 1. Calculate Core Indicators (OBV and CMF) ---

        # Calculate On-Balance Volume (OBV)
        obv_indicator = OnBalanceVolumeIndicator(close=df['close'], volume=df['volume'])
        df['obv'] = obv_indicator.on_balance_volume()

        # Calculate Chaikin Money Flow (CMF) (Kept for completeness, though CMF isn't in the LLM criteria)
        cmf_indicator = ChaikinMoneyFlowIndicator(
            high=df['high'],
            low=df['low'],
            close=df['close'],
            volume=df['volume']
        )
        df['cmf'] = cmf_indicator.chaikin_money_flow()




        # --- 2. Calculate Volatility Metrics (True Range and ATR-SMA) ---

        # Calculate True Range (TR)
        # This is the 'true_range' field the LLM instructions refer to.
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift(1))
        low_close = np.abs(df['low'] - df['close'].shift(1))

        # Use pandas' built-in max across columns
        df['true_range'] = pd.DataFrame({'hl': high_low, 'hc': high_close, 'lc': low_close}).max(axis=1)

        # The rest of your code remains the same:
        df['atr_10_sma'] = df['true_range'].rolling(window=10).mean()
        # --- 3. Calculate 5-Day Divergence Metrics ---

        # Calculate 5-day % change for OBV
        # This is the 'obv_change_5d_pct' field. Multiplied by 100 for percentage value.
        df['obv_change_5d_pct'] = df['obv'].pct_change(periods=5) * 100

        # Calculate 5-day % change for Close price
        # This is the 'close_change_5d_pct' field. Multiplied by 100 for percentage value.
        df['close_change_5d_pct'] = df['close'].pct_change(periods=5) * 100

        # --- 4. Final Processing ---

        # Drop rows with NaN values resulting from the rolling window (the first 9 days)
        # Only the rows with full indicator data are kept.
        df.dropna(subset=['atr_10_sma', 'obv_change_5d_pct', 'close_change_5d_pct'], inplace=True)

        # Select ONLY the columns that the LLM agent explicitly needs for its logic and context
        # This is critical for minimizing payload size and honoring the system instructions.
        reduced = df[['date', 'open', 'high', 'low', 'close', 'obv',
                      'true_range', 'atr_10_sma', 'obv_change_5d_pct', 'close_change_5d_pct']]

        # Return only the last 'num_days' records to respect the LLM's input limit (e.g., 20 days)
        result = reduced.to_dict('records')[-MAX_INPUT_DAYS:]

        return result

    except Exception as e:
        # Better logging for debugging what failed
        logging.error(f'Failed to calculate indicators: {str(e)}')
        return []  # Return an empty list on failure, not a dict


def fetch_index_data(ticker, key):
    data = fetch_historical_data(ticker, key)[::-1]

    indicators = get_indicators(data)

    return indicators


def get_sector_rankings(key):
    # sample from https://wire.insiderfinance.io/unlocking-sector-based-momentum-strategies-in-asset-allocation-8560187f3ae3
    sector_tickers = OrderedDict([('XLK', 'Technology'), ('XLF', 'Financials'), ('XLE', 'Energy'),
                                  ('XLV', 'Health Care'), ('XLI', 'Industrials'), ('XLP', 'Consumer Staples'),
                                  ('XLU', 'Utilities'), ('XLY', 'Consumer Discretionary'), ('XLB', 'Materials'),
                                  ('XLRE', 'Real Estate'), ('XLC', 'Communication Services'), ('^GSPC', 'S&P500')
                                  ])
    holder = []

    start_date  = (date.today() - BDay(300)).date().strftime('%Y-%m-%d')

    for ticker, sec in sector_tickers.items():
         data = fetch_performance(sec, ticker, key, start_date=start_date)
         holder.append(data)

    data = pd.concat(holder, axis=1)

    # Define momentum periods
    momentum_periods = {
        '1M': 21,  # 1 month
        '3M': 63,  # 3 months
        '6M': 126,  # 6 months
        '12M': 252  # 12 months
    }

    # Calculate momentum and rankings
    momentum_data = {}
    sector_names = list(sector_tickers.values())
    for period_name, period_days in momentum_periods.items():
        momentum = data[sector_names].pct_change(period_days)
        momentum = momentum.loc[start_date:]
        momentum_rank = momentum.rank(axis=1, ascending=False, method='first')
        momentum_rank = momentum_rank.shift(1)
        momentum_data[period_name] = momentum_rank

    holder = []
    for key in momentum_periods.keys():
        data = momentum_data[key]
        #data = data.rename(index=sector_tickers)
        holder.append(data.tail(1))
    alldf = pd.concat(holder)

    index_map = {0: '1M', 1: '3M', 2: '6M', 3: '1Y'}

    alldf = alldf.reset_index(drop=True)

    transposed = alldf.T.rename(columns=index_map)

    cols = transposed.columns

    data = transposed[cols[::-1]]
    return data.reset_index().to_dict('records')

def generate_with_sector_instructions(
    model_name: str,
    batch: Sequence[str],
    model: GenAIClient,
    inference_args: dict[str, Any]):
    return model.models.generate_content(
        model=model_name,
        contents=batch,
        config=types.GenerateContentConfig(
            system_instruction=system_instruction_string_anomaly_template
        ),
        **inference_args)


def run_inference(p, google_key, prompts=None):
    model_handler = GeminiModelHandler(
        model_name=MODEL_NAME,
        request_fn=generate_with_sector_instructions,
        api_key=google_key
    )

    read_prompts = None
    if not prompts:
        logging.info('Generating pipeline prompts')

        read_prompts = (p | "gemini xxToJson" >> beam.Map(to_json_string)
                        | 'gemini xxCombine jsons' >> beam.CombineGlobally(lambda elements: "".join(elements))
                        | 'gemini xxanotheer map' >> beam.Map(lambda item: f'{item}')
                        )
    else:
        pipeline_prompts = prompts
        read_prompts = p | "GetPrompts" >> beam.Create(pipeline_prompts)

    # The core of our pipeline: apply the RunInference transform.
    # Beam will handle batching and parallel API calls.
    predictions = read_prompts | "RunInference" >> RunInference(model_handler)

    # Parse the results to get clean text.
    llm_response = (predictions | "PostProcess" >> beam.ParDo(PostProcessor())
                    )

    return llm_response
    #debug = llm_response | 'Debugging inference output' >> beam.Map(logging.info)



class ETFHistoryCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    logging.info('Adding{}'.format(input))
    logging.info('acc is:{}'.format(accumulator))
    accumulator.append(input)
    return accumulator

  def merge_accumulators(self, accumulators):

    return list(chain(*accumulators))

  def extract_output(self, sum_count):
    return sum_count


class SectorsEmailSender(beam.DoFn):
  def __init__(self, recipients, key):
      self.recipients = recipients.split(',')
      self.key = key

  def _build_personalization(self, recipients):
      personalizations = []
      for recipient in recipients:
          logging.info('Adding personalization for {}'.format(recipient))
          person1 = Personalization()
          person1.add_to(Email(recipient))
          personalizations.append(person1)
      return personalizations

  def _build_html_message(self, rows):
      html = '<table border="1">'
      header_row = "<tr><th>Sector</th><th>Perf Week</th><th>Perf Month</th><th>Perf Quart</th><th>Perf Half</th><th>Perf Year</th><th>Recom</th></tr>"

      html += header_row
      row_template = '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'

      for dct in rows:
          data = [dct.get('Name', ''), dct.get('Perf Week', ''), dct.get('Perf Month', ''), 
                  dct.get('Perf Quart', ''), dct.get('Perf Half', ''),
                  dct.get('Perf Year', ''), dct.get('Recom', '')         ]
          html += row_template.format(*data)
      html += '</table>'
      return html

  def process(self, element):
      sector_returns = element
      logging.info(f'Processing returns:\n{sector_returns}')
      data = self._build_html_message(element)
      content = \
            '''
            <html>
               <body>
                 <p>Compare Results against informations here https://www.investopedia.com/articles/trading/05/020305.asp</p>
                 <br><br>{}
               </body></html>'''.format(data)

      message = Mail(
          from_email='gcp_cloud_mm@outlook.com',
          subject='Sectors Return Ranking for last year',
          html_content=content)

      personalizations = self._build_personalization(self.recipients)
      for pers in personalizations:
          message.add_personalization(pers)

      sg = SendGridAPIClient(self.key)

      response = sg.send(message)
      logging.info('Mail Sent:{}'.format(response.status_code))
      logging.info('Body:{}'.format(response.body))

class SectorRankGenerator:
    GROUPBY_COL = 'GICS Sector'  # Use 'GICS Sector' or 'GICS Sub-Industry'
    S_AND_P_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

    def __init__(self, fmpKey, numPerGroup):
        self.key = fmpKey
        self.numPerGroup = numPerGroup
        self.end_date = date.today()
        self.start_date = (self.end_date - BDay(120)).date()

    def get_latest_price(self, ticker, key):
        stat_url = 'https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={token}'.format(symbol=ticker,
                                                                                                   token=key)
        res = requests.get(stat_url).json()[0]
        return res['price']

    def get_historical(self, ticker, key, start_date, end_date):
        hist_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/{}?apikey={}'.format(ticker, key)
        data = requests.get(hist_url).json().get('historical')
        df = pd.DataFrame(data=data)
        df = df[['date', 'close']].rename(columns={'close' : ticker})
        return df[ (df.date > start_date) & (df.date < end_date)][::-1]

    def get_sandp_historicals(self):
        return pd.read_html(self.S_AND_P_URL)[0]

    def get_ticker_prices(self, symbols):
        ticker_data = []
        start_date = self.start_date.strftime('%Y-%m-%d')
        end_date = self.end_date.strftime('%Y-%m-%d')

        for symbol in symbols:
            result = self.get_historical(symbol, self.key, start_date, end_date)
            ticker_data.append(result)

        return reduce(lambda acc, item: acc.merge(item, on='date', how='left'),
                            ticker_data[1:], ticker_data[0])

    def get_rank(self):
        # Need to learn how this work
        ## historical consituents from https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=79d4f398184fb636fa32ac1f95ed67e6
        key = self.key
        ticker_info = self.get_sandp_historicals()
        tickers = [
            ticker.replace('.', '-')
            for ticker in ticker_info['Symbol'].unique().tolist()
        ]
        ticker_prices = self.get_ticker_prices(tickers)
        ticker_prices = ticker_prices.dropna().reset_index().drop(columns='date')

        growth = 100 * (ticker_prices.iloc[-1] / ticker_prices.iloc[0] - 1)
        growth = (
            growth
                .to_frame()
                .reset_index()
                # .drop(columns=['level_0'])
                .rename(columns={'index': 'Symbol', 0: 'Growth'})
        )

        growth = growth.merge(
            ticker_info[['Symbol', self.GROUPBY_COL]],
            on='Symbol',
            how='left',
        )

        # Find the ranking of each stock per sector
        growth['sector_rank'] = (
            growth
                .groupby(self.GROUPBY_COL)
            ['Growth']
                .rank(ascending=False)
        )

        # Filter to only the winning stocks, and sort the values
        growth = (
            growth[growth['sector_rank'] <= self.numPerGroup]
                .sort_values(
                [self.GROUPBY_COL, 'Growth'],
                ascending=False,
            )
        )

        tickers = [v for v in growth.Symbol.values if 'index' not in v]
        latest = map(lambda t: {'Symbol': t, 'Latest' : self.get_latest_price(t, key)}, tickers)
        df = pd.DataFrame(data = latest)
        res = pd.merge(growth, df, on='Symbol')
        oldest = ticker_prices.tail(1).T.reset_index().rename(columns={"index": "Symbol"})
        mgd = pd.merge(res, oldest, on='Symbol')

        return mgd



