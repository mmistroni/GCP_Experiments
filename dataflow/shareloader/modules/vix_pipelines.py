import apache_beam as beam
import pandas as pd
from openbb import obb
import requests
from datetime import date, datetime
from openbb_cftc.models.cot import CftcCotFetcher
import logging
import asyncio
import os
from shareloader.modules.vix_sentiment_calculator import VixSentimentCalculator
from shareloader.modules.correlation_analyzer import CorrelationAnalyzer, find_smallest_correlation
from shareloader.modules.signal_generator import  SignalGenerator

from pydantic import BaseModel, Field
from datetime import date, datetime
from typing import List, Dict, Any


class COTMetrics(BaseModel):
    """Defines the schema for data after sentiment calculation (res_df)."""
    date: date
    VIX_Close: float
    COT_Sentiment: float
    # Add other key columns that VixSentimentCalculator adds to the DataFrame


class OptimalParameters(BaseModel):
    """Schema for the final optimal parameters output (Stage 4)."""
    optimal_lookback: int
    optimal_holding_period: int
    optimal_correlation: float


class CorrelationResult(BaseModel):
    """Schema for the result of a single correlation calculation."""
    lookback_period: int = Field(alias='lookback_period')
    holding_period: int = Field(alias='holding_period')
    correlation_value: float


# Utility function for VIX remains the same, but is now called inside a DoFn
def get_historical_prices(ticker, start_date, key):
    # This remains a utility as it runs outside the main pipeline flow
    hist_url = f'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={ticker}&apikey={key}&from=2004-01-01'
    data = requests.get(hist_url).json()
    prices = [d for d in data if datetime.strptime(d['date'], '%Y-%m-%d').date() >= start_date]
    return pd.DataFrame(prices)


# --- NEW Stage 1.1: Acquire VIX Data ---
class AcquireVIXDataFn(beam.DoFn):
    def __init__(self, fmp_key: str):
        self.fmp_key = fmp_key

    def process(self, element):
        """Element is the dummy value (None). Returns keyed VIX DataFrame."""
        print('... Acquiring VIX Data ....')
        vix_df = get_historical_prices('^VIX', date(2004, 7, 20), self.fmp_key)
        # Key the output with a constant 'A'
        yield ('A', vix_df)


# --- NEW Stage 1.2: Acquire COT Data (Async/External) ---
class AcquireCOTDataFn(beam.DoFn):
    # No __init__ needed unless you pass credentials/IDs

    def __init__(self, credentials):
        self.credentials = credentials
        self.fetcher = CftcCotFetcher


    def process(self, element):
        """Element is the dummy value (None). Returns keyed COT DataFrame."""
        print('... Acquiring COT Data (Async Call) ....')
        # This is where the external/async call should happen
        cot_df = obb.regulators.cftc.cot(id='1170E1', provider='cftc').to_dataframe()
        # Key the output with a constant 'A'
        yield ('A', cot_df)

    async def fetch_data(self, element: str):
        logging.info(f'element is:{element}')

        params = dict(series_name=element)
        try:
            data = await self.fetcher.fetch_data(params, {})
            result = [d.model_dump(exclude_none=True) for d in data]
            if result:
                return result
            else:
                return []
        except Exception as e:
            logging.info(f'Failed to fetch data for {element}:{str(e)}')
            return []

    def process(self, element: str):
        logging.info(f'Input elements:{element}')
        with asyncio.Runner() as runner:
            return runner.run(self.fetch_data(element))

# Stage 2: Calculate Sentiment remains the same, but now it receives (key, tuple)
class CalculateSentimentFn(beam.DoFn):
    def __init__(self, cot_lookback, oi_lookback):
        self.calculator = VixSentimentCalculator(cot_lookback_period=cot_lookback, oi_lookback_period=oi_lookback)

    def process(self, keyed_combined_data: tuple):
        """Input is ('A', {'vix': [df], 'cot': [df]}) from CoGroupByKey"""
        key, grouped_data = keyed_combined_data
        print('... Stage 2: Calculating Sentiment ....')

        # Extract the DataFrames from the grouped lists
        vix_df = grouped_data['vix'][0]
        cot_df = grouped_data['cot'][0]

        # Ensure the inputs are Pandas DataFrames
        res_df = self.calculator.calculate_sentiment(vix_df, cot_df)

        # Yield the processed DataFrame (COT/VIX with sentiment metrics)
        yield res_df


# Stage 3: Correlation Analysis
class RunCorrelationAnalysisFn(beam.DoFn):
    def process(self, sentiment_df: pd.DataFrame):
        print('... Stage 3: Correlation Analysis ....')
        analyzer = CorrelationAnalyzer(sentiment_df)
        analyzer.run_analysis()
        results_df = analyzer.get_results_table()
        # Yield the DataFrame containing all lookback/holding period correlations
        yield results_df


# Stage 4: Find Optimal Correlation
class FindOptimalCorrelationFn(beam.DoFn):
    def process(self, results_df: pd.DataFrame):
        print('... Stage 4: Finding Optimal Correlation ....')
        optimal_lookback, optimal_holding_period, optimal_correlation = find_smallest_correlation(results_df)

        # Package result into the Pydantic model and yield as JSON
        optimal_params = OptimalParameters(
            optimal_lookback=optimal_lookback,
            optimal_holding_period=optimal_holding_period,
            optimal_correlation=optimal_correlation
        )
        # Key the JSON output with a constant 'B' for the next join
        yield ('B', optimal_params.model_dump_json())


# Stage 5: Signal Generation (Requires two inputs: Sentiment DF and Optimal Parameters)
class GenerateSignalFn(beam.DoFn):
    def process(self, keyed_joined_data: tuple):
        """Input is ('B', {'sentiment': [df], 'optimal': [json]}) from the final CoGroupByKey"""
        key, grouped_data = keyed_joined_data
        print('... Stage 5: Generating Signal ....')

        # 1. Extract and Parse Data
        optimal_params_json = grouped_data['optimal'][0]
        sentiment_df = grouped_data['sentiment'][0]

        # Use Pydantic to validate and load the optimal parameters
        optimal_params = OptimalParameters.model_validate_json(optimal_params_json)
        optimal_lookback = optimal_params.optimal_lookback

        # 2. Run Signal Generator
        generator = SignalGenerator(sentiment_df, optimal_lookback)
        signal = generator.get_current_signal()

        # Yield the final signal value
        yield signal