import unittest
import os
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date
from apache_beam.options.pipeline_options import PipelineOptions
from shareloader.modules.obb_utils import AsyncProcess, AsyncProcessSP500Multiples, ProcessHistorical, \
                                            AsyncFMPProcess
from shareloader.modules.obb_processes import AsyncCFTCTester
from shareloader.modules.launcher import StockSelectionCombineFn
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference
from datetime import datetime
import json
from shareloader.modules.launcher_pipelines import   run_etoro_pipeline
from unittest.mock import patch
import argparse
from shareloader.modules.dftester_utils import to_json_string, SampleOpenAIHandler, extract_json_list
from shareloader.modules.launcher_pipelines import run_inference
from google import genai
import apache_beam as beam
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler
from apache_beam.ml.inference.gemini_inference import generate_from_string
import asyncio
import apache_beam as beam
import openai as openai
from shareloader.modules.beam_inferences import run_gemini_pipeline


import apache_beam as beam
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler # New for Beam 2.66
from apache_beam.ml.inference.gemini_inference import generate_from_string # New for Beam 2.66
from apache_beam.options.pipeline_options import PipelineOptions
from openbb import obb
from shareloader.modules.vix_sentiment_calculator import VixSentimentCalculator
from shareloader.modules.correlation_analyzer import CorrelationAnalyzer, find_smallest_correlation
from shareloader.modules.signal_generator import SignalGenerator
from shareloader.modules.vix_pipelines import find_smallest_correlation,VixSentimentCalculator, AcquireCOTDataFn, \
                                            AcquireVIXDataFn, CalculateSentimentFn, RunCorrelationAnalysisFn,\
                                            FindOptimalCorrelationFn, GenerateSignalFn

import pandas as pd


import requests
from datetime import date
def get_historical_prices(ticker, start_date, key):

  hist_url = f'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={ticker}&apikey={key}&from=2004-01-01'

  data =  requests.get(hist_url).json()
  return [d for d in data if datetime.strptime(d['date'], '%Y-%m-%d').date() >=start_date]

def get_latest_cot():
    # async process
    return obb.regulators.cftc.cot(id='1170E1', provider='cftc')


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.debugSink = beam.Map(print)


    def test_cot_sentimente(self):
        # Step 1. getting cot and vix prices
        print('... Gettingn data ....')
        key = os.environ['FMPREPKEY']
        vix_prices = get_historical_prices('^VIX', date(2004, 7, 20), key)
        cot_df = get_latest_cot()
        # Step 2. calcuclate sentiment
        print('... Calculating Sentiment ....')
        calculator = VixSentimentCalculator(cot_lookback_period=52 * 5, oi_lookback_period=52 * 1)
        res = calculator.calculate_sentiment(pd.DataFrame(vix_prices), cot_df)
        # Step 3. Correlation analysis
        print('... Correlation analysis ....')
        analyzer = CorrelationAnalyzer(res)
        analyzer.run_analysis()
        results_df = analyzer.get_results_table()
        # Step 4:  Find optimal correlation
        print('... Optimal corr ....')
        optimal_lookback, optimal_holding_period, optimal_correlation = find_smallest_correlation(results_df)
        print("--- Optimal Parameter Search Results ---")
        print(f"Based on the analysis, the most predictive signal (smallest correlation) is found at:")
        print(f"Optimal Lookback Period (Row Index): {optimal_lookback} Weeks")
        print(f"Optimal Holding Period (Column Name): {optimal_holding_period} Weeks")
        print(f"Optimal Correlation Value: {optimal_correlation}")
        # Step 5.  Signal Generation
        # 2. Run Signal Generator
        print('... Generatign signaldata ....')
        generator = SignalGenerator(res, optimal_lookback)
        signal = generator.get_current_signal()
        print(signal)

    def test_cot_pipeline(self):
        key = os.environ['FMPREPKEY']
        sink = beam.Map(print)

        # --- FIXING THE KEYING LOGIC FOR COT/VIX ---
        def parse_and_key_records(record_list):
            """
            Parses data (JSON strings or DataFrame) into a stream of (date, dict) tuples.
            Assumes the ParDo yields a list containing the data structure (usually one element).
            """
            if not record_list:
                return

            first_element = record_list[0]
            data_records = []

            # CASE 1: The element is a Pandas DataFrame
            if isinstance(first_element, pd.DataFrame):
                print("Detected DataFrame input. Converting to list of records.")
                # Convert DataFrame rows into a list of dictionaries
                data_records = first_element.to_dict('records')

            # CASE 2: The element is a JSON string (or list of them, the old logic)
            elif isinstance(first_element, str):
                print("Detected JSON string input. Parsing...")
                for record_json in record_list:
                    try:
                        # Original logic for JSON parsing
                        data_records.append(json.loads(record_json))
                    except json.JSONDecodeError:
                        print(f"Skipping malformed JSON record.")
                        continue

            else:
                print(f"Skipping unknown data type received: {type(first_element)}.")
                return

            # 2. Key the processed dictionary records
            for record_dict in data_records:
                if 'date' in record_dict:
                    # Yields the (key, value) pair required for CoGroupByKey
                    yield (record_dict['date'], record_dict)
                else:
                    print(f"Skipping record without 'date' key: {record_dict}")

        with TestPipeline() as p:
            # Note: Best practice for production is to use --environment=sdk to pass keys securely
            fmp_key = os.environ.get('FMPREPKEY', 'DUMMY_KEY')

            # The starting PCollection is a single element used to trigger the fetches
            start_pcoll = p | 'Create Trigger' >> beam.Create(['Test'])

            # --- PARALLEL ACQUISITION STAGE (Stage 1) ---

            # 1.1 Acquire VIX Data
            keyed_vix = (
                    start_pcoll
                    | 'Acquire VIX Data' >> beam.ParDo(AcquireVIXDataFn(fmp_key=fmp_key))
                    # FIX: Use FlatMap with the new function to PARSE and KEY in one step.
                    | 'Parse & Key VIX' >> beam.FlatMap(parse_and_key_records)
            )

            # 1.2 Acquire COT Data
            keyed_cot = (
                    start_pcoll
                    | 'Acquire COT Data' >> beam.ParDo(AcquireCOTDataFn(credentials={}))
                    # FIX: Use FlatMap with the new function to PARSE and KEY in one step.
                    | 'Parse & Key COT' >> beam.FlatMap(parse_and_key_records)
            )

            # 1.3 Combine VIX and COT Data (Joins 1.1 and 1.2)
            # This transform now receives PCollections correctly keyed by date.

            keyed_cot | 'to sink' >> sink

            return

            combined_data_list = (
                    {'vix': keyed_vix, 'cot': keyed_cot}
                    | 'Combine VIX and COT' >> beam.CoGroupByKey()
            )

            combined_data_list | 'to sink' >> sink

            return

            # --- SENTIMENT CALCULATION STAGE (Stage 2) ---
            # Output is a single DataFrame
            sentiment_data = (
                    combined_data_list
                    | 'Calculate Sentiment Metrics' >> beam.ParDo(
                CalculateSentimentFn(cot_lookback=52 * 5, oi_lookback=52 * 1))
            )

            # --- CORRELATION ANALYSIS STAGE (Stage 3) ---
            correlation_results = (
                    sentiment_data
                    | 'Run Correlation Analysis' >> beam.ParDo(RunCorrelationAnalysisFn())
            )

            # --- OPTIMAL PARAMETER FINDING STAGE (Stage 4) ---
            # Output is a single JSON string, keyed by 'B' for the final join
            optimal_params_pcollection = (
                    correlation_results
                    | 'Find Optimal Parameters' >> beam.ParDo(FindOptimalCorrelationFn())
            )

            # --- SIGNAL GENERATION STAGE (Stage 5) ---

            # 5a. Key the Sentiment Data for the final join (Keyed by 'B')
            keyed_sentiment_for_join = sentiment_data | 'Key Sentiment for Join' >> beam.Map(lambda x: ('B', x))

            # 5b. Join the Sentiment Data (Stage 2) and Optimal Parameters (Stage 4)
            joined_data = (
                    {'sentiment': keyed_sentiment_for_join, 'optimal': optimal_params_pcollection}
                    | 'Join Data for Signal Generation' >> beam.CoGroupByKey()
            )

            # 5c. Generate the Signal
            final_signal = (
                    joined_data
                    | 'Generate Final Signal' >> beam.ParDo(GenerateSignalFn())
                    | 'Print Final Signal' >> beam.Map(lambda x: print(f"--- Final Signal: {x} ---"))
            )
            
            final_signal | 'to sink' >> sink
            ###

if __name__ == '__main__':
    unittest.main()
