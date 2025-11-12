import unittest
import os
from apache_beam.testing.test_pipeline import TestPipeline


import apache_beam as beam
from openbb import obb
from shareloader.modules.correlation_analyzer import CorrelationAnalyzer, find_smallest_correlation
from shareloader.modules.signal_generator import SignalGenerator
from shareloader.modules.vix_pipelines import find_smallest_correlation,VixSentimentCalculator, AcquireCOTDataFn, \
                                            AcquireVIXDataFn, CalculateSentimentFn, RunCorrelationAnalysisFn,\
                                            FindOptimalCorrelationFn, GenerateSignalFn, ExplodeCotToDailyFn

import pandas as pd
import numpy as np

import requests
import datetime
def get_historical_prices(ticker, start_date, key):

  hist_url = f'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={ticker}&apikey={key}&from=2004-01-01'

  data =  requests.get(hist_url).json()
  return [d for d in data if datetime.datetime.strptime(d['date'], '%Y-%m-%d').date() >=start_date]

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
        vix_prices = get_historical_prices('^VIX', datetime.date(2004, 7, 20), key)
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



    def test_cot_sentiment_backtest(self):

        def calculate_metrics(results_df: pd.DataFrame):
            # Total Return
            total_return = (results_df['Capital'].iloc[-1] - results_df['Capital'].iloc[0]) / \
                           results_df['Capital'].iloc[0]

            # Calculate daily returns for volatility analysis
            returns = results_df['Capital'].pct_change().dropna()

            # Annualized Return (assuming 252 trading days)
            annualized_return = returns.mean() * 252

            # Annualized Volatility
            annualized_volatility = returns.std() * np.sqrt(252)

            # Sharpe Ratio (Assuming a 0% risk-free rate for simplicity)
            sharpe_ratio = annualized_return / annualized_volatility

            # Max Drawdown (Helper function needed for proper max drawdown calculation)
            # The simple way:
            cumulative_max = results_df['Capital'].cummax()
            drawdown = (results_df['Capital'] - cumulative_max) / cumulative_max
            max_drawdown = drawdown.min()

            print("\n--- Backtest Results ---")
            print(f"Start Date: {results_df.index[0].strftime('%Y-%m-%d')}")
            print(f"End Date: {results_df.index[-1].strftime('%Y-%m-%d')}")
            print(f"Total Return: {total_return:.2%}")
            print(f"Annualized Return: {annualized_return:.2%}")
            print(f"Max Drawdown: {max_drawdown:.2%}")
            print(f"Sharpe Ratio: {sharpe_ratio:.2f}")

        def run_backtest_simulation(df: pd.DataFrame, initial_capital: float = 100000.0):
            # Ensure columns are present and dates are indices
            if df.index.dtype != 'datetime64[ns]':
                raise ValueError("DataFrame index must be a DatetimeIndex.")

            df = df.copy()

            # Initialize Tracking Columns
            df['Position'] = np.nan  # 1 for Long, 0 for Flat
            df['Entry_Price'] = np.nan
            df['Exit_Price'] = np.nan
            df['Exit_Date_Target'] = pd.NaT
            df['P_L'] = 0.0  # Profit/Loss for the day
            df['Capital'] = initial_capital

            # Simulation State Variables
            in_position = False

            # --- The Core Simulation Loop ---
            for i in range(len(df)):
                current_date = df.index[i]

                # Carry over capital from the previous day
                if i > 0:
                    df.loc[current_date, 'Capital'] = df.iloc[i - 1]['Capital']

                # 1. Check for Position Exit
                if in_position and current_date >= df.loc[df.index[i - 1], 'Exit_Date_Target']:
                    # Execute SELL based on today's closing price
                    exit_price = df.loc[current_date, 'VIX_Close']
                    entry_price = df.loc[df.index[i - 1], 'Entry_Price']  # Look back one row for the entry details

                    # Simple P&L calculation (assuming 1 unit/share for simplicity)
                    p_l = (exit_price - entry_price)

                    # Update Capital and Log Trade
                    df.loc[current_date, 'Capital'] += p_l
                    df.loc[current_date, 'P_L'] = p_l
                    df.loc[current_date, 'Exit_Price'] = exit_price
                    df.loc[current_date, 'Position'] = 0  # Mark position as closed

                    in_position = False

                # 2. Check for Position Entry (BUY)
                elif not in_position and df.loc[current_date, 'Trade_Signal'] == 'BUY':
                    # Execute BUY based on today's closing price
                    entry_price = df.loc[current_date, 'VIX_Close']

                    # Calculate the target exit date
                    hold_days = df.loc[current_date, 'Hold_Period_Days']
                    exit_date_target = current_date + timedelta(days=hold_days)  # Needs refinement for trading days

                    # Update Position Status
                    df.loc[current_date, 'Entry_Price'] = entry_price
                    df.loc[current_date, 'Exit_Date_Target'] = exit_date_target
                    df.loc[current_date, 'Position'] = 1  # Mark position as open

                    in_position = True

            return df.dropna(subset=['Position'])



        # Step 1. getting cot and vix prices
        print('... Gettingn data ....')
        key = os.environ['FMPREPKEY']
        vix_prices = get_historical_prices('^VIX', datetime.date(2004, 7, 20), key)
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
        final_df = generator.get_backtest_data()

        # Example usage (assuming 'final_df' contains all data and signal):
        final_results_df = run_backtest_simulation(final_df)
        calculate_metrics(final_results_df)


    def test_cot_pipeline(self):
        key = os.environ['FMPREPKEY']
        sink = beam.Map(print)
        KEY_BY_DATE = lambda record_list: [(record['date'], record) for record in record_list]

        with TestPipeline() as p:
            # Note: Best practice for production is to use --environment=sdk to pass keys securely
            fmp_key = os.environ.get('FMPREPKEY', 'DUMMY_KEY')

            # The starting PCollection is a single element used to trigger the fetches
            start_pcoll = p | 'Create Trigger' >> beam.Create(['Test'])

            # --- PARALLEL ACQUISITION STAGE (Stage 1) ---

            # 1.1 Acquire VIX Data (Keyed by 'A')
            vix_records = (
                    start_pcoll
                    | 'Acquire VIX Data' >> beam.ParDo(AcquireVIXDataFn(fmp_key=fmp_key))
            )

            # 2. Key VIX Records
            keyed_vix = (
                    vix_records
                    | 'Key VIX by Date' >> beam.Map(lambda record: (record['date'], record))
            )


            # 1.2 Acquire COT Data
            # FIX: Add a step to convert the list of records into (date, record) pairs.
            keyed_cot = (
                    start_pcoll
                    | 'Acquire COT Data' >> beam.ParDo(AcquireCOTDataFn(credentials={}))
            )

            # 1.2 Acquire COT Data and Explode to Daily Keys
            keyed_cot_daily = (
                    keyed_cot  # This is your PCollection of raw COT records
                    | 'Explode COT to Daily Keys' >> beam.ParDo(ExplodeCotToDailyFn())
            )

            return
            # 1.3 Combine VIX and COT Data (CoGroupByKey on the Daily Date)
            combined_data_list = (
                    {'vix': keyed_vix, 'cot': keyed_cot_daily}  # Use the exploded COT collection
                    | 'CoGroup VIX and COT' >> beam.CoGroupByKey()
            )

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
