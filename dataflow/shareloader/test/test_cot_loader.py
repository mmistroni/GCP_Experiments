import unittest
import os
from apache_beam.testing.test_pipeline import TestPipeline


import apache_beam as beam
from openbb import obb
from shareloader.modules.correlation_analyzer import CorrelationAnalyzer, find_smallest_correlation
from shareloader.modules.signal_generator import SignalGenerator, DualFactorSignalGenerator
from shareloader.modules.vix_pipelines import find_smallest_correlation,VixSentimentCalculator, AcquireCOTDataFn, \
                                            AcquireVIXDataFn, CalculateSentimentFn, RunCorrelationAnalysisFn,\
                                            FindOptimalCorrelationFn, GenerateSignalFn, ExplodeCotToDailyFn

import pandas as pd
import pandas as pd
import numpy as np
from typing import Tuple, Union, Any # Import Any for parameter type hinting

from datetime import timedelta
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


import pandas as pd
import numpy as np
from datetime import timedelta
from pydantic import BaseModel, Field


# ----------------------------------------------------
# 1. PARAMETER MODEL (The 'Pydantic' Component)
# ----------------------------------------------------
from pydantic import BaseModel, Field
from typing import Optional

from pydantic import BaseModel, Field
from typing import Optional, Any
import pandas as pd
import numpy as np
from typing import Tuple, Union

from pydantic import BaseModel, Field
from typing import Optional, Any
import pandas as pd
import numpy as np
from typing import Tuple, Union
from shareloader.modules.backtesting import BacktestParameters, Backtester, MetricsCalculator, StrategyEngine


# NOTE: The global COT_SPIKE_FILTER is now obsolete and should be removed.
# ----------------------------------------------------
# 2. STRATEGY AND TRADE SIZING ENGINE
# ----------------------------------------------------
import pandas as pd
import numpy as np
from typing import Tuple, Union


# Assuming BacktestParameters class is defined elsewhere

# ----------------------------------------------------
# 3. MAIN SIMULATION ENGINE
# ----------------------------------------------------

# Assuming BacktestParameters is defined and passed in (from Pydantic class)
# Assuming StrategyEngine is defined and returns ('SELL' for entry)



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

    def prepare_daily_backtest_data(self, cot_df: pd.DataFrame, vix_df: pd.DataFrame) -> pd.DataFrame:
        """
        Aligns weekly COT data (which is forward-filled) to daily VIX prices,
        preparing the final daily-indexed DataFrame.
        Assumes VIX price column is named 'close'.
        """

        # 1. Calculate the required 'noncomm_net' in the COT data
        cot_df = cot_df.copy()
        if 'noncomm_net' not in cot_df.columns:
            # Assuming the necessary underlying columns exist
            cot_df['noncomm_net'] = (
                    cot_df['noncomm_positions_long_all'] - cot_df['noncomm_positions_short_all']
            )

        cot_cols_to_keep = ['noncomm_net']
        cot_daily = cot_df[cot_cols_to_keep]

        # 2. Forward-Fill COT data to every trading day
        # Create a daily index covering the entire VIX range
        daily_index = vix_df.index

        # Reindex the COT data to the full daily index and forward-fill (ffill)
        cot_daily = cot_daily.reindex(daily_index).ffill()

        # 3. Final Merge (Align VIX and COT)
        # The VIX price column ('close') is included here
        final_merged_df = vix_df.merge(cot_daily, left_index=True, right_index=True, how='inner')

        if 'noncomm_net' not in final_merged_df.columns:
            raise ValueError("Failed to merge 'noncomm_net'. Check column names in COT input.")

        # 4. Drop leading NaNs from the start of the COT data
        return final_merged_df.dropna(subset=cot_cols_to_keep)
    # --- EXAMPLE USAGE ---
    # daily_aligned_df = prepare_daily_backtest_data(weekly_cot_df, daily_vix_df)
    # generator = SignalGenerator(df=daily_aligned_df, ...)

    def run_backtest_simulation(self, mock_df: pd.DataFrame,
                                             initial_capital: float,
                                             trailing_stop_pct:float ,
                                             take_profit_pct: float,
                                             max_risk_pct:float,
                                             commission_per_unit: float,
                                             vix_ratio_threshold:float):
        """
        Simulates the long-only trading strategy incorporating Trailing Stop-Loss and
        a PERCENTAGE-BASED Take Profit (take_profit_pct) that scales with entry price.

        :param take_profit_pct: Percentage gain from entry price to trigger exit (e.g., 0.30 for 30%).
        """
        # Example of how you would kick off the backtest:

        # 1. DEFINE PARAMETERS
        params = BacktestParameters(
            # Use your actual desired parameters here
            initial_capital=initial_capital,
            trailing_stop_pct=trailing_stop_pct,
            take_profit_pct=take_profit_pct,
            max_risk_pct=max_risk_pct,
            price_column='VIX_close',
            commission_per_unit=commission_per_unit,
            vix_ratio_threshold=vix_ratio_threshold
        )

        # 2. INSTANTIATE STRATEGY ENGINE
        strategy_engine = StrategyEngine(params)

        # 3. INSTANTIATE BACKTESTER & RUN SIMULATION
        backtester = Backtester(params, strategy_engine)
        results_df = backtester.run_simulation(mock_df)

        # 4. INSTANTIATE METRICS CALCULATOR & GET RESULTS
        metrics_calculator = MetricsCalculator(results_df)
        final_metrics = metrics_calculator.calculate_metrics()

        print(final_metrics)
        # You should see the exact same result: {'Initial Capital': 20000.0, 'Final Capital': 13797.18..., 'Total Closed Trades': 6, 'Win Rate (%)': 0.0, ...}
        results_df.to_csv('c:/Temp/VIX_NewBacktest.csv')

    def verify_backtest_input(self, df: pd.DataFrame) -> None:
        """Checks for the presence and content of the 'Trade_Signal' column."""
        required_col = 'Trade_Signal'

        # Check 1: Column Existence
        if required_col not in df.columns:
            raise KeyError(
                f"ERROR: Missing required column '{required_col}'. "
                "Ensure you are using the output of DualFactorSignalGenerator.get_backtest_data()."
            )

        # Check 2: Content Validation
        valid_signals = ['BUY', 'SELL', 'NEUTRAL']

        # Drop NaNs just in case and check the remaining unique values
        unique_signals = df[required_col].dropna().unique()

        # Check 3: Presence of an executable trade signal
        if 'BUY' not in unique_signals and 'SELL' not in unique_signals:
            raise ValueError(
                f"ERROR: '{required_col}' column contains no executable signals ('BUY' or 'SELL'). "
                "Check your DualFactorSignalGenerator thresholds (COT/SPX) or data range."
            )

        # Check 4: Trade Count (A quick final sanity check)
        trade_count = df[df[required_col].isin(['BUY', 'SELL'])].shape[0]
        print(f"✅ Input Verified: Found {trade_count} days with executable signals ('BUY' or 'SELL').")

        # Example usage:
        # backtest_input_df = signal_gen.get_backtest_data()
        # verify_backtest_input(backtest_input_df)
        # self.run_backtest_simulation(backtest_input_df, initial_capital=10000.0)


    def dq_checks(self, prepared_data_df: pd.DataFrame ) -> None:
        # Check 1: How many days is the VIX COT Index at or below the entry threshold?
        cot_threshold = 10.0  # Use your actual threshold if different
        extreme_sentiment_count = (prepared_data_df['vix_cot_index'] <= cot_threshold).sum()
        print(f"Days with Extreme VIX COT Sentiment (<= {cot_threshold}): {extreme_sentiment_count}")

        # Check 2: How many days is the SPX 10-day change at or below the shock threshold?
        shock_threshold = -0.010  # Use your actual threshold if different
        market_shock_count = (prepared_data_df['SPX_10D_Change'] <= shock_threshold).sum()
        print(f"Days with SPX Market Shock (<= {shock_threshold * 100}%): {market_shock_count}")

        # Check 3: How many days are BOTH conditions met?
        dual_factor_count = (
                (prepared_data_df['vix_cot_index'] <= 10.0) &
                (prepared_data_df['SPX_10D_Change'] <= -0.010)
        ).sum()
        print(f"Days with Dual-Factor Entry Signal: {dual_factor_count}")

        # Check 4: Print the column list and a sample of the data
        print("--- DataFrame Info ---")
        prepared_data_df.info()
        print("\n--- Sample Data ---")
        print(prepared_data_df[['vix_cot_index', 'SPX_10D_Change']].head(10))

    def test_cot_simulation(self):


        # prepare data
        print('... Gettingn data ....')
        key = os.environ['FMPREPKEY']
        vix_prices = get_historical_prices('^VIX', datetime.date(2004, 7, 20), key)
        spx_prices = get_historical_prices('^GSPC', datetime.date(2004, 7, 20), key)
        cot_df = get_latest_cot()

        cot_df.to_csv('C:/Temp/cot.csv', index=False)
        df2 = pd.DataFrame(spx_prices)
        df2.to_csv('C:/Temp/spx.csv', index=False)

        df3 = pd.DataFrame(vix_prices)
        df3.to_csv('C:/Temp/vix.csv', index=False)

        # Step 2. calcuclate sentiment
        print('... Calculating Sentiment ....')
        calculator = VixSentimentCalculator(pd.DataFrame(vix_prices), cot_df, pd.DataFrame(spx_prices))
        res = calculator.calculate_sentiment()
        res['close'] = res['VIX_close']
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

        initial_capital = 20000.0
        trailing_stop_pct = 0.2
        take_profit_pct = 0.15
        max_risk_pct = 0.040
        commission_per_unit = 0.01
        cot_buy_threshold = 10.0
        spx_shock_threshold = -0.020
        vix_ratio_threshold = 2.0

        signal_gen = DualFactorSignalGenerator(
            cot_buy_threshold=cot_buy_threshold,
            spx_shock_threshold=spx_shock_threshold,
            traded_asset_col='VIX_close'  # Specify the price series
        )
        prepared_data_df = res


        # 2. Process the data (the single point of entry for the DataFrame)
        # 'prepared_data_df' is the output from your VixSentimentCalculator

        # 1. Instantiate the Generator (using your confirmed thresholds)
        # 2. Process the data (This is where Trade_Signal is created and stored internally)
        # 'prepared_data_df' is the output from your VixSentimentCalculator
        signal_gen.process_data(prepared_data_df)

        # 3. Retrieve the final, backtest-ready data (This DataFrame NOW has 'Trade_Signal')
        backtest_input_df = signal_gen.get_backtest_data()

        self.verify_backtest_input(backtest_input_df)


        # NOTE: Using the default price_column='close'
        self.run_backtest_simulation(backtest_input_df, initial_capital=initial_capital,
                                            trailing_stop_pct=trailing_stop_pct,
                                            take_profit_pct=take_profit_pct,
                                            max_risk_pct=max_risk_pct,
                                            commission_per_unit=commission_per_unit,
                                           vix_ratio_threshold=vix_ratio_threshold)

        '''
        results_df.to_csv('c:/Temp/VIXPNL.csv')

        # --- Calculating and Printing Final P&L ---
        # 1. Get the final value from the last row of the 'Capital' column
        final_capital = results_df['Capital'].iloc[-1]

        # 2. Calculate the Profit and Loss
        final_pnl = final_capital - initial_capital

        # 3. Calculate Return Percentage
        return_percentage = (final_pnl / initial_capital) * 100

        print("\n--- Backtest Summary ---")
        print(f"Initial Capital: ${initial_capital:,.2f}")
        print(f"Final Capital:   ${final_capital:,.2f}")
        print("------------------------")
        print(f"Total P&L:       ${final_pnl:,.2f}")
        print(f"Return:          {return_percentage:.2f}%")
        '''
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
