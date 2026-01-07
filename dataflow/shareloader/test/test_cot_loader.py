import unittest
import os
from apache_beam.testing.test_pipeline import TestPipeline


import apache_beam as beam
from openbb import obb
from shareloader.modules.correlation_analyzer import CorrelationAnalyzer, find_smallest_correlation
from shareloader.modules.signal_generator import SignalGenerator, DualFactorSignalGenerator
from shareloader.modules.vix_pipelines import VixSentimentCalculator, AcquireCOTDataFn, \
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

    def run_backtest_simulation(
            self,
            prepared_df: pd.DataFrame,
            initial_capital: float,
            trailing_stop_pct: float,
            take_profit_pct: float,
            max_risk_pct: float,
            commission_per_unit: float,
            optimal_lookback: int,  # Added from CorrelationAnalyzer
            optimal_hold_weeks: int  # Added from CorrelationAnalyzer
    ):
        """
        Orchestrates the full backtest pipeline: Signal Generation -> Simulation -> Metrics.
        """

        # 1. GENERATE SIGNALS
        # We use the optimal holding period found by the analyzer
        signal_gen = DualFactorSignalGenerator(
            traded_asset_col='vix_close',
            fixed_hold_period_days=optimal_hold_weeks * 5  # Convert weeks to days
        )

        # Process data to add 'Trade_Signal' and 'Hold_Period_Days'
        df_with_signals = signal_gen.process_data(prepared_df)

        # 2. DEFINE PARAMETERS (Pydantic Validation)
        params = BacktestParameters(
            initial_capital=initial_capital,
            trailing_stop_pct=trailing_stop_pct,
            take_profit_pct=take_profit_pct,
            max_risk_pct=max_risk_pct,
            price_column='vix_close',  # Matches vix_utils/SentimentCalc
            commission_per_unit=commission_per_unit
        )

        # 3. INSTANTIATE STRATEGY ENGINE
        # The engine now uses the 'BUY' signals from the generator
        strategy_engine = StrategyEngine(params)

        # 4. INSTANTIATE BACKTESTER & RUN SIMULATION
        backtester = Backtester(params, strategy_engine)
        results_df = backtester.run_simulation(df_with_signals)

        # 5. INSTANTIATE METRICS CALCULATOR & GET RESULTS
        metrics_calculator = MetricsCalculator(results_df)
        final_metrics = metrics_calculator.calculate_metrics()

        # --- Print Detailed Report ---
        print("\n" + "=" * 45)
        print("      VIX FEATURE AGENT: BACKTEST RESULTS")
        print("=" * 45)
        print(f"Optimal Lookback: {optimal_lookback} weeks")
        print(f"Optimal Hold:     {optimal_hold_weeks} weeks")
        print("-" * 45)
        for key, value in final_metrics.items():
            if isinstance(value, float):
                print(f"{key:25}: {value:>15.2f}")
            else:
                print(f"{key:25}: {value:>15}")
        print("=" * 45)

        results_df.to_csv('c:/Temp/VIX_NewBacktest.csv')
        return results_df, final_metrics

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
        from shareloader.modules.vix_market_data import get_vix_market_data
        import os
        import pandas as pd

        # --- Step 1: Data Acquisition ---
        print('... Getting data ....')
        key = os.environ.get('FMPREPKEY')


        # Assuming these return DataFrames with a DatetimeIndex
        cot_df = get_latest_cot()
        vix_prices = get_vix_market_data()  # Returns DF with 'Spot_VIX', etc.
        spx_prices = get_historical_prices('^GSPC', datetime.date(2004, 7, 20), key)

        # CONVERT spx_prices to a Series/DataFrame and merge
        df_spx = pd.DataFrame(spx_prices).set_index('date')['close'].rename('SPX_close')

        # MERGE SPX into VIX so the calculator sees both
        vix_prices = vix_prices.join(df_spx, how='inner')

        # CALCULATE the change here so it definitely exists
        vix_prices['SPX_1D_Change'] = vix_prices['SPX_close'].pct_change()
        # Note: Ensure spx_prices is merged into vix_prices or available for SignalGenerator

        # --- Step 2: Optimal Parameter Search (Optimization Loop) ---
        print('... Finding Optimal Parameters ....')

        lookbacks_to_test = [52, 104, 156]
        master_results = {}

        for lb in lookbacks_to_test:
            # Calculate sentiment for this specific lookback candidate
            calculator = VixSentimentCalculator(cot_lookback_period=lb)
            temp_res = calculator.calculate_sentiment(vix_prices, cot_df)

            # Analyze the predictive power (correlations) of this lookback
            analyzer = CorrelationAnalyzer(temp_res)
            master_results[lb] = analyzer.run_analysis(lags=[4, 8, 12])

        # Find which (Lookback, Lag) had the strongest predictive signal
        optimal_lookback, optimal_holding_weeks, optimal_correlation = find_smallest_correlation(master_results)

        print("\n--- Optimal Parameter Search Results ---")
        print(f"Optimal Lookback Period: {optimal_lookback} Weeks")
        print(f"Optimal Holding Period: {optimal_holding_weeks} Weeks")
        print(f"Strongest Correlation: {optimal_correlation:.4f}")

        # --- Step 3: Final Data Preparation ---
        print('... Preparing Final Sentiment Data ....')
        # Generate the FINAL sentiment dataframe using the optimal lookback found above
        calculator = VixSentimentCalculator(cot_lookback_period=optimal_lookback)
        res = calculator.calculate_sentiment(vix_prices, cot_df)

        # --- Step 4: Signal Generation ---
        print('... Generating Signals ....')
        # Instantiate the Generator with your strategy thresholds
        signal_gen = DualFactorSignalGenerator(
            cot_buy_threshold=20.0,
            spx_shock_threshold=-0.020
        )

        # Process the data to add 'Trade_Signal' and 'Hold_Period_Days'
        # We convert optimal_holding_weeks to days for the backtester
        prepared_data_df = signal_gen.process_data(res, hold_days=optimal_holding_weeks * 5)

        # --- Step 5: Backtest Simulation ---
        print('... Running Backtest ....')

        print(prepared_data_df.tail(10))

        prepared_data_df.to_csv('c:/Temp/vix_signal.csv', header=True, index=False)



        return
        # Strategy Parameters
        initial_capital = 20000.0
        trailing_stop_pct = 0.2
        take_profit_pct = 0.15
        max_risk_pct = 0.04
        commission_per_unit = 0.01

        # Run the simulation using the orchestration method
        # Note: We pass the optimized holding period to the simulation
        results_df, final_metrics = self.run_backtest_simulation(
            prepared_df=prepared_data_df,
            initial_capital=initial_capital,
            trailing_stop_pct=trailing_stop_pct,
            take_profit_pct=take_profit_pct,
            max_risk_pct=max_risk_pct,
            commission_per_unit=commission_per_unit,
            optimal_lookback=optimal_lookback,
            optimal_hold_weeks=optimal_holding_weeks
        )

        print("\n--- Final Backtest Metrics ---")
        print(final_metrics)
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


if __name__ == '__main__':
    unittest.main()
