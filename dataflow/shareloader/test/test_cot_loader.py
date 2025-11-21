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


class BacktestParameters(BaseModel):
    """
    Defines and validates the key parameters for the backtest.
    """
    price_column: str = Field('VIX_close', description="Column name for the traded instrument's price.")
    spx_column: str = Field('SPX_close', description="Column name for the S&P 500 price data.")
    initial_capital: float = Field(20000.0, gt=0, description="Starting cash.")

    # Risk Management Parameters
    trailing_stop_pct: float = Field(0.10, gt=0, lt=1, description="Percentage for TSL.")
    take_profit_pct: float = Field(0.20, gt=0, lt=1, description="Percentage for TP.")
    max_risk_pct: float = Field(0.015, gt=0, lt=1, description="Max % of capital to risk per trade (the fix).")
    commission_per_unit: float = Field(0.01, ge=0, description="Commission per unit traded.")

    # Strategy-Specific Entry Filter Parameter
    cot_entry_threshold: float = Field(10.0, ge=0, description="VIX COT Index threshold for entry confirmation.")

# NOTE: The global COT_SPIKE_FILTER is now obsolete and should be removed.
# ----------------------------------------------------
# 2. STRATEGY AND TRADE SIZING ENGINE
# ----------------------------------------------------
import pandas as pd
import numpy as np
from typing import Tuple, Union


# Assuming BacktestParameters class is defined elsewhere

def StrategyEngine(current_row: pd.Series, current_capital: float, in_position: bool, entry_price: float,
                   params) -> Tuple[str, float, Union[str, None]]:
    """
    Determines the trading action (SELL/BUY, EXIT, HOLD) based on the pre-calculated
    signal and the position state.

    :returns: (action: str, units_to_trade: float, exit_reason: str or None)
    """

    action = 'NEUTRAL'
    units_to_trade = 0.0
    exit_reason = None

    # --- Entry Logic (Hypothesis: Invert Long VIX to Short VIX) ---
    # We rely *only* on the 'Trade_Signal' being 'BUY', as the generator already
    # ensured the dual factor (COT <= threshold AND SPX Shock) was met.
    if not in_position and current_row['Trade_Signal'] == 'BUY':

        # 1. Risk-Based Position Sizing
        max_dollar_risk = current_capital * params.max_risk_pct

        # Risk defined by TSL distance (loss if price moves against the short position)
        # Note: For a SHORT, the stop loss is above the entry price.
        # For simplicity, we use the Trailing Stop % * current price as the maximum dollar risk per unit.
        stop_loss_distance_per_unit = current_row[params.price_column] * params.trailing_stop_pct

        # Calculate units: (Max Dollar Risk) / (Loss per Unit)
        # Add small epsilon (1e-9) to prevent division by zero
        units_held = max_dollar_risk / (stop_loss_distance_per_unit + 1e-9)

        # FIX: Action is 'SELL' to initiate a SHORT position, testing the inverted direction.
        return 'SELL', units_held, None

    # --- Exit Logic (Priority Check when in a position) ---
    elif in_position:
        peak_price = current_row['Peak_Price_in_Position'] if 'Peak_Price_in_Position' in current_row and not np.isnan(
            current_row.get('Peak_Price_in_Position', entry_price)) else entry_price
        current_price = current_row[params.price_column]

        # Note: TSL/TP triggers below are simplified and assumed to be relative to the entry.
        # For a short trade, TSL is activated when price RISES and TP is activated when price FALLS.

        # 1. Trailing Stop-Loss (Loss if price rises against the short trade)
        tsl_trigger = entry_price * (1.0 + params.trailing_stop_pct)  # Price rises above entry + TSL
        if current_price >= tsl_trigger:
            return 'EXIT', 0.0, 'TSL'

        # 2. Take Profit (Profit if price falls by TP amount)
        tp_trigger_price = entry_price * (1.0 - params.take_profit_pct)  # Price falls below entry - TP
        if current_price <= tp_trigger_price:
            return 'EXIT', 0.0, 'TP'

        # 3. Time-Based Exit
        if 'Exit_Date_Target' in current_row and current_row.name >= current_row['Exit_Date_Target']:
            # Exit after hold period regardless of profit/loss for a clean test
            return 'EXIT', 0.0, 'Time'

    # --- Default: Hold ---
    return action, units_to_trade, exit_reason


# ----------------------------------------------------
# 3. MAIN SIMULATION ENGINE
# ----------------------------------------------------
def run_simulation_engine(df: pd.DataFrame, params: BacktestParameters) -> pd.DataFrame:
    df = df.sort_index().copy()

    # Pre-calculate Exit Date Target
    df['Exit_Date_Target'] = df.index + pd.to_timedelta(df['Hold_Period_Days'], unit='D')

    # Initialize tracking columns
    df['Position_Size'] = 0.0
    df['Realized_P_L'] = 0.0
    df['Capital'] = np.nan  # Will be filled iteratively
    df['Peak_Price_in_Position'] = np.nan
    df['Exit_Reason'] = None
    df['Entry_Price'] = np.nan

    # Position State Variables
    in_position = False
    units_held = 0.0
    entry_price = 0.0
    peak_price = 0.0
    running_capital = params.initial_capital  # <-- Use running variable for calculation

    # Set initial capital
    if not df.empty:
        df.loc[df.index[0], 'Capital'] = running_capital

    for i in range(len(df)):
        current_date = df.index[i]
        current_row = df.loc[current_date]
        current_price = current_row[params.price_column]

        # 1. Update Peak Price and Position Tracking
        df.loc[current_date, 'Position_Size'] = units_held
        df.loc[current_date, 'Peak_Price_in_Position'] = peak_price

        if in_position:
            peak_price = max(peak_price, current_price)
            df.loc[current_date, 'Peak_Price_in_Position'] = peak_price

        # --- Call External Strategy Engine for Decision ---
        # FIX: 'action' is defined here as the first output of the StrategyEngine call
        action, units_to_trade, exit_reason = StrategyEngine(
            current_row,
            running_capital,  # Pass the running capital for accurate sizing
            in_position,
            entry_price,
            params
        )

        # --- Execute Actions (Entry/Exit) ---
        if action == 'EXIT':
            exit_price = current_price

            # P&L Calculation: Correct for LONG VIX position
            gross_p_l = (exit_price - entry_price) * units_held
            total_commission = units_held * params.commission_per_unit

            # FIX: 'net_p_l' is now explicitly defined here
            net_p_l = gross_p_l - total_commission

            # Update running capital and DataFrame column
            running_capital += net_p_l
            df.loc[current_date, 'Capital'] = running_capital
            df.loc[current_date, 'Realized_P_L'] = net_p_l
            df.loc[current_date, 'Exit_Reason'] = exit_reason

            # Reset position state variables
            in_position = False
            units_held = 0.0
            entry_price = 0.0
            peak_price = 0.0

        elif action == 'BUY' and not in_position:
            units_held = units_to_trade
            entry_price = current_price

            df.loc[current_date, 'Position_Size'] = units_held
            df.loc[current_date, 'Entry_Price'] = entry_price

            # NOTE: Capital is NOT reduced on entry (assuming margin/futures)
            # If you need to deduct cash for the full price, you would update running_capital here.

            in_position = True
            peak_price = entry_price

        # --- Carry Capital Forward ---
        # If 'Capital' wasn't updated by an EXIT event today, carry the running_capital forward
        if pd.isna(df.loc[current_date, 'Capital']):
            df.loc[current_date, 'Capital'] = running_capital

    # --- FINAL LIQUIDATION LOGIC (at end of loop) ---
    if in_position:
        # ... (Liquidation logic remains mostly the same, as it defines net_p_l locally) ...
        final_date = df.index[-1]
        final_price = df.loc[final_date, params.price_column]

        gross_p_l = (final_price - entry_price) * units_held
        total_commission = units_held * params.commission_per_unit
        net_p_l = gross_p_l - total_commission

        # Use the last recorded capital value
        df.loc[final_date, 'Capital'] += net_p_l
        df.loc[final_date, 'Realized_P_L'] += net_p_l

    return df




# ----------------------------------------------------
# 4. P&L REPORTING FUNCTION (Externalized Metrics)
# ----------------------------------------------------
def calculate_backtest_metrics(df: pd.DataFrame) -> dict:
    """Calculates key performance metrics from the simulation results DataFrame."""

    # Filter closed trades
    closed_trades = df[df['Realized_P_L'].abs() > 0.0001].copy()

    if df.empty or len(closed_trades) == 0:
        return {'Error': 'No trades executed or DataFrame is empty.'}

    # Capital and P&L
    initial_capital = df['Capital'].iloc[0]
    final_capital = df['Capital'].iloc[-1]
    total_pnl = final_capital - initial_capital

    # Max Drawdown
    peak_capital = df['Capital'].expanding().max()
    drawdown = (peak_capital - df['Capital']) / peak_capital
    max_drawdown = drawdown.max() * 100

    # Win/Loss Metrics
    win_trades = closed_trades[closed_trades['Realized_P_L'] > 0]
    loss_trades = closed_trades[closed_trades['Realized_P_L'] < 0]

    win_rate = len(win_trades) / len(closed_trades) * 100
    avg_win = win_trades['Realized_P_L'].mean()
    avg_loss = loss_trades['Realized_P_L'].mean()

    # Risk/Reward Ratio
    risk_reward_ratio = abs(avg_win / avg_loss) if avg_loss is not None and avg_loss != 0 else np.nan

    metrics = {
        'Initial Capital': initial_capital,
        'Final Capital': final_capital,
        'Total P&L': total_pnl,
        'Max Drawdown (%)': max_drawdown,
        'Total Closed Trades': len(closed_trades),
        'Win Rate (%)': win_rate,
        'Average Win ($)': avg_win,
        'Average Loss ($)': avg_loss,
        'Risk/Reward Ratio (AW/AL)': risk_reward_ratio,
        'Max Drawdown Date': df['Capital'].idxmin().strftime('%Y-%m-%d')
    }

    return metrics





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

    def run_backtest_simulation(self, mock_df: pd.DataFrame, price_column: str = 'close',
                                             initial_capital: float = 10000.0,
                                             trailing_stop_pct: float = 0.10, take_profit_pct: float = 0.20,
                                             position_sizing_ratio: float = 1.0, commission_per_unit: float = 0.01):
        """
        Simulates the long-only trading strategy incorporating Trailing Stop-Loss and
        a PERCENTAGE-BASED Take Profit (take_profit_pct) that scales with entry price.

        :param take_profit_pct: Percentage gain from entry price to trigger exit (e.g., 0.30 for 30%).
        """
        # Example of how you would kick off the backtest:

        # 1. (Assume mock_df is your loaded price data)
        params = BacktestParameters(
                initial_capital=initial_capital,
                trailing_stop_pct=0.10,  # TSL 10%
                take_profit_pct=0.50,    # TP 50%
                max_risk_pct=0.040,      # Max Risk 4.0%
                # commission_per_unit uses your default
            )

        # 2. Run the simulation
        results_df = run_simulation_engine(mock_df, params)

        # 3. Calculate metrics
        final_metrics = calculate_backtest_metrics(results_df)

        print(final_metrics)

        results_df.to_csv('c:/Temp/VIX_NewBacktest.csv')



    def test_cot_simulation(self):


        # prepare data
        print('... Gettingn data ....')
        key = os.environ['FMPREPKEY']
        vix_prices = get_historical_prices('^VIX', datetime.date(2004, 7, 20), key)
        spx_prices = get_historical_prices('^GSPC', datetime.date(2004, 7, 20), key)

        cot_df = get_latest_cot()
        # Step 2. calcuclate sentiment
        print('... Calculating Sentiment ....')
        calculator = VixSentimentCalculator(cot_lookback_period=52 * 5, oi_lookback_period=52 * 1)
        res = calculator.calculate_sentiment(pd.DataFrame(vix_prices), cot_df, pd.DataFrame(spx_prices))
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

        signal_gen = DualFactorSignalGenerator(
            cot_buy_threshold=10.0,
            spx_shock_threshold=-0.015,
            traded_asset_col='VIX_close'  # Specify the price series
        )

        # 2. Process the data (the single point of entry for the DataFrame)
        # 'prepared_data_df' is the output from your VixSentimentCalculator
        signal_gen.process_data(res)

        # 3. Retrieve the final, backtest-ready data (NO parameters needed!)
        mock_df = signal_gen.get_backtest_data()
        #generator = DualFactorSignalGenerator() # SignalGenerator(res, optimal_lookback)
        #mock_df = generator.get_backtest_data()

        initial_capital = 20000.0

        # NOTE: Using the default price_column='close'
        self.run_backtest_simulation(mock_df, initial_capital=initial_capital)

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
