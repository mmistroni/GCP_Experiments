import pandas as pd


# Assume EXTREME_LOW_PERCENTILE, EXTREME_HIGH_PERCENTILE, and OPTIMAL_LOOKBACK_WEEKS
# are defined globally (e.g., 0.10, 0.90, and 156)
INPUT_FILE = 'outputs_vix_analysis_dated_with_sentiment.csv'
OPTIMAL_LOOKBACK_WEEKS = 156 # 3 Years
EXTREME_LOW_PERCENTILE = 0.10 # 10th percentile for Extreme Bullish (BUY VIX)
EXTREME_HIGH_PERCENTILE = 0.90 # 90th percentile for Extreme Bearish (SELL VIX)
# --------
class SignalGenerator:
    """
    Calculates the VIX COT Index and dynamic Buy/Sell thresholds across the entire
    historical period and compiles the final Signal DataFrame for backtesting.
    """

    def __init__(self, df: pd.DataFrame, optimal_lookback_weeks: int):
        self.df = df
        self.optimal_lookback = optimal_lookback_weeks
        self.df = self.df.sort_index()  # Ensure data is sorted by date/index

        # 1. Calculate the core COT Index series
        self.cot_index_series = self._calculate_vix_cot_index(self.optimal_lookback)

        # 2. Calculate dynamic thresholds for the entire history
        self.buy_vix_thresholds = self.cot_index_series.rolling(
            window=self.optimal_lookback
        ).quantile(EXTREME_LOW_PERCENTILE).shift(1)  # Shift(1) to prevent look-ahead bias

        self.sell_vix_thresholds = self.cot_index_series.rolling(
            window=self.optimal_lookback
        ).quantile(EXTREME_HIGH_PERCENTILE).shift(1)  # Shift(1) to prevent look-ahead bias

        # 3. Compile the final signal DataFrame
        self.signal_df = self._compile_signal_dataframe()

    def _calculate_vix_cot_index(self, lookback_period: int) -> pd.Series:
        """Calculates the VIX COT Index using a specified lookback period."""
        net_position = self.df['noncomm_net']

        # Calculate the rolling maximum and minimum of the Net Position
        rolling_max = net_position.rolling(window=lookback_period).max()
        rolling_min = net_position.rolling(window=lookback_period).min()

        # COT Index Formula: (Net - Min) / (Max - Min) * 100
        cot_index = ((net_position - rolling_min) / (rolling_max - rolling_min)) * 100

        return cot_index

    def _compile_signal_dataframe(self) -> pd.DataFrame:
        """Creates the final, comprehensive DataFrame needed for backtesting."""

        # Start with a copy of the key data
        df_out = self.df[['noncomm_net', 'open_interest', 'close']].copy()

        # Add the calculated index and dynamic thresholds
        df_out['COT_Index'] = self.cot_index_series
        df_out['Buy_Threshold'] = self.buy_vix_thresholds
        df_out['Sell_Threshold'] = self.sell_vix_thresholds

        # Generate the signal for every day
        def generate_signal(row):
            if row['COT_Index'] <= row['Buy_Threshold']:
                return "BUY"
            elif row['COT_Index'] >= row['Sell_Threshold']:
                return "SELL"
            else:
                return "NEUTRAL"

        # Apply the signal generation function
        df_out['Trade_Signal'] = df_out.apply(generate_signal, axis=1)

        # Add the look-forward period (12 weeks, which is ~60 trading days)
        df_out['Hold_Period_Days'] = 60  # Set to 60 trading days for 12 weeks

        return df_out.dropna(subset=['COT_Index'])  # Drop initial lookback nulls

    def get_backtest_data(self) -> pd.DataFrame:
        """Public method to return the full backtesting DataFrame."""
        return self.signal_df

    def get_current_signal(self):
        """
        Keeps the original print function for displaying the latest signal,
        but retrieves values from the compiled DataFrame.
        """
        latest_row = self.signal_df.iloc[-1]
        latest_cot_index = latest_row['COT_Index']
        latest_date = latest_row.name.strftime('%Y-%m-%d')  # Assuming index is DatetimeIndex

        # Use the stored thresholds for the printout
        buy_threshold = latest_row['Buy_Threshold']
        sell_threshold = latest_row['Sell_Threshold']

        signal = latest_row['Trade_Signal']

        # --- Print Logic (Optimized) ---
        print("\n--- VIX COT Strategy Signal Report ---")
        print(f"Optimal Lookback Period Used: {self.optimal_lookback} Weeks")
        print(f"Latest COT Report Date: {latest_date}")
        print("-" * 40)
        print("DYNAMIC THRESHOLDS (Calculated from Historical Index Data):")
        print(
            f"  1. EXTREME BULLISH (BUY VIX) Threshold ({EXTREME_LOW_PERCENTILE * 100:.0f}%-ile): {buy_threshold:.2f}%")
        print(
            f"  2. EXTREME BEARISH (SELL VIX) Threshold ({EXTREME_HIGH_PERCENTILE * 100:.0f}%-ile): {sell_threshold:.2f}%")
        print("-" * 40)
        print(f"LATEST VIX COT INDEX VALUE: {latest_cot_index:.2f}%")
        print(f"GENERATED SIGNAL: {signal}")
        print("Expected Trade Duration: 12 Weeks (60 Trading Days)")

        return latest_cot_index, signal


import pandas as pd
import numpy as np

import pandas as pd
import numpy as np


# Assuming the DataFrame 'df' is available and contains:
# 'vix_cot_index', 'SPX_10D_Change', and 'VIX_close' (or whatever price series you trade)

import pandas as pd
import numpy as np
from typing import Optional

import pandas as pd
import numpy as np
from typing import Optional, Dict


class DualFactorSignalGenerator:
    """
    Generates VIX long/short trading signals based on a Dual-Factor Strategy:
    1. VIX COT Index (Extreme Sentiment)
    2. SPX 10-Day Momentum (Market Shock Filter)

    This class processes the prepared data and outputs all columns required
    by an Event-Driven Backtest Engine (e.g., VIX factors, Trade_Signal, Hold_Period).
    """

    def __init__(
            self,
            cot_buy_threshold: float = 10.0,
            spx_shock_threshold: float = -0.010,  # -1.0% drop over 10 days
            cot_sell_threshold: float = 90.0,
            traded_asset_col: str = 'VIX_close',  # The price column for P&L calculation
            fixed_hold_period_days: int = 20  # Default holding period (e.g., 4 weeks)
    ):
        """
        Initializes the signal generator with key strategy parameters.
        """
        self.cot_buy_threshold = cot_buy_threshold
        self.spx_shock_threshold = spx_shock_threshold
        self.cot_sell_threshold = cot_sell_threshold
        self.traded_asset_col = traded_asset_col
        self.fixed_hold_period_days = fixed_hold_period_days
        self.df: Optional[pd.DataFrame] = None  # Stores the processed data internally

    def process_data(self, df: pd.DataFrame) -> None:
        """
        Accepts the raw, prepared daily DataFrame, validates it, and stores it internally.
        It then generates the trading signals and the hold period column.
        """
        required_input_cols = ['vix_cot_index', 'SPX_10D_Change', self.traded_asset_col]

        # Validation checks
        missing = [col for col in required_input_cols if col not in df.columns]
        if missing:
            raise ValueError(
                f"DataFrame missing required columns: {missing}. "
                f"Ensure 'vix_cot_index' is calculated with the optimal lookback."
            )

        self.df = df.copy()

        # 1. Generate the instantaneous signals ('BUY', 'SELL', 'NEUTRAL')
        self._generate_signals()

        # 2. Add the crucial 'Hold_Period_Days' column for the backtester's Exit_Date_Target
        self.df['Hold_Period_Days'] = self.fixed_hold_period_days

    import numpy as np
    import pandas as pd

    def _generate_signals(self) -> None:
        """
        Applies the Hybrid Strategy signal: Single-Factor SPX Panic Entry.

        This uses a tight 1-day SPX shock filter to generate the 'BUY' signal
        for a Long VIX position, isolating high-conviction panic days.
        """
        df = self.df

        # --- ENTRY CONDITION (Single-Factor Panic Filter) ---
        # We use the SPX 1-Day Change and the tighter -0.015 threshold
        # (as defined in your self.spx_shock_threshold parameter)

        # 1. Check for the existence of the required 1-Day Change column
        if 'SPX_1D_Change' not in df.columns:
            raise KeyError(
                "Missing 'SPX_1D_Change' column. "
                "Ensure the VixSentimentCalculator is updated to calculate 1-day momentum."
            )

        # Condition: SPX drops by at least 1.5% in one day (or the threshold set in params)
        panic_condition = (df['SPX_1D_Change'] <= self.spx_shock_threshold)

        # --- SIGNAL GENERATION ---
        # The signal is simply 'BUY' if the panic condition is met.
        # We explicitly remove the 'SELL' condition and the dual-factor COT condition.
        df['Trade_Signal'] = np.select(
            [
                panic_condition  # Single-Factor BUY condition
            ],
            [
                'BUY'
            ],
            default='NEUTRAL'
        )

        # --- NOTE ON HOLD PERIOD ---
        # The 'Hold_Period_Days' column, used for time-based exits,
        # should be calculated elsewhere or set to a large fixed value (e.g., 30 days)
        # to allow the TSL and TP logic in the StrategyEngine to control the exit.
    def get_backtest_data(self) -> pd.DataFrame:
        """
        Returns the processed DataFrame containing ALL factors, price, and signals
        required by the Event-Driven Backtest Engine (e.g., run_simulation_engine).

        Requires NO PARAMETERS.
        """
        if self.df is None:
            raise RuntimeError("Data has not been processed. Call .process_data(df) first.")

        # Return all columns needed by the StrategyEngine and the main loop.
        required_cols = [
            self.traded_asset_col,
            'vix_cot_index',
            'SPX_10D_Change',
            'Trade_Signal',
            'Hold_Period_Days'
        ]

        # Final check on columns
        missing = [col for col in required_cols if col not in self.df.columns]
        if missing:
            # Should not happen if _generate_signals runs correctly, but good for safety
            raise KeyError(f"Internal processing error: Final DataFrame is missing columns: {missing}")

        return self.df[required_cols].dropna()

# --- Example Usage (Class instantiation) ---
if __name__ == '__main__':
    # --- Mock Data for demonstration ---
    data = {
        'vix_cot_index': [5.0, 15.0, 8.0, 12.0, 95.0, 3.0],
        'SPX_10D_Change': [-0.015, -0.005, -0.012, -0.015, 0.002, -0.020]
    }
    dates = pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05', '2023-01-06'])
    prepared_df = pd.DataFrame(data, index=dates)

    # 1. Initialize the Generator with specific parameters (e.g., 5th percentile COT, -1.5% SPX shock)
    signal_gen = DualFactorSignalGenerator(
        cot_buy_threshold=5.0,  # Only use the most extreme 5% of COT data
        spx_shock_threshold=-0.015  # Only buy after a -1.5% drop or worse
    )

    # 2. Generate the signals
    signals_df = signal_gen.generate_signals(prepared_df.copy())

    print("--- Strategy Signals Generated (Class-Based) ---")
    print(signals_df[['vix_cot_index', 'SPX_10D_Change', 'Trade_Signal']])

    # Interpretation with new thresholds (5.0 and -0.015):
    # 2023-01-01: COT=5.0 (Hit) AND SPX=-1.5% (Hit) -> BUY
    # 2023-01-03: COT=8.0 (Miss) -> NEUTRAL
    # 2023-01-06: COT=3.0 (Hit) AND SPX=-2.0% (Hit) -> BUY