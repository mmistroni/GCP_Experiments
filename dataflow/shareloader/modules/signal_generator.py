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

import pandas as pd
import numpy as np
from typing import Optional


class DualFactorSignalGenerator:
    """
    Generates signals by combining:
    1. Weekly VIX COT Index (Extreme Sentiment)
    2. Daily SPX Momentum (Panic Filter)
    """

    def __init__(
            self,
            cot_buy_threshold: float = 20.0,
            spx_shock_threshold: float = -0.015,
            traded_asset_col: str = 'vix_close'
    ):
        self.cot_buy_threshold = cot_buy_threshold
        self.spx_shock_threshold = spx_shock_threshold
        self.traded_asset_col = traded_asset_col
        self.df: Optional[pd.DataFrame] = None

    def process_data(self, df: pd.DataFrame, hold_days: int = 20) -> pd.DataFrame:
        """
        AMENDED: Now accepts hold_days dynamically from the Analyzer results.
        """
        # Ensure we use the lowercase name from your VixSentimentCalculator
        if self.traded_asset_col not in df.columns and 'Spot_VIX' in df.columns:
            df = df.rename(columns={'Spot_VIX': self.traded_asset_col})

        self.df = df.copy()

        # 1. Forward-fill weekly COT data to make it daily-ready
        if 'vix_cot_index' in self.df.columns:
            self.df['vix_cot_index'] = self.df['vix_cot_index'].ffill()

        # 2. Generate Signals (COT + SPX Panic)
        self._generate_signals()

        # 3. Add Dynamic Hold Period found by the Analyzer
        self.df['Hold_Period_Days'] = hold_days

        return self.get_backtest_data()

    def _generate_signals(self) -> None:
        df = self.df

        # 1. Forward-fill the COT Index so it's available every day of the week
        df['vix_cot_index'] = df['vix_cot_index'].ffill()

        # 2. Relax the SPX threshold slightly (e.g., -1.5% instead of -2%)
        # And check for the "Decimal vs Percent" issue
        spx_panic = df['SPX_1D_Change'] <= self.spx_shock_threshold

        # 3. COT Condition (Speculators are heavily short VIX)
        cot_bullish = df['vix_cot_index'] <= self.cot_buy_threshold

        # --- DUAL FACTOR LOGIC ---
        # We use np.where for speed and clarity
        df['Trade_Signal'] = np.where(
            spx_panic & cot_bullish,
            'BUY',
            'NEUTRAL'
        )

        # DEBUG: Print how many signals were found
        signal_count = (df['Trade_Signal'] == 'BUY').sum()
        print(f"DEBUG: Found {signal_count} BUY signals out of {len(df)} rows.")

    def get_backtest_data(self) -> pd.DataFrame:
        if self.df is None:
            raise RuntimeError("Call .process_data(df) first.")

        required_cols = [
            self.traded_asset_col,
            'vix_cot_index',
            'SPX_1D_Change',
            'Trade_Signal',
            'Hold_Period_Days'
        ]

        if 'Contango_Pct' in self.df.columns:
            required_cols.append('Contango_Pct')

        return self.df[required_cols].dropna(subset=['Trade_Signal'])