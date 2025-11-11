import pandas as pd
import numpy as np

# --- Pydantic Reminder ---
# After finding these final numerical thresholds, you should lock them into a
# Pydantic model for your strategy configuration. This ensures that your trading
# agent or backtest tool always uses validated, statistically optimal parameters.
# -------------------------

# --- Configuration (Based on Correlation Analysis) ---
INPUT_FILE = 'outputs_vix_analysis_dated_with_sentiment.csv'
OPTIMAL_LOOKBACK_WEEKS = 156 # 3 Years
EXTREME_LOW_PERCENTILE = 0.10 # 10th percentile for Extreme Bullish (BUY VIX)
EXTREME_HIGH_PERCENTILE = 0.90 # 90th percentile for Extreme Bearish (SELL VIX)
# -----------------------------------------------------


class SignalGenerator:
    """
    Calculates the VIX COT Index using the optimal 156-week lookback and
    determines the dynamic Buy/Sell thresholds and the current signal.
    """
    def __init__(self, df: pd.DataFrame, optimal_lookback:int):
        self.df = df
        # Recalculate the COT index using the optimal 156-week lookback
        self.cot_index_series = self._calculate_vix_cot_index(optimal_lookback)

        # Calculate the dynamic thresholds from the newly calculated COT Index
        self.buy_vix_threshold = self.cot_index_series.quantile(EXTREME_LOW_PERCENTILE)
        self.sell_vix_threshold = self.cot_index_series.quantile(EXTREME_HIGH_PERCENTILE)

    def _calculate_vix_cot_index(self, lookback_period: int) -> pd.Series:
        """
        Calculates the VIX COT Index using a specified lookback period.
        """
        net_position = self.df['noncomm_net']

        # Calculate the rolling maximum and minimum of the Net Position
        rolling_max = net_position.rolling(window=lookback_period).max()
        rolling_min = net_position.rolling(window=lookback_period).min()

        # COT Index Formula: (Net - Min) / (Max - Min) * 100
        # The index is only valid when the window is full (after 'lookback_period' weeks)
        cot_index = ((net_position - rolling_min) / (rolling_max - rolling_min)) * 100

        return cot_index

    def get_current_signal(self):
        """
        Determines the current signal based on the latest COT Index value.
        """
        # Get the latest valid COT Index value
        latest_cot_index = self.cot_index_series.dropna().iloc[-1]
        latest_date = self.cot_index_series.dropna().index[-1].strftime('%Y-%m-%d')

        signal = "Neutral"

        if latest_cot_index <= self.buy_vix_threshold:
            signal = "EXTREME BULLISH (BUY VIX) - Expected VIX move in 12 weeks"
        elif latest_cot_index >= self.sell_vix_threshold:
            signal = "EXTREME BEARISH (SELL VIX) - Expected VIX move in 12 weeks"
        else:
            # Check for moderate signals for more context (using 30th/70th percentiles)
            if latest_cot_index < self.cot_index_series.quantile(0.30):
                 signal = "Bullish/Complacent (Monitor for Extreme)"
            elif latest_cot_index > self.cot_index_series.quantile(0.70):
                 signal = "Bearish/Fearful (Monitor for Extreme)"


        print("\n--- VIX COT Strategy Signal Report ---")
        print(f"Optimal Lookback Period Used: {OPTIMAL_LOOKBACK_WEEKS} Weeks (3 Years)")
        print(f"Latest COT Report Date: {latest_date}")
        print("-" * 40)
        print("DYNAMIC THRESHOLDS (Calculated from 156-Week COT Index History):")
        print(f"  1. EXTREME BULLISH (BUY VIX) Threshold (10th %-ile): {self.buy_vix_threshold:.2f}%")
        print(f"  2. EXTREME BEARISH (SELL VIX) Threshold (90th %-ile): {self.sell_vix_threshold:.2f}%")
        print("-" * 40)
        print(f"LATEST VIX COT INDEX VALUE: {latest_cot_index:.2f}%")
        print(f"GENERATED SIGNAL: {signal}")
        print("Expected Trade Duration: 12 Weeks")

        return latest_cot_index, signal


