import pandas as pd
import numpy as np

# --- Pydantic Reminder ---
# As you are setting up a quantitative strategy, this is a perfect time to apply Pydantic.
# You could use a Pydantic model to enforce that the LOOKBACK_WEEKS and PREDICTION_LAGS
# lists contain only valid, positive integers, ensuring your analysis parameters are robust!
# -------------------------

# --- Configuration ---
# 1. Lookback periods to test (in weeks) - determines how the COT Index is calculated
LOOKBACK_WEEKS = [52, 104, 156, 260] # 1, 2, 3, and 5 years

# 2. Prediction lags to test (in weeks) - determines how far forward we look for VIX change
PREDICTION_LAGS = [2, 4, 8, 12]

# File to load
INPUT_FILE = 'outputs_vix_analysis_dated_with_sentiment.csv'
# ---------------------


class CorrelationAnalyzer:
    """
    Analyzes the lagged correlation between the VIX COT Index (X)
    and the future VIX price change (Y).
    """

    def __init__(self, df: pd.DataFrame):
        # Ensure we are working with a clean copy
        self.df = df.copy()

        # FIXED: Match the column names produced by VixSentimentCalculator
        self.cot_index_col = 'vix_cot_index'
        self.vix_close_col = 'vix_close'  # Changed from VIX_close to vix_close
        self.results = {}

    def _calculate_vix_cot_index(self, lookback_period: int) -> pd.Series:
        # Check if 'noncomm_net' exists (it comes from the COT part of the previous DF)
        if 'noncomm_net' not in self.df.columns:
            raise KeyError("DataFrame must contain 'noncomm_net' column from COT data.")

        net_position = self.df['noncomm_net']

        if len(net_position) < lookback_period:
            return pd.Series([np.nan] * len(net_position), index=net_position.index)

        rolling_max = net_position.rolling(window=lookback_period).max()
        rolling_min = net_position.rolling(window=lookback_period).min()

        # Added small epsilon to avoid division by zero
        cot_index = ((net_position - rolling_min) / (rolling_max - rolling_min + 1e-9)) * 100
        return cot_index

    def _calculate_future_change(self, lag: int) -> pd.Series:
        """
        Calculates the VIX price percent change over the specified future lag.
        """
        # Since the data is now weekly (Tuesdays), a lag of 4 = 4 weeks.
        future_vix = self.df[self.vix_close_col].shift(-lag)
        current_vix = self.df[self.vix_close_col]

        # Percent change formula
        vix_change = (future_vix - current_vix) / current_vix
        return vix_change

    def run_analysis(self, lookback_weeks: list[int] = LOOKBACK_WEEKS,
                     prediction_lags: list[int] = PREDICTION_LAGS) -> dict:
        self.results = {}  # Reset results

        for lookback in lookback_weeks:
            cot_series = self._calculate_vix_cot_index(lookback)
            lag_correlations = {}

            for lag in prediction_lags:
                change_series = self._calculate_future_change(lag)

                combined_df = pd.DataFrame({
                    'COT_Index': cot_series,
                    'Future_Change': change_series
                }).dropna()

                # We need a decent sample size for weekly data
                if len(combined_df) > 26:  # Half a year of weekly data
                    correlation = combined_df['COT_Index'].corr(combined_df['Future_Change'])
                    lag_correlations[lag] = correlation
                else:
                    lag_correlations[lag] = np.nan

            self.results[lookback] = lag_correlations

        return self.results

    def get_results_table(self) -> pd.DataFrame:
        # Create table where Index = Lookback and Columns = Lag
        return pd.DataFrame(self.results).T.sort_index()


def find_smallest_correlation(results_df: pd.DataFrame):
    """
    Identifies the best parameters based on the strongest negative correlation.
    Returns: (Lookback Weeks, Holding Period Weeks, Correlation Value)
    """
    # .stack() turns the table into a Series with a MultiIndex (Lookback, Lag)
    stacked = results_df.stack()

    # Find the index of the minimum (most negative) correlation
    min_location = stacked.idxmin()

    optimal_lookback = int(min_location[0])
    optimal_holding_period = int(min_location[1])
    optimal_correlation = stacked.loc[min_location]

    return optimal_lookback, optimal_holding_period, optimal_correlation