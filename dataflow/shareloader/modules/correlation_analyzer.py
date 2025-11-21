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
    and the future VIX price change (Y) for multiple lookback periods.
    """
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.cot_index_col = 'vix_cot_index'
        self.vix_close_col = 'VIX_close'
        self.results = {}

    def _calculate_vix_cot_index(self, lookback_period: int) -> pd.Series:
        """
        Recalculates the VIX COT Index using a specified lookback period.
        """
        net_position = self.df['noncomm_net']

        # Calculate the rolling maximum and minimum of the Net Position
        if len(net_position) < lookback_period:
            return pd.Series([np.nan] * len(net_position), index=net_position.index)

        rolling_max = net_position.rolling(window=lookback_period).max()
        rolling_min = net_position.rolling(window=lookback_period).min()

        # COT Index Formula: (Net - Min) / (Max - Min) * 100
        cot_index = ((net_position - rolling_min) / (rolling_max - rolling_min)) * 100

        return cot_index

    def _calculate_future_change(self, lag: int) -> pd.Series:
        """
        Calculates the VIX price percent change over the specified future lag.
        """
        # The change is calculated from time t to t + lag
        # .shift(-lag) moves the future value (t+lag) back to row t
        future_vix = self.df[self.vix_close_col].shift(-lag)
        current_vix = self.df[self.vix_close_col]

        # VIX Future Change = (Future VIX - Current VIX) / Current VIX
        vix_change = (future_vix - current_vix) / current_vix
        return vix_change

    def run_analysis(self, lookback_weeks: list[int] = LOOKBACK_WEEKS,
                     prediction_lags: list[int] = PREDICTION_LAGS) -> dict:
        """
        Runs the correlation test for all combinations of lookback periods and prediction lags.
        """
        print("--- Starting Lagged Correlation Analysis ---")

        for lookback in lookback_weeks:
            # 1. Calculate the COT Index based on the current lookback window
            cot_series = self._calculate_vix_cot_index(lookback)

            # Temporary storage for this lookback's results
            lag_correlations = {}

            for lag in prediction_lags:
                # 2. Calculate the future VIX price change for the current lag
                change_series = self._calculate_future_change(lag)

                # 3. Calculate the Pearson correlation
                # Only use rows where both the COT Index and the Future Change are not NaN
                combined_df = pd.DataFrame({
                    'COT_Index': cot_series,
                    'Future_Change': change_series
                }).dropna()

                # Ensure enough data points for a meaningful correlation
                if len(combined_df) > 50:
                    correlation = combined_df['COT_Index'].corr(combined_df['Future_Change'])
                    lag_correlations[lag] = correlation
                    # print(f"  > COT Lookback: {lookback} weeks, Prediction Lag: {lag} weeks, Correlation: {correlation:.4f}")
                else:
                    lag_correlations[lag] = np.nan
                    # print(f"  > COT Lookback: {lookback} weeks, Prediction Lag: {lag} weeks, (Not enough data)")

            self.results[lookback] = lag_correlations

        return self.results

    def get_results_table(self) -> pd.DataFrame:
        """Returns the results as a formatted DataFrame."""
        return pd.DataFrame(self.results).T.round(4)


def find_smallest_correlation(df: pd.DataFrame):
    """
    Identifies the smallest (most negative) correlation value in the DataFrame,
    and returns its corresponding Lookback (Row Index) and Lag (Column Name).

    The function assumes the DataFrame's index represents the Lookback Weeks
    and the columns represent the Prediction Lags (Holding Periods).
    """
    # 1. Find the MultiIndex location (Row Index, Column Name) of the minimum value
    # .stack() flattens the DataFrame into a Series, indexed by a MultiIndex (Lookback, Lag)
    # .idxmin() finds the MultiIndex tuple that corresponds to the global minimum value.
    min_location = df.stack().idxmin()

    # The row index (Lookback) is an integer
    optimal_lookback = int(min_location[0])

    # The column name (Holding Period/Lag) is an integer (pandas uses the column name)
    optimal_holding_period = int(min_location[1])

    # Retrieve the actual correlation value at that location
    optimal_correlation = df.loc[optimal_lookback, optimal_holding_period]

    return optimal_lookback, optimal_holding_period, optimal_correlation


