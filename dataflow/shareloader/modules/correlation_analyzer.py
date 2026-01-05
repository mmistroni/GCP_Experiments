import pandas as pd
import numpy as np
from typing import List, Dict
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
    def __init__(self, df: pd.DataFrame):
        self.df = df # Accepts the result of SentimentCalculator

    def run_analysis(self, lags: List[int] = [4, 8, 12]) -> Dict[int, float]:
        results = {}
        for lag in lags:
            future_change = (self.df['vix_close'].shift(-lag) - self.df['vix_close']) / self.df['vix_close']
            corr = self.df['vix_cot_index'].corr(future_change)
            results[lag] = corr
        return results

def find_smallest_correlation(all_results: dict):
    df_results = pd.DataFrame(all_results).T
    try:
        min_loc = df_results.stack().idxmin() # Finding most negative correlation
        return int(min_loc[0]), int(min_loc[1]), df_results.loc[min_loc]
    except:
        return None, None, None