import pandas as pd
import numpy as np
from typing import Dict

import pandas as pd
import numpy as np
from typing import Dict, Any


class VixSentimentCalculator:
    def __init__(self, vix_df: pd.DataFrame, spx_df: pd.DataFrame, cot_df: pd.DataFrame):
        # Assuming these dataframes are pre-loaded and assigned
        self.vix_df = vix_df
        self.spx_df = spx_df
        self.cot_df = cot_df
        self.df = pd.DataFrame()  # The final merged DataFrame

    def _prepare_vix_data(self) -> pd.DataFrame:
        vix_df = self.vix_df.copy()
        vix_df.index = pd.to_datetime(vix_df.index)
        vix_df = vix_df.sort_index()
        vix_df = vix_df.rename(columns={'close': 'VIX_close'})

        # NEW: Calculate 200-day MA and VIX Ratio for spike filtering
        vix_df['VIX_MA200'] = vix_df['VIX_close'].rolling(window=200).mean()
        vix_df['VIX_Ratio'] = vix_df['VIX_close'] / vix_df['VIX_MA200']

        return vix_df[['VIX_close', 'VIX_MA200', 'VIX_Ratio']]

    def _prepare_spx_data(self) -> pd.DataFrame:
        spx_df = self.spx_df.copy()
        spx_df.index = pd.to_datetime(spx_df.index)
        spx_df = spx_df.sort_index().rename(columns={'close': 'SPX_close'})

        # Calculate 1-day and 10-day Rate of Change
        spx_df['SPX_1D_Change'] = spx_df['SPX_close'].pct_change(1)
        spx_df['SPX_10D_Change'] = spx_df['SPX_close'].pct_change(10)

        return spx_df[['SPX_close', 'SPX_1D_Change', 'SPX_10D_Change']]

    def calculate_sentiment(self) -> pd.DataFrame:
        # Placeholder for full merging logic (assumes merging of VIX, SPX, COT data)
        # Final merged data is stored in self.df
        self.df = pd.merge(self._prepare_vix_data(), self._prepare_spx_data(),
                           left_index=True, right_index=True, how='inner')
        # ... (Include COT data merge here) ...
        return self.df