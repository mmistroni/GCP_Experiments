import pandas as pd
import numpy as np

class VixSentimentCalculator:
    def __init__(self, cot_lookback_period: int = 156, oi_lookback_period: int = 52):
        self.cot_lookback_period = cot_lookback_period
        self.oi_lookback_period = oi_lookback_period
        self.VIX_CODE = '1170E1'
        self.COT_COLUMNS = {
            'noncomm_positions_long_all': 'noncomm_long',
            'noncomm_positions_short_all': 'noncomm_short',
            'open_interest_all': 'open_interest'
        }

    def _prepare_vix_cot_data(self, cot_df: pd.DataFrame) -> pd.DataFrame:
        """Cleans raw COT data and calculates Net Positioning."""
        df = cot_df.reset_index()
        vix_cot = df[df['cftc_contract_market_code'] == self.VIX_CODE].copy()
        vix_cot = vix_cot.rename(columns=self.COT_COLUMNS)

        vix_cot['date'] = pd.to_datetime(vix_cot['date'])
        vix_cot = vix_cot.set_index('date').sort_index()

        vix_cot['noncomm_net'] = vix_cot['noncomm_long'] - vix_cot['noncomm_short']
        vix_cot['vix_oi_ratio'] = (
                vix_cot['open_interest'].rolling(window=self.oi_lookback_period).mean() /
                vix_cot['open_interest']
        )
        return vix_cot[['noncomm_net', 'open_interest', 'vix_oi_ratio']]

    def _calculate_cot_index(self, vix_cot_df: pd.DataFrame) -> pd.DataFrame:
        """Calculates the 0-100 normalized COT Index."""
        net = vix_cot_df['noncomm_net']
        rolling_max = net.rolling(window=self.cot_lookback_period).max()
        rolling_min = net.rolling(window=self.cot_lookback_period).min()

        denom = (rolling_max - rolling_min).replace(0, np.nan)
        vix_cot_df['vix_cot_index'] = ((net - rolling_min) / denom) * 100
        return vix_cot_df

    def _prepare_vix_price_data(self, vix_df: pd.DataFrame) -> pd.DataFrame:
        """Resamples daily VIX/Contango data to Tuesday (COT Release Day)."""
        vix_df = vix_df.copy()
        vix_df.index = pd.to_datetime(vix_df.index)

        # Resample to Tuesday (W-TUE) to match COT alignment
        vix_weekly = vix_df.resample('W-TUE').agg({
            'Spot_VIX': 'last',
            'Contango_Pct': 'mean'
        })

        return vix_weekly.rename(columns={'Spot_VIX': 'vix_close'})

    def _label_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies human-readable labels to the COT Index scores."""
        conditions = [
            (df['vix_cot_index'] <= 10),
            (df['vix_cot_index'] <= 30),
            (df['vix_cot_index'] >= 90),
            (df['vix_cot_index'] >= 70)
        ]
        choices = ['Extreme Bullish', 'Bullish', 'Extreme Bearish', 'Bearish']
        df['sentiment_label'] = np.select(conditions, choices, default='Neutral/Moderate')
        return df

    def calculate_sentiment(self, vix_df: pd.DataFrame, cot_df: pd.DataFrame) -> pd.DataFrame:
        """MAIN ENTRY: Orchestrates the calculation pipeline."""
        # 1. Prepare raw COT columns
        vix_cot_data = self._prepare_vix_cot_data(cot_df)

        # 2. Add the normalized Index
        vix_cot_indexed = self._calculate_cot_index(vix_cot_data)

        # 3. Prepare the weekly price data
        vix_weekly_price = self._prepare_vix_price_data(vix_df)

        # 4. Merge Sentiment with Price
        final_df = vix_cot_indexed.merge(
            vix_weekly_price, left_index=True, right_index=True, how='inner'
        )

        # 5. Apply Labels and cleanup
        return self._label_sentiment(final_df).dropna(subset=['vix_cot_index'])
