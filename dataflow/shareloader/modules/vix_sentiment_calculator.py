import pandas as pd
import numpy as np
class VixSentimentCalculator:
    def __init__(self, cot_lookback_period: int = 52 * 5, oi_lookback_period: int = 52 * 1):
        self.cot_lookback_period = cot_lookback_period
        self.oi_lookback_period = oi_lookback_period
        self.COT_COLUMNS = {
            'noncomm_positions_long_all': 'noncomm_long',
            'noncomm_positions_short_all': 'noncomm_short',
            'open_interest_all': 'open_interest',
            'commodity': 'commodity_name'
        }
        self.VIX_CODE = '1170E1'

    def _prepare_vix_cot_data(self, cot_df: pd.DataFrame) -> pd.DataFrame:
        # COT data often comes with a multi-index or string dates from OpenBB
        cot_df = cot_df.reset_index()
        vix_cot_df = cot_df[cot_df['cftc_contract_market_code'] == self.VIX_CODE].copy()
        vix_cot_df = vix_cot_df.rename(columns=self.COT_COLUMNS)

        # Standardize COT date to Tuesday index
        vix_cot_df['date'] = pd.to_datetime(vix_cot_df['date'])
        vix_cot_df = vix_cot_df.set_index('date').sort_index()

        vix_cot_df['noncomm_net'] = vix_cot_df['noncomm_long'] - vix_cot_df['noncomm_short']
        vix_cot_df['vix_oi_ratio'] = (
                vix_cot_df['open_interest'].rolling(window=self.oi_lookback_period).mean() /
                vix_cot_df['open_interest']
        )
        return vix_cot_df[['noncomm_net', 'open_interest', 'vix_oi_ratio']]

    def _prepare_vix_price_data(self, vix_df: pd.DataFrame) -> pd.DataFrame:
        """
        FIXED: Now handles 'Spot_VIX' and 'Contango_Pct' from your vix_utils output.
        """
        # Your vix_utils code returns Trade Date as index. Let's make sure it's datetime.
        vix_df.index = pd.to_datetime(vix_df.index)

        # Resample to Tuesday (COT Day)
        # We take the mean of Contango and the last Spot price of the week
        vix_weekly = vix_df.resample('W-TUE').agg({
            'Spot_VIX': 'last',
            'Contango_Pct': 'mean'
        })

        # Rename for internal consistency with your calculator logic
        vix_weekly = vix_weekly.rename(columns={'Spot_VIX': 'vix_close'})
        return vix_weekly

    def _calculate_cot_index(self, vix_cot_df: pd.DataFrame) -> pd.DataFrame:
        net_position = vix_cot_df['noncomm_net']
        rolling_max = net_position.rolling(window=self.cot_lookback_period).max()
        rolling_min = net_position.rolling(window=self.cot_lookback_period).min()

        # Avoid division by zero if data is flat
        denom = (rolling_max - rolling_min).replace(0, np.nan)
        vix_cot_df['vix_cot_index'] = ((net_position - rolling_min) / denom) * 100
        return vix_cot_df

    def _label_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        conditions = [
            (df['vix_cot_index'] <= 10), (df['vix_cot_index'] <= 30),
            (df['vix_cot_index'] >= 90), (df['vix_cot_index'] >= 70)
        ]
        choices = ['Extreme Bullish', 'Bullish', 'Extreme Bearish', 'Bearish']
        df['sentiment_label'] = np.select(conditions, choices, default='Neutral/Moderate')
        return df

    def calculate_sentiment(self, vix_df: pd.DataFrame, cot_df: pd.DataFrame) -> pd.DataFrame:
        vix_cot_data = self._prepare_vix_cot_data(cot_df)
        vix_weekly_price = self._prepare_vix_price_data(vix_df)
        vix_cot_indexed = self._calculate_cot_index(vix_cot_data)

        # Merge - Note: vix_weekly_price now includes Contango_Pct
        final_df = vix_cot_indexed.merge(
            vix_weekly_price, left_index=True, right_index=True, how='inner'
        )
        return self._label_sentiment(final_df).dropna(subset=['vix_cot_index'])

    def get_text_sentiment(self, final_df: pd.DataFrame) -> str:
        if final_df.empty: return "Error: No data available."
        latest = final_df.iloc[-1]

        # Custom logic adding Contango context
        contango_msg = "Contango" if latest['Contango_Pct'] > 0 else "Backwardation"

        # Existing logic...
        # (Include your logic from your prompt here, but add the contango context)
        return f"Latest Sentiment: {latest['sentiment_label']} | Market in {contango_msg} ({latest['Contango_Pct']:.2%})"