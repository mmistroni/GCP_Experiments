import pandas as pd
import numpy as np
from typing import Dict


class VixSentimentCalculator:
    """
    Calculates VIX Sentiment: VIX COT Index and VIX Open Interest (OI) Ratio,
    with proper time-series alignment to COT report dates (Tuesdays) AND
    includes SPX 10-day momentum for shock filtering.
    """

    # Define the core columns needed for VIX COT analysis
    def __init__(self, cot_lookback_period: int = 52 * 5, oi_lookback_period: int = 52 * 1):
        """
        Initializes the calculator.

        Args:
            cot_lookback_period (int): Weeks for COT Index historical range (e.g., 5 years).
            oi_lookback_period (int): Weeks for VIX OI sentiment ratio calculation (e.g., 1 year).
        """
        self.cot_lookback_period = cot_lookback_period
        self.oi_lookback_period = oi_lookback_period
        self.COT_COLUMNS = {
            # 'report_week': 'date', # Removed as date is handled via set_index
            'noncomm_positions_long_all': 'noncomm_long',
            'noncomm_positions_short_all': 'noncomm_short',
            'open_interest_all': 'open_interest',
            'commodity': 'commodity_name'
        }

        # VIX Futures CFTC Commodity Code
        self.VIX_CODE = '1170E1'

    # --- Data Preparation ---

    def _prepare_vix_cot_data(self, cot_df: pd.DataFrame) -> pd.DataFrame:
        """Filters, renames, and calculates Net Position and VIX OI Ratio (Weekly)."""

        vix_cot_df = cot_df[
            cot_df['cftc_contract_market_code'] == self.VIX_CODE
            ].copy()

        vix_cot_df = vix_cot_df.rename(columns=self.COT_COLUMNS)

        # 1. Standardize COT Index: date is the COT report's Tuesday
        vix_cot_df['date'] = pd.to_datetime(vix_cot_df['date'])
        vix_cot_df = vix_cot_df.set_index('date').sort_index()

        # 2. Calculate Non-Commercial Net Position
        vix_cot_df['noncomm_net'] = (
                vix_cot_df['noncomm_long'] - vix_cot_df['noncomm_short']
        )

        # 3. Calculate VIX OI Ratio (Rolling Mean / Current OI)
        vix_cot_df['vix_oi_ratio'] = (
                vix_cot_df['open_interest'].rolling(window=self.oi_lookback_period).mean() /
                vix_cot_df['open_interest']
        )

        # Keep only the weekly sentiment components
        return vix_cot_df[['noncomm_net', 'open_interest', 'vix_oi_ratio']]

    # --- MODIFIED: Renamed 'close' column to VIX_close and removed resampling ---
    def _prepare_vix_price_data(self, vix_df: pd.DataFrame) -> pd.DataFrame:
        """Ensures VIX price data is indexed by date (Daily)."""
        if 'close' not in vix_df.columns:
            raise ValueError("VIX DataFrame must contain a 'close' column (lowercase).")

        #vix_df['date'] = pd.to_datetime(vix_df['date'])
        vix_df = vix_df.sort_index()
        vix_df = vix_df.rename(columns={'close': 'VIX_close'})

        # Keep only the daily VIX price
        return vix_df[['VIX_close']]

    # --- NEW METHOD: Prepares SPX daily data and calculates 10D momentum ---
    def _prepare_spx_data(self, spx_df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensures SPX price data is indexed by date and calculates the
        10-day price change (Daily).
        """
        if 'close' not in spx_df.columns:
            raise ValueError("SPX DataFrame must contain a 'close' column (lowercase).")

        spx_df['date'] = pd.to_datetime(spx_df['date'])
        spx_df = spx_df.set_index('date').sort_index()
        spx_df = spx_df.rename(columns={'close': 'SPX_close'})

        # Calculate the 10-day Rate of Change for the SPX Close (the 'shock' filter)
        spx_df['SPX_10D_Change'] = spx_df['SPX_close'].pct_change(10)

        # Keep the daily SPX price and the momentum filter
        return spx_df[['SPX_close', 'SPX_10D_Change']]

    # --- Calculation (Unchanged) ---
    def _calculate_cot_index(self, vix_cot_df: pd.DataFrame) -> pd.DataFrame:
        """Calculates the COT Index for the Non-Commercial Net Position."""
        # ... (Implementation is exactly the same as provided by user) ...
        net_position = vix_cot_df['noncomm_net']
        rolling_max = net_position.rolling(window=self.cot_lookback_period).max()
        rolling_min = net_position.rolling(window=self.cot_lookback_period).min()
        cot_index = ((net_position - rolling_min) / (rolling_max - rolling_min)) * 100
        vix_cot_df['vix_cot_index'] = cot_index
        return vix_cot_df

    # --- Sentiment Labelling (Unchanged) ---
    def _label_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds a categorical sentiment label based on the VIX COT Index."""
        # ... (Implementation is exactly the same as provided by user) ...
        conditions = [
            (df['vix_cot_index'] <= 10),
            (df['vix_cot_index'] <= 30),
            (df['vix_cot_index'] >= 90),
            (df['vix_cot_index'] >= 70)
        ]
        choices = [
            'Extreme Bullish',
            'Bullish',
            'Extreme Bearish',
            'Bearish'
        ]
        df['sentiment_label'] = np.select(conditions, choices, default='Neutral/Moderate')
        return df

    # --- MODIFIED MAIN METHOD ---
    def calculate_sentiment(self, vix_df: pd.DataFrame, cot_df: pd.DataFrame, spx_df: pd.DataFrame) -> pd.DataFrame:
        """
        Combines VIX (Daily), SPX (Daily), and COT (Weekly) data,
        calculates indices, and forward-fills the COT signal for daily use.
        """

        # 1. Prepare and process daily VIX and SPX data
        vix_daily_price = self._prepare_vix_price_data(vix_df)
        spx_daily_data = self._prepare_spx_data(spx_df)

        # Merge VIX and SPX (DAILY DataFrame)
        daily_price_df = vix_daily_price.join(spx_daily_data, how='inner')

        # 2. Prepare and calculate Net Position and OI Ratio from COT data (WEEKLY DataFrame)
        vix_cot_data = self._prepare_vix_cot_data(cot_df)

        # 3. Calculate the VIX COT Index (WEEKLY DataFrame)
        vix_cot_indexed = self._calculate_cot_index(vix_cot_data)

        # 4. Add the Sentiment Label (WEEKLY DataFrame)
        vix_cot_labeled = self._label_sentiment(vix_cot_indexed)

        # 5. Merge Daily Price Data with Weekly COT Data
        # We perform a left join to keep ALL daily price rows, and fill missing COT values
        # The weekly COT data columns are: 'noncomm_net', 'open_interest', 'vix_oi_ratio', 'vix_cot_index', 'sentiment_label'
        final_df = daily_price_df.join(
            vix_cot_labeled[[
                'vix_cot_index',
                'sentiment_label'
            ]],
            how='left'
        )

        # 6. Forward-Fill (ffill) the weekly COT signal to align with daily price data
        final_df['vix_cot_index'] = final_df['vix_cot_index'].ffill()
        final_df['sentiment_label'] = final_df['sentiment_label'].ffill()

        # Drop rows where the COT index is still NaN (initial lookback period)
        return final_df.dropna(subset=['vix_cot_index'])

    # The get_text_sentiment method remains unchanged
    def get_text_sentiment(self, final_df: pd.DataFrame) -> str:
        """
        Generates a text sentiment based on the latest VIX COT Index and VIX Price.
        (Uses the VIX_close column from the final daily frame)
        """
        if final_df.empty:
            return "Error: No data available for sentiment calculation."

        latest = final_df.iloc[-1]
        cot_index = latest['vix_cot_index']
        vix_price = latest['VIX_close']  # Renamed column

        sentiment = ""

        # --- 1. Base Sentiment from VIX COT Index (Primary Signal) ---
        if cot_index <= 10:
            sentiment = "Extremely Bullish (VIX COT at historical low, speculators are aggressively short VIX)."
        elif cot_index <= 30:
            sentiment = "Bullish (VIX COT well below neutral, large speculators are short VIX)."
        elif cot_index >= 90:
            sentiment = "Extremely Bearish (VIX COT at historical high, speculators are heavily long VIX/hedging)."
        elif cot_index >= 70:
            sentiment = "Bearish (VIX COT well above neutral, large speculators are long VIX)."
        else:
            sentiment = "Neutral/Moderate (VIX COT positioning is balanced)."

        # --- 2. VIX Price Context (Confirmation) ---
        if vix_price > 25:
            sentiment += f" VIX Price ({vix_price:.1f}) is high, suggesting actual market stress."
        elif vix_price < 15 and "Bullish" in sentiment:
            sentiment += f" VIX Price ({vix_price:.1f}) is low, confirming complacency."
        elif vix_price < 15 and "Bearish" in sentiment:
            sentiment += f" VIX Price ({vix_price:.1f}) is low, suggesting a potential short-term reversal or false COT signal."

        return f"Latest VIX Sentiment ({latest.name.strftime('%Y-%m-%d')}): {sentiment}"