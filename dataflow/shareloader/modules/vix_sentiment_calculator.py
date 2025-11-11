import pandas as pd
import numpy as np

# --- The constants and initial imports remain the same ---
LOOKBACK_WINDOW_SIZE = 156
from openbb import obb
from pprint import pprint
import pandas as pd
import numpy as np

class VixSentimentCalculator:
    """
    Calculates VIX Sentiment: VIX COT Index and VIX Open Interest (OI) Ratio,
    with proper time-series alignment to COT report dates (Tuesdays).
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
            #'report_week': 'date',
            'noncomm_positions_long_all': 'noncomm_long',
            'noncomm_positions_short_all': 'noncomm_short',
            'open_interest_all': 'open_interest',
            'commodity': 'commodity_name'
        }

        # VIX Futures CFTC Commodity Code
        self.VIX_CODE = '1170E1'

    # --- Data Preparation ---

    def _prepare_vix_cot_data(self, cot_df: pd.DataFrame) -> pd.DataFrame:
        """Filters, renames, and calculates Net Position and VIX OI Ratio."""

        cot_df = cot_df.reset_index()
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

        return vix_cot_df[['noncomm_net', 'open_interest', 'vix_oi_ratio']]

    def _prepare_vix_price_data(self, vix_df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensures VIX price data is indexed by date and resamples it to the
        weekly COT date (Tuesday).
        """
        # --- AMENDMENT START ---
        # Check for the correct lowercase column name
        if 'close' not in vix_df.columns:
            raise ValueError("VIX DataFrame must contain a 'close' column (lowercase).")

        # 1. Ensure Index is Datetime and Sorted
        vix_df['date'] = pd.to_datetime(vix_df['date'])
        vix_df = vix_df.set_index('date').sort_index()
        vix_weekly = vix_df['close'].resample('W-TUE').last().to_frame()
        vix_weekly.columns = ['vix_close']
        vix_weekly
        # --- AMENDMENT END ---
        return vix_weekly

    # --- Calculation ---

    def _calculate_cot_index(self, vix_cot_df: pd.DataFrame) -> pd.DataFrame:
        """Calculates the COT Index for the Non-Commercial Net Position."""

        net_position = vix_cot_df['noncomm_net']

        # Calculate the rolling maximum and minimum of the Net Position
        rolling_max = net_position.rolling(window=self.cot_lookback_period).max()
        rolling_min = net_position.rolling(window=self.cot_lookback_period).min()

        # COT Index Formula
        cot_index = ((net_position - rolling_min) / (rolling_max - rolling_min)) * 100

        vix_cot_df['vix_cot_index'] = cot_index

        return vix_cot_df

    # --- NEW METHOD: Sentiment Labelling ---
    def _label_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds a categorical sentiment label based on the VIX COT Index.
        The VIX COT Index is a contrarian indicator.
        """
        conditions = [
            (df['vix_cot_index'] <= 10),           # Extreme Net Short VIX -> Extreme Bullish Market
            (df['vix_cot_index'] <= 30),           # Net Short VIX -> Bullish Market
            (df['vix_cot_index'] >= 90),           # Extreme Net Long VIX -> Extreme Bearish Market
            (df['vix_cot_index'] >= 70)            # Net Long VIX -> Bearish Market
        ]

        # Note: Sentiments are MARKET sentiments (e.g., Extreme Bullish Market = BUY VIX Signal)
        choices = [
            'Extreme Bullish',
            'Bullish',
            'Extreme Bearish',
            'Bearish'
        ]

        # Use np.select for efficient conditional column creation
        df['sentiment_label'] = np.select(conditions, choices, default='Neutral/Moderate')

        return df

    # --- Main Method ---

    def calculate_sentiment(self, vix_df: pd.DataFrame, cot_df: pd.DataFrame) -> pd.DataFrame:
        """
        Combines VIX and COT data after resampling, calculates indices,
        and returns the merged result with the sentiment label.
        """
        # 1. Prepare and calculate Net Position and OI Ratio from COT data
        vix_cot_data = self._prepare_vix_cot_data(cot_df)

        # 2. Prepare and resample VIX price data (uses lowercase 'close')
        vix_weekly_price = self._prepare_vix_price_data(vix_df)

        # 3. Calculate the VIX COT Index
        vix_cot_indexed = self._calculate_cot_index(vix_cot_data)

        # 4. Merge DataFrames on the date index (COT report Tuesday)
        final_df = vix_cot_indexed.merge(
            vix_weekly_price,
            left_index=True,
            right_index=True,
            how='inner'
        )

        # 5. --- NEW STEP: Add the Sentiment Label ---
        final_df = self._label_sentiment(final_df)

        return final_df.dropna(subset=['vix_cot_index'])

    # The get_text_sentiment method remains unchanged
    def get_text_sentiment(self, final_df: pd.DataFrame) -> str:
        """
        Generates a text sentiment based on the latest VIX COT Index and VIX Price.
        """
        if final_df.empty:
            return "Error: No data available for sentiment calculation."

        latest = final_df.iloc[-1]
        cot_index = latest['vix_cot_index']
        vix_price = latest['vix_close']

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