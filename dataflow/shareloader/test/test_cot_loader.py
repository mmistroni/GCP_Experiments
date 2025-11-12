import unittest
import os
from apache_beam.testing.test_pipeline import TestPipeline


import apache_beam as beam
from openbb import obb
from shareloader.modules.correlation_analyzer import CorrelationAnalyzer, find_smallest_correlation
from shareloader.modules.signal_generator import SignalGenerator
from shareloader.modules.vix_pipelines import find_smallest_correlation,VixSentimentCalculator, AcquireCOTDataFn, \
                                            AcquireVIXDataFn, CalculateSentimentFn, RunCorrelationAnalysisFn,\
                                            FindOptimalCorrelationFn, GenerateSignalFn, ExplodeCotToDailyFn

import pandas as pd
import numpy as np

import requests
import datetime
def get_historical_prices(ticker, start_date, key):

  hist_url = f'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={ticker}&apikey={key}&from=2004-01-01'

  data =  requests.get(hist_url).json()
  return [d for d in data if datetime.datetime.strptime(d['date'], '%Y-%m-%d').date() >=start_date]

def get_latest_cot():
    # async process
    return obb.regulators.cftc.cot(id='1170E1', provider='cftc')


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.debugSink = beam.Map(print)


    def test_cot_sentimente(self):
        # Step 1. getting cot and vix prices
        print('... Gettingn data ....')
        key = os.environ['FMPREPKEY']
        vix_prices = get_historical_prices('^VIX', datetime.date(2004, 7, 20), key)
        cot_df = get_latest_cot()
        # Step 2. calcuclate sentiment
        print('... Calculating Sentiment ....')
        calculator = VixSentimentCalculator(cot_lookback_period=52 * 5, oi_lookback_period=52 * 1)
        res = calculator.calculate_sentiment(pd.DataFrame(vix_prices), cot_df)
        # Step 3. Correlation analysis
        print('... Correlation analysis ....')
        analyzer = CorrelationAnalyzer(res)
        analyzer.run_analysis()
        results_df = analyzer.get_results_table()
        # Step 4:  Find optimal correlation
        print('... Optimal corr ....')
        optimal_lookback, optimal_holding_period, optimal_correlation = find_smallest_correlation(results_df)
        print("--- Optimal Parameter Search Results ---")
        print(f"Based on the analysis, the most predictive signal (smallest correlation) is found at:")
        print(f"Optimal Lookback Period (Row Index): {optimal_lookback} Weeks")
        print(f"Optimal Holding Period (Column Name): {optimal_holding_period} Weeks")
        print(f"Optimal Correlation Value: {optimal_correlation}")
        # Step 5.  Signal Generation
        # 2. Run Signal Generator
        print('... Generatign signaldata ....')
        generator = SignalGenerator(res, optimal_lookback)
        signal = generator.get_current_signal()
        print(signal)

    import pandas as pd
    import numpy as np
    from datetime import timedelta

    # --- ASSUMED INPUT DATA (REPLACE WITH YOUR ACTUAL LOADED DATA) ---
    # cot_df: DataFrame with Date index and columns like 'noncomm_positions_long_all', 'noncomm_positions_short_all'
    # vix_df: DataFrame with Date index and daily 'VIX_Close' price

    import pandas as pd
    import numpy as np
    from datetime import timedelta

    # --- ASSUMED INPUT DATA ---
    # cot_df: DataFrame with Date index and columns like 'noncomm_positions_long_all',
    #         'noncomm_positions_short_all' (used to calculate noncomm_net)
    # vix_df: DataFrame with Date index and daily 'VIX_Close' price

    def prepare_daily_backtest_data(self, cot_df: pd.DataFrame, vix_df: pd.DataFrame) -> pd.DataFrame:
        """
        Aligns weekly COT data (which is forward-filled) to daily VIX prices,
        preparing the final daily-indexed DataFrame.
        Assumes VIX price column is named 'close'.
        """

        # 1. Calculate the required 'noncomm_net' in the COT data
        cot_df = cot_df.copy()
        if 'noncomm_net' not in cot_df.columns:
            # Assuming the necessary underlying columns exist
            cot_df['noncomm_net'] = (
                    cot_df['noncomm_positions_long_all'] - cot_df['noncomm_positions_short_all']
            )

        cot_cols_to_keep = ['noncomm_net']
        cot_daily = cot_df[cot_cols_to_keep]

        # 2. Forward-Fill COT data to every trading day
        # Create a daily index covering the entire VIX range
        daily_index = vix_df.index

        # Reindex the COT data to the full daily index and forward-fill (ffill)
        cot_daily = cot_daily.reindex(daily_index).ffill()

        # 3. Final Merge (Align VIX and COT)
        # The VIX price column ('close') is included here
        final_merged_df = vix_df.merge(cot_daily, left_index=True, right_index=True, how='inner')

        if 'noncomm_net' not in final_merged_df.columns:
            raise ValueError("Failed to merge 'noncomm_net'. Check column names in COT input.")

        # 4. Drop leading NaNs from the start of the COT data
        return final_merged_df.dropna(subset=cot_cols_to_keep)
    # --- EXAMPLE USAGE ---
    # daily_aligned_df = prepare_daily_backtest_data(weekly_cot_df, daily_vix_df)
    # generator = SignalGenerator(df=daily_aligned_df, ...)

    def run_backtest_simulation(self, df: pd.DataFrame, price_column: str = 'close', initial_capital: float = 100000.0):
        """
        Simulates the long-only trading strategy based on BUY/SELL signals and a fixed hold period.
        Defaults to using the 'close' price column.
        """
        df.index = pd.to_datetime(df.index)
        df = df.sort_index().copy()

        df['Position'] = np.nan
        df['Entry_Price'] = np.nan
        df['Exit_Price'] = np.nan
        df['Exit_Date_Target'] = pd.NaT
        df['P_L'] = 0.0
        df['Capital'] = initial_capital

        in_position = False

        for i in range(len(df)):
            current_date = df.index[i]

            if i > 0:
                df.loc[current_date, 'Capital'] = df.iloc[i - 1]['Capital']
                if in_position:
                    # Carry forward position details
                    df.loc[current_date, 'Position'] = df.iloc[i - 1]['Position']
                    df.loc[current_date, 'Entry_Price'] = df.iloc[i - 1]['Entry_Price']
                    df.loc[current_date, 'Exit_Date_Target'] = df.iloc[i - 1]['Exit_Date_Target']

            # 1. Exit Logic
            if in_position and i > 0 and current_date >= df.iloc[i - 1]['Exit_Date_Target']:
                exit_price = df.loc[current_date, price_column]
                entry_price = df.iloc[i - 1]['Entry_Price']
                p_l = (exit_price - entry_price)

                df.loc[current_date, 'Capital'] += p_l
                df.loc[current_date, 'P_L'] = p_l
                df.loc[current_date, 'Exit_Price'] = exit_price
                df.loc[current_date, 'Position'] = 0
                in_position = False

            # 2. Entry Logic (Long-Only, Max 1 position)
            elif not in_position and df.loc[current_date, 'Trade_Signal'] == 'BUY':
                entry_price = df.loc[current_date, price_column]
                hold_days = df.loc[current_date, 'Hold_Period_Days']
                exit_date_target = current_date + timedelta(days=hold_days)

                df.loc[current_date, 'Entry_Price'] = entry_price
                df.loc[current_date, 'Exit_Date_Target'] = exit_date_target
                df.loc[current_date, 'Position'] = 1
                in_position = True

        return df

    def test_cot_simulation(self):

        # prepare data
        print('... Gettingn data ....')
        key = os.environ['FMPREPKEY']
        vix_prices = get_historical_prices('^VIX', datetime.date(2004, 7, 20), key)
        cot_df = get_latest_cot()
        # Step 2. calcuclate sentiment
        print('... Calculating Sentiment ....')
        calculator = VixSentimentCalculator(cot_lookback_period=52 * 5, oi_lookback_period=52 * 1)
        res = calculator.calculate_sentiment(pd.DataFrame(vix_prices), cot_df)
        res['close'] = res['vix_close']
        # Step 3. Correlation analysis
        print('... Correlation analysis ....')
        analyzer = CorrelationAnalyzer(res)
        analyzer.run_analysis()
        results_df = analyzer.get_results_table()
        # Step 4:  Find optimal correlation
        print('... Optimal corr ....')
        optimal_lookback, optimal_holding_period, optimal_correlation = find_smallest_correlation(results_df)
        print("--- Optimal Parameter Search Results ---")
        print(f"Based on the analysis, the most predictive signal (smallest correlation) is found at:")
        print(f"Optimal Lookback Period (Row Index): {optimal_lookback} Weeks")
        print(f"Optimal Holding Period (Column Name): {optimal_holding_period} Weeks")
        print(f"Optimal Correlation Value: {optimal_correlation}")
        # Step 5.  Signal Generation
        # 2. Run Signal Generator
        print('... Generatign signaldata ....')
        generator = SignalGenerator(res, optimal_lookback)
        mock_df = generator.get_backtest_data()
        initial_capital = 100000.0

        # NOTE: Using the default price_column='close'
        results_df = self.run_backtest_simulation(mock_df, initial_capital=initial_capital)

        # --- Assertions ---

        # Verify Entry (Day 1: 2025-01-01)
        entry_row = results_df.loc['2025-01-01']
        self.assertEqual(entry_row['Position'], 1, "Position must be 1 (Long) on entry day.")
        self.assertEqual(entry_row['Entry_Price'], 10.00, "Entry price must be 10.00.")
        self.assertTrue(entry_row['Exit_Date_Target'] == datetime(2025, 1, 5), "Exit target date must be 2025-01-05.")

        # Verify Exit (Day 5: 2025-01-05)
        exit_row = results_df.loc['2025-01-05']
        expected_pnl = 12.00 - 10.00

        self.assertEqual(exit_row['Position'], 0, "Position must be 0 (Closed) on exit day.")
        self.assertAlmostEqual(exit_row['P_L'], expected_pnl, 4, "P&L must be +2.00.")
        self.assertAlmostEqual(exit_row['Capital'], initial_capital + expected_pnl, 4,
                               "Capital must reflect the realized P&L.")



    def test_cot_pipeline(self):
        key = os.environ['FMPREPKEY']
        sink = beam.Map(print)
        KEY_BY_DATE = lambda record_list: [(record['date'], record) for record in record_list]

        with TestPipeline() as p:
            # Note: Best practice for production is to use --environment=sdk to pass keys securely
            fmp_key = os.environ.get('FMPREPKEY', 'DUMMY_KEY')

            # The starting PCollection is a single element used to trigger the fetches
            start_pcoll = p | 'Create Trigger' >> beam.Create(['Test'])

            # --- PARALLEL ACQUISITION STAGE (Stage 1) ---

            # 1.1 Acquire VIX Data (Keyed by 'A')
            vix_records = (
                    start_pcoll
                    | 'Acquire VIX Data' >> beam.ParDo(AcquireVIXDataFn(fmp_key=fmp_key))
            )

            # 2. Key VIX Records
            keyed_vix = (
                    vix_records
                    | 'Key VIX by Date' >> beam.Map(lambda record: (record['date'], record))
            )


            # 1.2 Acquire COT Data
            # FIX: Add a step to convert the list of records into (date, record) pairs.
            keyed_cot = (
                    start_pcoll
                    | 'Acquire COT Data' >> beam.ParDo(AcquireCOTDataFn(credentials={}))
            )

            # 1.2 Acquire COT Data and Explode to Daily Keys
            keyed_cot_daily = (
                    keyed_cot  # This is your PCollection of raw COT records
                    | 'Explode COT to Daily Keys' >> beam.ParDo(ExplodeCotToDailyFn())
            )

            return
            # 1.3 Combine VIX and COT Data (CoGroupByKey on the Daily Date)
            combined_data_list = (
                    {'vix': keyed_vix, 'cot': keyed_cot_daily}  # Use the exploded COT collection
                    | 'CoGroup VIX and COT' >> beam.CoGroupByKey()
            )

            return


            # --- SENTIMENT CALCULATION STAGE (Stage 2) ---
            # Output is a single DataFrame
            sentiment_data = (
                    combined_data_list
                    | 'Calculate Sentiment Metrics' >> beam.ParDo(
                CalculateSentimentFn(cot_lookback=52 * 5, oi_lookback=52 * 1))
            )

            # --- CORRELATION ANALYSIS STAGE (Stage 3) ---
            correlation_results = (
                    sentiment_data
                    | 'Run Correlation Analysis' >> beam.ParDo(RunCorrelationAnalysisFn())
            )

            # --- OPTIMAL PARAMETER FINDING STAGE (Stage 4) ---
            # Output is a single JSON string, keyed by 'B' for the final join
            optimal_params_pcollection = (
                    correlation_results
                    | 'Find Optimal Parameters' >> beam.ParDo(FindOptimalCorrelationFn())
            )

            # --- SIGNAL GENERATION STAGE (Stage 5) ---

            # 5a. Key the Sentiment Data for the final join (Keyed by 'B')
            keyed_sentiment_for_join = sentiment_data | 'Key Sentiment for Join' >> beam.Map(lambda x: ('B', x))

            # 5b. Join the Sentiment Data (Stage 2) and Optimal Parameters (Stage 4)
            joined_data = (
                    {'sentiment': keyed_sentiment_for_join, 'optimal': optimal_params_pcollection}
                    | 'Join Data for Signal Generation' >> beam.CoGroupByKey()
            )

            # 5c. Generate the Signal
            final_signal = (
                    joined_data
                    | 'Generate Final Signal' >> beam.ParDo(GenerateSignalFn())
                    | 'Print Final Signal' >> beam.Map(lambda x: print(f"--- Final Signal: {x} ---"))
            )
            
            final_signal | 'to sink' >> sink
            ###

if __name__ == '__main__':
    unittest.main()
