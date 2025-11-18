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
from datetime import timedelta
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

    def run_backtest_simulation(self, df: pd.DataFrame, price_column: str = 'close',
                                             initial_capital: float = 10000.0,
                                             trailing_stop_pct: float = 0.10, take_profit_pct: float = 0.20,
                                             position_sizing_ratio: float = 1.0, commission_per_unit: float = 0.01):
        """
        Simulates the long-only trading strategy incorporating Trailing Stop-Loss and
        a PERCENTAGE-BASED Take Profit (take_profit_pct) that scales with entry price.

        :param take_profit_pct: Percentage gain from entry price to trigger exit (e.g., 0.30 for 30%).
        """
        from datetime import timedelta
        import numpy as np

        df.index = pd.to_datetime(df.index)
        df = df.sort_index().copy()

        # Initialize new tracking columns
        df['Position_Size'] = 0.0
        df['Realized_P_L'] = 0.0
        df['Capital'] = initial_capital
        df['Peak_Price_in_Position'] = np.nan
        df['Exit_Reason'] = None

        # Position State Variables
        in_position = False
        units_held = 0.0
        entry_price = 0.0
        exit_date_target = pd.NaT
        peak_price = 0.0

        if not df.empty:
            df.loc[df.index[0], 'Capital'] = initial_capital

        # Pre-populate capital
        for i in range(1, len(df)):
            df.loc[df.index[i], 'Capital'] = df.loc[df.index[i - 1], 'Capital']

        for i in range(len(df)):
            current_date = df.index[i]
            current_price = df.loc[current_date, price_column]

            if i > 0:
                df.loc[current_date, 'Position_Size'] = units_held
                df.loc[current_date, 'Peak_Price_in_Position'] = peak_price

            # --- Exit Check & Logic ---
            exit_reason = None
            if in_position:
                # 1. Update Peak Price for TSL
                peak_price = max(peak_price, current_price)
                df.loc[current_date, 'Peak_Price_in_Position'] = peak_price

                # Determine exit triggers
                tsl_trigger = peak_price * (1.0 - trailing_stop_pct)
                tp_trigger_price = entry_price * (1.0 + take_profit_pct)  # NEW TP CALCULATION

                # 1. Check Trailing Stop-Loss (Highest Priority - Safety Net)
                if current_price <= tsl_trigger:
                    exit_reason = 'TSL'

                # 2. Check Take Profit Target (Percentage Gain - Capture Spikes)
                elif current_price >= tp_trigger_price:
                    exit_reason = 'TP'

                # 3. Check Time-Based Exit (Soft Target with Loss-Aversion Overlay)
                elif current_date >= exit_date_target:
                    if current_price > entry_price:
                        exit_reason = 'Time'

                # Execute Exit if a reason is found
                if exit_reason:
                    exit_price = current_price

                    # Calculate P&L
                    gross_p_l = (exit_price - entry_price) * units_held
                    total_commission = units_held * commission_per_unit
                    net_p_l = gross_p_l - total_commission

                    # Update Capital and Tracking
                    df.loc[current_date, 'Capital'] += net_p_l
                    df.loc[current_date, 'Realized_P_L'] = net_p_l
                    df.loc[current_date, 'Exit_Reason'] = exit_reason

                    # Reset position state
                    in_position = False
                    units_held = 0.0
                    entry_price = 0.0
                    peak_price = 0.0

            # --- Entry Logic (Long-Only) ---
            if not in_position and df.loc[current_date, 'Trade_Signal'] == 'BUY':
                entry_price = current_price
                capital_to_risk = df.loc[current_date, 'Capital'] * position_sizing_ratio
                units_held = capital_to_risk / (entry_price + 1e-9)

                df.loc[current_date, 'Position_Size'] = units_held
                df.loc[current_date, 'Entry_Price'] = entry_price

                hold_days = df.loc[current_date, 'Hold_Period_Days']
                exit_date_target = current_date + timedelta(days=int(hold_days))

                in_position = True
                peak_price = entry_price

                # --- FINAL LIQUIDATION LOGIC ---
        if in_position:
            final_date = df.index[-1]
            final_price = df.loc[final_date, price_column]

            gross_p_l = (final_price - entry_price) * units_held
            total_commission = units_held * commission_per_unit
            net_p_l = gross_p_l - total_commission

            df.loc[final_date, 'Capital'] += net_p_l
            df.loc[final_date, 'Realized_P_L'] += net_p_l

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

        initial_capital = 20000.0

        # NOTE: Using the default price_column='close'
        results_df = self.run_backtest_simulation(mock_df, initial_capital=initial_capital)

        results_df.to_csv('c:/Temp/VIXPNL.csv')

        # --- Calculating and Printing Final P&L ---
        # 1. Get the final value from the last row of the 'Capital' column
        final_capital = results_df['Capital'].iloc[-1]

        # 2. Calculate the Profit and Loss
        final_pnl = final_capital - initial_capital

        # 3. Calculate Return Percentage
        return_percentage = (final_pnl / initial_capital) * 100

        print("\n--- Backtest Summary ---")
        print(f"Initial Capital: ${initial_capital:,.2f}")
        print(f"Final Capital:   ${final_capital:,.2f}")
        print("------------------------")
        print(f"Total P&L:       ${final_pnl:,.2f}")
        print(f"Return:          {return_percentage:.2f}%")

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
