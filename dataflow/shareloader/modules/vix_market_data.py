import vix_utils
import pandas as pd
import asyncio


async def get_vix_strategy_data():
    vix_futures_history = await vix_utils.async_load_vix_term_structure()
    vix_cash_history = await vix_utils.async_get_vix_index_histories()

    # 1. Process Cash Data (Spot VIX)
    cash_df = vix_cash_history.reset_index()
    spot_vix = cash_df[cash_df['Symbol'] == 'VIX'].copy()
    spot_vix = spot_vix.set_index('Trade Date')['Close'].rename("Spot_VIX")

    # 2. Process Futures Data
    futures_df = vix_futures_history.reset_index()

    # --- CRITICAL FIX: Remove Weekly contracts and duplicates ---
    # We only want standard Monthly futures for a clean term structure
    standard_futures = futures_df[futures_df['Weekly'] == False].copy()

    # Even within Monthly, sometimes there are overlaps; we take the first entry per date/tenor
    standard_futures = standard_futures.drop_duplicates(subset=['Trade Date', 'Tenor_Monthly'])

    # 3. Extract Tenors
    vx1 = standard_futures[standard_futures['Tenor_Monthly'] == 1].set_index('Trade Date')['Close'].rename("VX1")
    vx2 = standard_futures[standard_futures['Tenor_Monthly'] == 2].set_index('Trade Date')['Close'].rename("VX2")

    # 4. Merge
    # Using 'inner' join ensures dates match perfectly across Spot, VX1, and VX2
    df = pd.concat([spot_vix, vx1, vx2], axis=1).dropna()

    # 5. Features
    df['Contango_Pct'] = (df['VX1'] / df['Spot_VIX']) - 1

    return df


def get_vix_market_data():
    return asyncio.run(get_vix_strategy_data())

