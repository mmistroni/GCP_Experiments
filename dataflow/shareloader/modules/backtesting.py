from typing import Tuple, Union
import pandas as pd
import numpy as np
from pydantic import BaseModel, Field

from typing import Tuple, Union


class StrategyEngine:
    def __init__(self, params):
        self.params = params

    def get_action(self, current_row: pd.Series, current_capital: float, in_position: bool, entry_price: float) -> \
    Tuple[str, float, Union[str, None]]:
        params = self.params

        # --- ENTRY LOGIC (LONG VIX PANIC) ---
        # We now look for the 'BUY' signal from your SignalGenerator
        if not in_position and current_row.get('Trade_Signal') == 'BUY':
            # Risk-Based Position Sizing
            max_dollar_risk = current_capital * params.max_risk_pct
            # Stop loss distance (VIX is volatile, so we use the params.trailing_stop_pct)
            stop_loss_dist = current_row[params.price_column] * params.trailing_stop_pct
            units_to_buy = max_dollar_risk / (stop_loss_dist + 1e-9)

            return 'BUY', units_to_buy, None

        # --- EXIT LOGIC (LONG POSITION) ---
        elif in_position:
            current_price = current_row[params.price_column]

            # 1. Trailing Stop-Loss (Price falls below TSL trigger)
            tsl_trigger = entry_price * (1.0 - params.trailing_stop_pct)
            if current_price <= tsl_trigger:
                return 'EXIT', 0.0, 'TSL'

            # 2. Take Profit (Price rises above TP trigger)
            tp_trigger = entry_price * (1.0 + params.take_profit_pct)
            if current_price >= tp_trigger:
                return 'EXIT', 0.0, 'TP'

            # 3. Time-Based Exit (Check if we reached the end of the holding period)
            # This requires 'Exit_Date' to be calculated in the backtest loop
            if 'Exit_Date_Target' in current_row and current_row.name >= current_row['Exit_Date_Target']:
                return 'EXIT', 0.0, 'Time'

        return 'NEUTRAL', 0.0, None
from typing import Dict, Any


class MetricsCalculator:
    def __init__(self, results_df: pd.DataFrame):
        self.df = results_df

    def _calculate_max_drawdown(self) -> Dict[str, Union[float, str]]:
        """Calculates Max Drawdown (MDD) based on daily Capital."""
        capital = self.df['Capital']
        if capital.isnull().all():
            return {"Max Drawdown (%)": 0.0, "Max Drawdown Date": self.df.index[-1].strftime('%Y-%m-%d')}

        capital_peak = capital.cummax()
        drawdown = (capital - capital_peak) / capital_peak
        max_drawdown_pct = drawdown.min()
        mdd_date = drawdown.idxmin()

        return {
            "Max Drawdown (%)": max_drawdown_pct * 100,
            "Max Drawdown Date": mdd_date.strftime('%Y-%m-%d')
        }

    def calculate_metrics(self) -> Dict[str, Any]:
        """Calculates all backtest metrics."""

        # Filter for closed trades
        closed_trades = self.df[self.df['Exit_Reason'].notna() & (self.df['Realized_P_L'] != 0.0)]

        total_closed_trades = len(closed_trades)
        wins = closed_trades[closed_trades['Realized_P_L'] > 0]
        losses = closed_trades[closed_trades['Realized_P_L'] <= 0]

        # Avoid division by zero
        win_rate = (len(wins) / total_closed_trades) * 100 if total_closed_trades > 0 else 0.0
        avg_win = wins['Realized_P_L'].mean()
        avg_loss = losses['Realized_P_L'].mean()

        # Calculate R/R Ratio
        risk_reward_ratio = abs(avg_win / avg_loss) if avg_loss else np.nan

        # Compile final metrics
        final_metrics = {
            'Initial Capital': self.df['Capital'].iloc[0],
            'Final Capital': self.df['Capital'].iloc[-1],
            'Total P&L': self.df['Realized_P_L'].sum(),
            **self._calculate_max_drawdown(),  # MDD metrics
            'Total Closed Trades': total_closed_trades,
            'Win Rate (%)': win_rate,
            'Average Win ($)': avg_win,
            'Average Loss ($)': avg_loss,
            'Risk/Reward Ratio (AW/AL)': risk_reward_ratio,
        }
        return final_metrics


class Backtester:
    def __init__(self, params, strategy_engine: StrategyEngine):
        self.params = params
        self.strategy = strategy_engine


def run_simulation(self, df: pd.DataFrame) -> pd.DataFrame:
    """Runs the Long VIX backtest based on Trade_Signal entries."""
    params = self.params
    df = df.sort_index().copy()

    # --- Initialize Result Columns ---
    df['Capital'] = np.nan
    df['Realized_P_L'] = 0.0
    df['Position_Size'] = 0.0
    df['Entry_Price'] = np.nan
    df['Exit_Reason'] = None

    # State variables
    in_position = False
    units_held = 0.0
    entry_price = 0.0
    running_capital = params.initial_capital
    exit_target_date = None

    # Set starting capital
    if not df.empty:
        df.loc[df.index[0], 'Capital'] = running_capital

    for i in range(len(df)):
        current_date = df.index[i]
        current_row = df.loc[current_date]
        current_price = current_row[params.price_column]

        # --- 1. Call Strategy Engine for Action ---
        # Note: We pass exit_target_date to handle time-based exits
        action, units_to_trade, exit_reason = self.strategy.get_action(
            current_row, running_capital, in_position, entry_price
        )

        # --- 2. Execution Logic ---

        # A. EXIT LOGIC (Long Position)
        if action == 'EXIT' and in_position:
            exit_price = current_price

            # P&L Calculation: (Exit - Entry) * Units
            gross_p_l = (exit_price - entry_price) * units_held
            total_commission = units_held * params.commission_per_unit
            net_p_l = gross_p_l - total_commission

            running_capital += net_p_l
            df.loc[current_date, 'Capital'] = running_capital
            df.loc[current_date, 'Realized_P_L'] = net_p_l
            df.loc[current_date, 'Exit_Reason'] = exit_reason

            # Reset State
            in_position, units_held, entry_price = False, 0.0, 0.0
            exit_target_date = None

        # B. ENTRY LOGIC (Long Position)
        elif action == 'BUY' and not in_position:
            units_held = units_to_trade
            entry_price = current_price
            in_position = True

            # Store Entry Info
            df.loc[current_date, 'Position_Size'] = units_held
            df.loc[current_date, 'Entry_Price'] = entry_price

            # Calculate Time-Based Exit Target if Hold_Period_Days exists
            if 'Hold_Period_Days' in current_row:
                days_to_hold = int(current_row['Hold_Period_Days'])
                # Find the date index 'days_to_hold' steps ahead
                future_idx = i + days_to_hold
                if future_idx < len(df):
                    exit_target_date = df.index[future_idx]

        # C. CARRY CAPITAL FORWARD
        if pd.isna(df.loc[current_date, 'Capital']):
            df.loc[current_date, 'Capital'] = running_capital

    # --- 3. Final Liquidation (End of Data) ---
    if in_position:
        final_date = df.index[-1]
        final_price = df.loc[final_date, params.price_column]
        net_p_l = ((final_price - entry_price) * units_held) - (units_held * params.commission_per_unit)
        df.loc[final_date, 'Capital'] += net_p_l
        df.loc[final_date, 'Realized_P_L'] += net_p_l
        df.loc[final_date, 'Exit_Reason'] = 'Final_Liquidation'

    return df
from pydantic import BaseModel, Field


class BacktestParameters(BaseModel):
    """
    Validated parameters for the Long VIX Panic Strategy.
    """
    # Column Names
    price_column: str = Field('vix_close', description="The price used for P&L (from vix_utils/SentimentCalc)")
    signal_column: str = Field('Trade_Signal', description="The BUY/SELL signal column")

    # Capital & Sizing
    initial_capital: float = Field(20000.0, gt=0)
    max_risk_pct: float = Field(0.02, gt=0, lt=1, description="Risk 2% of capital per trade")

    # Exit Logic
    trailing_stop_pct: float = Field(0.15, gt=0, lt=1, description="TSL distance (e.g., 15%)")
    take_profit_pct: float = Field(0.30, gt=0, lt=1, description="TP target (e.g., 30%)")
    commission_per_unit: float = Field(0.01, ge=0)

    # Time-based exit (if signal generator provides it)
    use_time_exit: bool = Field(True, description="Whether to use the Hold_Period_Days as an exit trigger")