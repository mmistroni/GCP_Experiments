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
        """
        Determines the trading action (SELL/EXIT, HOLD) for a SHORT VIX strategy.
        """
        params = self.params
        action = 'NEUTRAL'
        units_to_trade = 0.0
        exit_reason = None

        # --- Entry Logic (SHORT VIX Mean Reversion) ---
        # The threshold (e.g., 2.0) is assumed to be passed via params
        if not in_position and current_row['VIX_Ratio'] >= params.vix_ratio_threshold:

            # Risk-Based Position Sizing: Risk is defined by TSL distance (loss if price rises)
            max_dollar_risk = current_capital * params.max_risk_pct

            # For SHORT: Stop loss is above the entry price (TSL % increase)
            stop_loss_distance_per_unit = current_row[params.price_column] * params.trailing_stop_pct
            units_held = max_dollar_risk / (stop_loss_distance_per_unit + 1e-9)

            return 'SELL', units_held, None  # <-- SELL action initiates the SHORT position

        # --- Exit Logic (Priority Check when in a position) ---
        elif in_position:
            # Note: For Short, peak tracking is irrelevant unless using Trailing TP/TSL logic.
            # We will use simple fixed TSL/TP relative to entry.
            current_price = current_row[params.price_column]

            # 1. Trailing Stop-Loss (Loss if price rises against the short trade)
            tsl_trigger = entry_price * (1.0 + params.trailing_stop_pct)
            if current_price >= tsl_trigger:
                return 'EXIT', 0.0, 'TSL'  # Price rises to TSL

            # 2. Take Profit (Profit if price falls by TP amount)
            tp_trigger_price = entry_price * (1.0 - params.take_profit_pct)
            if current_price <= tp_trigger_price:
                return 'EXIT', 0.0, 'TP'  # Price falls to TP

            # 3. Time-Based Exit (Assuming a long hold period for now)
            if 'Exit_Date_Target' in current_row and current_row.name >= current_row['Exit_Date_Target']:
                return 'EXIT', 0.0, 'Time'

        return action, units_to_trade, exit_reason

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
        """Runs the short VIX mean reversion backtest simulation."""

        params = self.params
        df = df.sort_index().copy()

        # ... (Initialization and state variables remain the same) ...
        # Assume df column initialization is handled here.

        in_position, units_held, entry_price, peak_price = False, 0.0, 0.0, 0.0
        running_capital = params.initial_capital

        # Set initial capital
        if not df.empty:
            df.loc[df.index[0], 'Capital'] = running_capital

        for i in range(len(df)):
            current_date = df.index[i]
            current_row = df.loc[current_date]
            current_price = current_row[params.price_column]

            # ... (Position tracking updates remain the same) ...

            # --- Call Strategy Engine ---
            action, units_to_trade, exit_reason = self.strategy.get_action(
                current_row, running_capital, in_position, entry_price
            )

            # --- Execute Actions (Entry/Exit) ---
            if action == 'EXIT':
                exit_price = current_price

                # FIX: P&L Calculation for a SHORT trade: (Entry Price - Exit Price) * Units
                gross_p_l = (entry_price - exit_price) * units_held  # <--- SHORT P&L
                total_commission = units_held * params.commission_per_unit
                net_p_l = gross_p_l - total_commission

                running_capital += net_p_l
                df.loc[current_date, 'Capital'] = running_capital
                df.loc[current_date, 'Realized_P_L'] = net_p_l
                df.loc[current_date, 'Exit_Reason'] = exit_reason

                # Reset state
                in_position, units_held, entry_price, peak_price = False, 0.0, 0.0, 0.0

            # Entry Check: 'SELL' for SHORT trade
            elif action == 'SELL' and not in_position:
                units_held = units_to_trade
                entry_price = current_price

                df.loc[current_date, 'Position_Size'] = units_held
                df.loc[current_date, 'Entry_Price'] = entry_price

                in_position = True
                peak_price = entry_price  # Start peak tracking

            # Carry Capital Forward
            if pd.isna(df.loc[current_date, 'Capital']):
                df.loc[current_date, 'Capital'] = running_capital

        # --- FINAL LIQUIDATION LOGIC ---
        if in_position:
            final_date = df.index[-1]
            final_price = df.loc[final_date, params.price_column]

            # SHORT P&L Calculation for final liquidation
            gross_p_l = (entry_price - final_price) * units_held
            total_commission = units_held * params.commission_per_unit
            net_p_l = gross_p_l - total_commission

            df.loc[final_date, 'Capital'] += net_p_l
            df.loc[final_date, 'Realized_P_L'] += net_p_l

        return df

class BacktestParameters(BaseModel):
    """
    Defines and validates the key parameters for the backtest.
    """
    price_column: str = Field('VIX_close', description="Column name for the traded instrument's price.")
    spx_column: str = Field('SPX_close', description="Column name for the S&P 500 price data.")
    initial_capital: float = Field(20000.0, gt=0, description="Starting cash.")

    # Risk Management Parameters
    trailing_stop_pct: float = Field(0.10, gt=0, lt=1, description="Percentage for TSL.")
    take_profit_pct: float = Field(0.20, gt=0, lt=1, description="Percentage for TP.")
    max_risk_pct: float = Field(0.015, gt=0, lt=1, description="Max % of capital to risk per trade (the fix).")
    commission_per_unit: float = Field(0.01, ge=0, description="Commission per unit traded.")
    vix_ratio_threshold:float = Field(2.0, ge=0, description="Commission per unit traded.")

    # Strategy-Specific Entry Filter Parameter
    cot_entry_threshold: float = Field(10.0, ge=0, description="VIX COT Index threshold for entry confirmation.")
