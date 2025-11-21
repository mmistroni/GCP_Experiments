from typing import Tuple, Union
import pandas as pd
import numpy as np


class StrategyEngine:
    def __init__(self, params):
        self.params = params

    def get_action(self, current_row: pd.Series, current_capital: float, in_position: bool, entry_price: float) -> \
    Tuple[str, float, Union[str, None]]:
        """
        Determines the trading action (BUY/EXIT, HOLD) for a LONG VIX strategy.
        """
        params = self.params
        action = 'NEUTRAL'
        units_to_trade = 0.0
        exit_reason = None

        # --- Entry Logic (LONG VIX Direction) ---
        if not in_position and current_row['Trade_Signal'] == 'BUY':

            # 1. Risk-Based Position Sizing
            max_dollar_risk = current_capital * params.max_risk_pct
            stop_loss_distance_per_unit = current_row[params.price_column] * params.trailing_stop_pct
            units_held = max_dollar_risk / (stop_loss_distance_per_unit + 1e-9)

            return 'BUY', units_held, None

        # --- Exit Logic (Priority Check when in a position) ---
        elif in_position:
            # Safely retrieve peak price
            peak_price = current_row.get('Peak_Price_in_Position', entry_price)
            if np.isnan(peak_price):
                peak_price = entry_price

            current_price = current_row[params.price_column]

            # 1. Trailing Stop-Loss (TSL)
            tsl_trigger = peak_price * (1.0 - params.trailing_stop_pct)
            if current_price <= tsl_trigger:
                return 'EXIT', 0.0, 'TSL'

            # 2. Take Profit (TP)
            tp_trigger_price = entry_price * (1.0 + params.take_profit_pct)
            if current_price >= tp_trigger_price:
                return 'EXIT', 0.0, 'TP'

            # 3. Time-Based Exit
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
        """Runs the backtest simulation."""

        params = self.params
        df = df.sort_index().copy()

        # Pre-calculate Exit Date Target
        df['Exit_Date_Target'] = df.index + pd.to_timedelta(df['Hold_Period_Days'], unit='D')

        # Initialize tracking columns (omitted for brevity, assume they are initialized)
        df['Position_Size'] = 0.0
        df['Realized_P_L'] = 0.0
        df['Capital'] = np.nan
        df['Peak_Price_in_Position'] = np.nan
        df['Exit_Reason'] = None
        df['Entry_Price'] = np.nan

        # Position State Variables
        in_position, units_held, entry_price, peak_price = False, 0.0, 0.0, 0.0
        running_capital = params.initial_capital

        if not df.empty:
            df.loc[df.index[0], 'Capital'] = running_capital

        for i in range(len(df)):
            current_date = df.index[i]
            current_row = df.loc[current_date]
            current_price = current_row[params.price_column]

            # 1. Update Position Tracking
            if in_position:
                peak_price = max(peak_price, current_price)

            df.loc[current_date, 'Position_Size'] = units_held
            df.loc[current_date, 'Peak_Price_in_Position'] = peak_price

            # --- Call Strategy Engine ---
            action, units_to_trade, exit_reason = self.strategy.get_action(
                current_row, running_capital, in_position, entry_price
            )

            # --- Execute Actions (Entry/Exit) ---
            if action == 'EXIT':
                exit_price = current_price

                # P&L Calculation for a LONG trade
                gross_p_l = (exit_price - entry_price) * units_held
                total_commission = units_held * params.commission_per_unit
                net_p_l = gross_p_l - total_commission

                running_capital += net_p_l
                df.loc[current_date, 'Capital'] = running_capital
                df.loc[current_date, 'Realized_P_L'] = net_p_l
                df.loc[current_date, 'Exit_Reason'] = exit_reason

                # Reset state
                in_position, units_held, entry_price, peak_price = False, 0.0, 0.0, 0.0

            # Entry Check: 'BUY' for LONG trade
            elif action == 'BUY' and not in_position:
                units_held = units_to_trade
                entry_price = current_price

                df.loc[current_date, 'Position_Size'] = units_held
                df.loc[current_date, 'Entry_Price'] = entry_price

                in_position = True
                peak_price = entry_price

            # Carry Capital Forward
            if pd.isna(df.loc[current_date, 'Capital']):
                df.loc[current_date, 'Capital'] = running_capital

        # --- FINAL LIQUIDATION LOGIC ---
        if in_position:
            # ... (Liquidation logic remains the same, calculating final P&L) ...
            pass  # Keep your correct final liquidation logic here

        return df