"""
ReportGenerator for prediction market backtesting.

Produces formatted text summaries, matplotlib equity/drawdown charts,
and CSV exports from backtest results.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import numpy as np
import structlog

if TYPE_CHECKING:
    from ..models.config import BacktestConfig
    from .metrics import EquityPoint, TradeRecord

logger = structlog.get_logger(__name__)


class ReportGenerator:
    """
    Generates reports and visualizations from backtest results.

    Produces text summaries, equity curve plots, drawdown charts,
    and trade CSV exports.
    """

    # ------------------------------------------------------------------
    # Text summary
    # ------------------------------------------------------------------

    def generate_summary(
        self,
        strategy_name: str,
        config: BacktestConfig,
        metrics: dict[str, float],
        equity_curve: list,  # list[EquityPoint]
        trade_log: list,  # list[TradeRecord]
    ) -> str:
        """
        Generate formatted text summary of backtest results.

        Includes:
        - Header with strategy name, time range, platform
        - Capital summary (initial, final, return)
        - Risk metrics (Sharpe, Sortino, max drawdown)
        - Trading metrics (win rate, profit factor, trades count)
        - Top 5 winning and losing trades

        Returns:
            Formatted multi-line string.
        """
        bar = "\u2550" * 54  # ══════...

        start_dt = datetime.fromtimestamp(
            config.start_time_ms / 1000, tz=timezone.utc
        )
        end_dt = datetime.fromtimestamp(
            config.end_time_ms / 1000, tz=timezone.utc
        )
        duration = end_dt - start_dt

        # Compute human-readable duration
        duration_str = self._format_duration_days(duration)

        # Final equity from equity curve (fallback to initial cash)
        if equity_curve:
            final_equity = float(equity_curve[-1].equity)
        else:
            final_equity = config.initial_cash

        total_return_pct = metrics.get("total_return_pct", 0.0)
        sharpe = metrics.get("sharpe_ratio", 0.0)
        sortino = metrics.get("sortino_ratio", 0.0)
        max_dd_pct = metrics.get("max_drawdown_pct", 0.0)
        max_dd_duration_ms = metrics.get("max_drawdown_duration_ms", 0.0)
        num_trades = int(metrics.get("num_trades", 0))
        win_rate = metrics.get("win_rate", 0.0) * 100.0
        profit_factor = metrics.get("profit_factor", 0.0)
        expectancy = metrics.get("expectancy", 0.0)
        avg_trade_pnl = metrics.get("avg_trade_pnl", 0.0)
        total_fees = metrics.get("total_fees", 0.0)

        max_dd_dur_str = self._format_duration_ms(max_dd_duration_ms)

        platform = config.platform or "all"

        lines: list[str] = []
        lines.append(bar)
        lines.append(f"  Backtest Report: {strategy_name}")
        lines.append(bar)
        lines.append("")
        lines.append(
            f"Period:     {start_dt.strftime('%Y-%m-%d %H:%M')} UTC  "
            f"\u2192  {end_dt.strftime('%Y-%m-%d %H:%M')} UTC"
        )
        lines.append(f"Platform:   {platform}")
        lines.append(f"Duration:   {duration_str}")
        lines.append("")

        # -- Capital ---------------------------------------------------
        lines.append(
            f"\u2500\u2500 Capital "
            + "\u2500" * 45
        )
        lines.append(f"Initial:      ${config.initial_cash:,.2f}")
        lines.append(f"Final:        ${final_equity:,.2f}")
        lines.append(f"Return:       {total_return_pct:.2f}%")
        lines.append("")

        # -- Risk Metrics ----------------------------------------------
        lines.append(
            f"\u2500\u2500 Risk Metrics "
            + "\u2500" * 40
        )
        lines.append(f"Sharpe Ratio:     {sharpe:.2f}")
        lines.append(f"Sortino Ratio:    {sortino:.2f}")
        lines.append(f"Max Drawdown:     {max_dd_pct:.2f}%")
        lines.append(f"Max DD Duration:  {max_dd_dur_str}")
        lines.append("")

        # -- Trading Metrics -------------------------------------------
        lines.append(
            f"\u2500\u2500 Trading Metrics "
            + "\u2500" * 37
        )
        lines.append(f"Total Trades:     {num_trades}")
        lines.append(f"Win Rate:         {win_rate:.2f}%")
        lines.append(f"Profit Factor:    {profit_factor:.2f}")
        lines.append(f"Expectancy:       ${expectancy:,.2f}")
        lines.append(f"Avg Trade P&L:    ${avg_trade_pnl:,.2f}")
        lines.append(f"Total Fees:       ${total_fees:,.2f}")

        # -- Top winning / losing trades --------------------------------
        if trade_log:
            sorted_by_pnl = sorted(
                trade_log,
                key=lambda t: float(t.realized_pnl),
                reverse=True,
            )

            top_winners = [t for t in sorted_by_pnl if float(t.realized_pnl) > 0][
                :5
            ]
            top_losers = [t for t in sorted_by_pnl if float(t.realized_pnl) <= 0][
                -5:
            ]
            # Losers should be ordered worst first (most negative at top)
            top_losers = list(reversed(top_losers))

            lines.append("")
            lines.append(
                f"\u2500\u2500 Top Winning Trades "
                + "\u2500" * 34
            )
            if top_winners:
                for i, t in enumerate(top_winners, start=1):
                    pnl = float(t.realized_pnl)
                    entry_p = float(t.entry_price)
                    exit_p = float(t.exit_price) if t.exit_price is not None else 0.0
                    lines.append(
                        f"  {i}. {t.asset_id}  {t.side.upper()}  "
                        f"+${pnl:,.2f}  ({entry_p:.2f} \u2192 {exit_p:.2f})"
                    )
            else:
                lines.append("  (none)")

            lines.append("")
            lines.append(
                f"\u2500\u2500 Top Losing Trades "
                + "\u2500" * 35
            )
            if top_losers:
                for i, t in enumerate(top_losers, start=1):
                    pnl = float(t.realized_pnl)
                    entry_p = float(t.entry_price)
                    exit_p = float(t.exit_price) if t.exit_price is not None else 0.0
                    lines.append(
                        f"  {i}. {t.asset_id}  {t.side.upper()}  "
                        f"-${abs(pnl):,.2f}  ({entry_p:.2f} \u2192 {exit_p:.2f})"
                    )
            else:
                lines.append("  (none)")

        lines.append(bar)

        summary = "\n".join(lines)

        logger.info(
            "report_summary_generated",
            strategy_name=strategy_name,
            num_trades=num_trades,
        )
        return summary

    # ------------------------------------------------------------------
    # Equity curve plot
    # ------------------------------------------------------------------

    def plot_equity_curve(
        self,
        equity_curve: list,  # list[EquityPoint]
        strategy_name: str,
        filepath: str,
    ) -> None:
        """
        Plot equity curve and save as PNG.

        Shows:
        - Equity line over time
        - Drawdown periods shaded in red
        - Initial capital as horizontal dotted line
        - Title, axis labels, grid
        """
        if not equity_curve:
            logger.warning(
                "plot_equity_curve_skipped",
                reason="empty equity curve",
                strategy_name=strategy_name,
            )
            return

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import matplotlib.ticker as mticker

        timestamps = [
            datetime.fromtimestamp(p.timestamp_ms / 1000, tz=timezone.utc)
            for p in equity_curve
        ]
        equity_values = np.array(
            [float(p.equity) for p in equity_curve], dtype=np.float64
        )

        # Compute drawdown regions for shading
        running_max = np.maximum.accumulate(equity_values)
        in_drawdown = equity_values < running_max

        initial_capital = equity_values[0]

        fig, ax = plt.subplots(figsize=(12, 6))

        # Shade drawdown periods in red
        self._shade_drawdown_periods(ax, timestamps, equity_values, in_drawdown)

        # Plot equity line
        ax.plot(
            timestamps,
            equity_values,
            linewidth=1.8,
            color="#2E86AB",
            label="Equity",
            zorder=3,
        )

        # Initial capital reference line
        ax.axhline(
            y=initial_capital,
            color="gray",
            linestyle=":",
            linewidth=1,
            alpha=0.7,
            label=f"Initial (${initial_capital:,.2f})",
        )

        ax.set_title(f"Equity Curve: {strategy_name}", fontsize=14, fontweight="bold")
        ax.set_xlabel("Date (UTC)")
        ax.set_ylabel("Equity ($)")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)

        fig.autofmt_xdate()
        plt.tight_layout()

        # Ensure output directory exists
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)

        plt.savefig(filepath, dpi=150, bbox_inches="tight")
        plt.close(fig)

        logger.info(
            "plot_equity_curve_saved",
            filepath=filepath,
            strategy_name=strategy_name,
            num_points=len(equity_curve),
        )

    # ------------------------------------------------------------------
    # Drawdown plot
    # ------------------------------------------------------------------

    def plot_drawdown(
        self,
        equity_curve: list,  # list[EquityPoint]
        strategy_name: str,
        filepath: str,
    ) -> None:
        """
        Plot drawdown chart and save as PNG.

        Shows:
        - Drawdown percentage over time (negative values)
        - Max drawdown highlighted
        - Title, axis labels, grid
        """
        if not equity_curve:
            logger.warning(
                "plot_drawdown_skipped",
                reason="empty equity curve",
                strategy_name=strategy_name,
            )
            return

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker

        timestamps = [
            datetime.fromtimestamp(p.timestamp_ms / 1000, tz=timezone.utc)
            for p in equity_curve
        ]
        equity_values = np.array(
            [float(p.equity) for p in equity_curve], dtype=np.float64
        )

        running_max = np.maximum.accumulate(equity_values)
        drawdown_pct = ((equity_values - running_max) / running_max) * 100.0

        # Find max drawdown point for highlight
        max_dd_idx = int(np.argmin(drawdown_pct))

        fig, ax = plt.subplots(figsize=(12, 4))

        # Fill drawdown area in red
        ax.fill_between(
            timestamps,
            drawdown_pct,
            0,
            color="#E74C3C",
            alpha=0.35,
            label="Drawdown",
        )
        ax.plot(
            timestamps,
            drawdown_pct,
            linewidth=1.2,
            color="#E74C3C",
            alpha=0.8,
        )

        # Highlight max drawdown with a marker
        ax.scatter(
            [timestamps[max_dd_idx]],
            [drawdown_pct[max_dd_idx]],
            color="#8B0000",
            zorder=5,
            s=60,
            label=f"Max DD: {drawdown_pct[max_dd_idx]:.2f}%",
        )

        # Shade the max drawdown trough in darker red
        dd_start, dd_end = self._find_max_drawdown_period(
            equity_values, running_max
        )
        if dd_start is not None and dd_end is not None:
            ax.fill_between(
                timestamps[dd_start : dd_end + 1],
                drawdown_pct[dd_start : dd_end + 1],
                0,
                color="#8B0000",
                alpha=0.3,
            )

        ax.set_title(
            f"Drawdown: {strategy_name}", fontsize=14, fontweight="bold"
        )
        ax.set_xlabel("Date (UTC)")
        ax.set_ylabel("Drawdown (%)")
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x:.1f}%")
        )
        ax.legend(loc="lower left")
        ax.grid(True, alpha=0.3)

        fig.autofmt_xdate()
        plt.tight_layout()

        Path(filepath).parent.mkdir(parents=True, exist_ok=True)

        plt.savefig(filepath, dpi=150, bbox_inches="tight")
        plt.close(fig)

        logger.info(
            "plot_drawdown_saved",
            filepath=filepath,
            strategy_name=strategy_name,
            num_points=len(equity_curve),
        )

    # ------------------------------------------------------------------
    # CSV export
    # ------------------------------------------------------------------

    def export_trades_csv(
        self,
        trade_log: list,  # list[TradeRecord]
        filepath: str,
    ) -> None:
        """
        Export trade log as CSV.

        Columns: entry_time, exit_time, asset_id, side, quantity,
                 entry_price, exit_price, pnl, fees, is_winner

        Timestamps formatted as ISO8601.
        """
        if not trade_log:
            logger.warning(
                "export_trades_csv_skipped",
                reason="empty trade log",
            )
            return

        fieldnames = [
            "entry_time",
            "exit_time",
            "asset_id",
            "side",
            "quantity",
            "entry_price",
            "exit_price",
            "pnl",
            "fees",
            "is_winner",
        ]

        Path(filepath).parent.mkdir(parents=True, exist_ok=True)

        with open(filepath, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for trade in trade_log:
                entry_dt = datetime.fromtimestamp(
                    trade.entry_time_ms / 1000, tz=timezone.utc
                )
                exit_dt = (
                    datetime.fromtimestamp(
                        trade.exit_time_ms / 1000, tz=timezone.utc
                    )
                    if trade.exit_time_ms is not None
                    else None
                )

                writer.writerow(
                    {
                        "entry_time": entry_dt.isoformat(),
                        "exit_time": exit_dt.isoformat() if exit_dt else "",
                        "asset_id": trade.asset_id,
                        "side": trade.side,
                        "quantity": str(trade.quantity),
                        "entry_price": str(trade.entry_price),
                        "exit_price": (
                            str(trade.exit_price)
                            if trade.exit_price is not None
                            else ""
                        ),
                        "pnl": str(trade.realized_pnl),
                        "fees": str(trade.fees),
                        "is_winner": str(trade.is_winner),
                    }
                )

        logger.info(
            "trades_csv_exported",
            filepath=filepath,
            num_trades=len(trade_log),
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _shade_drawdown_periods(
        ax,
        timestamps: list[datetime],
        equity_values: np.ndarray,
        in_drawdown: np.ndarray,
    ) -> None:
        """Shade drawdown regions in light red on the equity curve chart."""
        i = 0
        n = len(in_drawdown)
        while i < n:
            if in_drawdown[i]:
                start = i
                while i < n and in_drawdown[i]:
                    i += 1
                end = i  # exclusive
                ax.axvspan(
                    timestamps[start],
                    timestamps[min(end, n - 1)],
                    color="#E74C3C",
                    alpha=0.1,
                    zorder=1,
                )
            else:
                i += 1

    @staticmethod
    def _find_max_drawdown_period(
        equity_values: np.ndarray,
        running_max: np.ndarray,
    ) -> tuple[Optional[int], Optional[int]]:
        """
        Find the start and end indices of the maximum drawdown period.

        The period starts where the running max was set (peak before the
        trough) and ends when equity recovers to that peak (or at the
        last index if it never recovers).

        Returns:
            (start_idx, end_idx) or (None, None) if no drawdown exists.
        """
        drawdowns = (equity_values - running_max) / running_max
        if len(drawdowns) == 0 or np.min(drawdowns) >= 0:
            return None, None

        trough_idx = int(np.argmin(drawdowns))

        # Walk backward from trough to find the peak that started this drawdown
        peak_val = running_max[trough_idx]
        dd_start = trough_idx
        for j in range(trough_idx - 1, -1, -1):
            if equity_values[j] >= peak_val:
                dd_start = j
                break
        else:
            dd_start = 0

        # Walk forward from trough to find recovery
        dd_end = len(equity_values) - 1
        for j in range(trough_idx + 1, len(equity_values)):
            if equity_values[j] >= peak_val:
                dd_end = j
                break

        return dd_start, dd_end

    @staticmethod
    def _format_duration_days(duration) -> str:
        """Format a timedelta into a human-readable days string."""
        total_seconds = int(duration.total_seconds())
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60

        if days > 0:
            if hours > 0:
                return f"{days} days, {hours}h"
            return f"{days} days"
        if hours > 0:
            if minutes > 0:
                return f"{hours}h {minutes}m"
            return f"{hours}h"
        return f"{minutes}m"

    @staticmethod
    def _format_duration_ms(duration_ms: float) -> str:
        """Format a millisecond duration into a human-readable string."""
        total_seconds = int(duration_ms / 1000)
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60

        if days > 0:
            if hours > 0:
                return f"{days}d {hours}h"
            return f"{days}d"
        if hours > 0:
            if minutes > 0:
                return f"{hours}h {minutes}m"
            return f"{hours}h"
        if minutes > 0:
            return f"{minutes}m"
        if total_seconds > 0:
            return f"{total_seconds}s"
        return "0s"
