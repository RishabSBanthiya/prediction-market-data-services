"""
Backtest configuration and result models.

This module defines the core configuration for backtests, fee schedules,
and result reporting structures.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, model_validator


class BacktestConfig(BaseModel):
    """Configuration for a backtest run."""

    postgres_dsn: str
    platform: Optional[str] = None
    start_time_ms: int
    end_time_ms: int
    asset_ids: Optional[list[str]] = None
    listener_id: Optional[str] = None
    initial_cash: float = 10000.0
    include_forward_filled: bool = False
    maker_fee_bps: int = 0
    taker_fee_bps: int = 0
    max_events_in_memory: int = 5_000_000

    @model_validator(mode="after")
    def validate_config(self):
        if self.start_time_ms >= self.end_time_ms:
            raise ValueError(
                f"start_time_ms ({self.start_time_ms}) must be less than "
                f"end_time_ms ({self.end_time_ms})"
            )

        if self.initial_cash <= 0:
            raise ValueError(
                f"initial_cash must be positive, got {self.initial_cash}"
            )

        if self.asset_ids is None and self.listener_id is None:
            raise ValueError(
                "Must specify either asset_ids or listener_id to define backtest scope"
            )

        if self.platform is not None and self.platform not in ["polymarket", "kalshi"]:
            raise ValueError(
                f"platform must be 'polymarket', 'kalshi', or None, got '{self.platform}'"
            )

        if self.maker_fee_bps < 0 or self.taker_fee_bps < 0:
            raise ValueError(
                f"Fee rates cannot be negative: maker={self.maker_fee_bps}, "
                f"taker={self.taker_fee_bps}"
            )

        if self.max_events_in_memory <= 0:
            raise ValueError(
                f"max_events_in_memory must be positive, got {self.max_events_in_memory}"
            )

        return self


@dataclass
class FeeSchedule:
    """Fee schedule for a trading platform."""

    maker_fee_bps: int
    taker_fee_bps: int

    def calculate_fee(
        self, quantity: Decimal, price: Decimal, is_maker: bool
    ) -> Decimal:
        """Calculate trading fee for an order."""
        fee_bps = self.maker_fee_bps if is_maker else self.taker_fee_bps
        notional = quantity * price
        return notional * Decimal(fee_bps) / Decimal(10000)

    @classmethod
    def polymarket(cls) -> "FeeSchedule":
        """Standard Polymarket fee schedule (0 bps maker/taker)."""
        return cls(maker_fee_bps=0, taker_fee_bps=0)

    @classmethod
    def kalshi(cls) -> "FeeSchedule":
        """Standard Kalshi fee schedule (50 bps maker, 150 bps taker)."""
        return cls(maker_fee_bps=50, taker_fee_bps=150)


class BacktestResult(BaseModel):
    """Results from a backtest run."""

    config: BacktestConfig
    strategy_name: str
    total_return: float
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    max_drawdown: float
    win_rate: float
    profit_factor: Optional[float] = None
    num_trades: int
    num_winning_trades: int
    num_losing_trades: int
    avg_win: float
    avg_loss: float
    total_fees_paid: float
    equity_curve: list[tuple[int, float]]
    drawdown_curve: list[tuple[int, float]]
    final_equity: float

    def summary(self) -> str:
        """Generate a formatted human-readable summary of backtest results."""
        lines = [
            "=" * 70,
            f"Backtest Results: {self.strategy_name}",
            "=" * 70,
            "",
            "Configuration:",
            f"  Platform:           {self.config.platform or 'All'}",
            f"  Start Time:         {self.config.start_time_ms}",
            f"  End Time:           {self.config.end_time_ms}",
            f"  Initial Cash:       ${self.config.initial_cash:,.2f}",
            f"  Forward-Filled:     {self.config.include_forward_filled}",
            f"  Maker Fee:          {self.config.maker_fee_bps} bps",
            f"  Taker Fee:          {self.config.taker_fee_bps} bps",
            "",
            "Performance:",
            f"  Final Equity:       ${self.final_equity:,.2f}",
            f"  Total Return:       {self.total_return:+.2%}",
            f"  Max Drawdown:       {self.max_drawdown:.2%}",
            f"  Sharpe Ratio:       {f'{self.sharpe_ratio:.3f}' if self.sharpe_ratio is not None else 'N/A'}",
            f"  Sortino Ratio:      {f'{self.sortino_ratio:.3f}' if self.sortino_ratio is not None else 'N/A'}",
            "",
            "Trading Statistics:",
            f"  Total Trades:       {self.num_trades}",
            f"  Winning Trades:     {self.num_winning_trades}",
            f"  Losing Trades:      {self.num_losing_trades}",
            f"  Win Rate:           {self.win_rate:.2%}",
            f"  Average Win:        ${self.avg_win:,.2f}",
            f"  Average Loss:       ${self.avg_loss:,.2f}",
            f"  Profit Factor:      {f'{self.profit_factor:.3f}' if self.profit_factor is not None else 'N/A'}",
            f"  Total Fees Paid:    ${self.total_fees_paid:,.2f}",
            "",
            "=" * 70,
        ]
        return "\n".join(lines)

    def plot_equity(self, output_path: str) -> None:
        """Generate equity curve plot with drawdown visualization."""
        import matplotlib.pyplot as plt
        from datetime import datetime

        if not self.equity_curve:
            raise ValueError("No equity curve data to plot")

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

        timestamps = [datetime.fromtimestamp(ts / 1000) for ts, _ in self.equity_curve]
        equity_values = [val for _, val in self.equity_curve]
        drawdown_values = [dd * 100 for _, dd in self.drawdown_curve]

        ax1.plot(timestamps, equity_values, linewidth=2, color="#2E86AB", label="Equity")
        ax1.axhline(
            y=self.config.initial_cash,
            color="gray",
            linestyle="--",
            linewidth=1,
            alpha=0.7,
            label="Initial Cash",
        )
        ax1.set_ylabel("Portfolio Value ($)")
        ax1.set_title(
            f"Backtest Results: {self.strategy_name}\n"
            f"Return: {self.total_return:+.2%} | Max DD: {self.max_drawdown:.2%} | "
            f"Sharpe: {f'{self.sharpe_ratio:.2f}' if self.sharpe_ratio is not None else 'N/A'}"
        )
        ax1.legend(loc="upper left")
        ax1.grid(True, alpha=0.3)

        ax2.fill_between(
            timestamps, drawdown_values, 0, color="#A23B72", alpha=0.5, label="Drawdown"
        )
        ax2.plot(timestamps, drawdown_values, linewidth=1.5, color="#A23B72")
        ax2.set_ylabel("Drawdown (%)")
        ax2.set_xlabel("Date")
        ax2.legend(loc="lower left")
        ax2.grid(True, alpha=0.3)
        ax2.invert_yaxis()

        fig.autofmt_xdate()
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
