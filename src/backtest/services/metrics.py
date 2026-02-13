"""
MetricsCollector for prediction market backtesting.

Tracks equity over time, records trades, and calculates standard trading
performance metrics including Sharpe ratio, Sortino ratio, max drawdown,
win rate, profit factor, and expectancy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

import numpy as np
import structlog

from ..models.order import Fill, OrderSide
from ..models.portfolio import Portfolio

logger = structlog.get_logger(__name__)


@dataclass
class TradeRecord:
    """Record of a completed trade (entry fill + optional exit fill)."""

    asset_id: str
    side: str  # "buy" or "sell"
    entry_price: Decimal
    exit_price: Optional[Decimal]  # None if still open
    quantity: Decimal
    entry_time_ms: int
    exit_time_ms: Optional[int]
    realized_pnl: Decimal
    fees: Decimal
    is_winner: bool


@dataclass
class EquityPoint:
    """Snapshot of portfolio equity at a point in time."""

    timestamp_ms: int
    equity: Decimal
    cash: Decimal
    position_value: Decimal


@dataclass
class _OpenTracker:
    """
    Internal tracker for an open position direction on a single asset.

    Accumulates entry fills until a closing fill reduces the position,
    at which point a TradeRecord is emitted.
    """

    asset_id: str
    side: str  # "buy" or "sell" (the entry side)
    entry_fills: list[Fill] = field(default_factory=list)
    total_quantity: Decimal = Decimal("0")
    total_cost: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
    first_entry_time_ms: int = 0

    @property
    def avg_entry_price(self) -> Decimal:
        if self.total_quantity == 0:
            return Decimal("0")
        return self.total_cost / self.total_quantity


class MetricsCollector:
    """
    Collects and computes performance metrics for a backtest.

    Tracks equity snapshots at a configurable interval, records fills to
    build trade records, and calculates standard trading performance metrics
    on demand.
    """

    def __init__(
        self,
        initial_cash: Decimal,
        equity_sample_interval_ms: int = 60_000,
    ):
        """
        Args:
            initial_cash: Starting capital for return calculations.
            equity_sample_interval_ms: Minimum interval between equity samples
                (default 1 minute). Keeps the equity curve manageable for long
                backtests.
        """
        self._initial_cash = initial_cash
        self._equity_sample_interval_ms = equity_sample_interval_ms

        self._equity_curve: list[EquityPoint] = []
        self._trade_log: list[TradeRecord] = []

        # Tracks open position entry fills per asset so we can pair them with
        # closing fills to produce TradeRecords.
        self._open_trackers: dict[str, _OpenTracker] = {}

        # Timestamp of the last equity sample (for time-gating).
        self._last_sample_ts: Optional[int] = None

        logger.info(
            "metrics_collector_initialized",
            initial_cash=str(initial_cash),
            equity_sample_interval_ms=equity_sample_interval_ms,
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def record_fill(self, fill: Fill, portfolio: Portfolio) -> None:
        """
        Record a fill and update trade tracking.

        Pairs entry and exit fills for the same asset into TradeRecords.
        Also samples equity if the configured interval has elapsed since the
        last sample.

        Args:
            fill: The execution fill to record.
            portfolio: Current portfolio state (used for equity sampling).
        """
        self._process_fill_for_trades(fill)

        # Time-gated equity sampling
        if self._last_sample_ts is None or (
            fill.timestamp_ms - self._last_sample_ts
            >= self._equity_sample_interval_ms
        ):
            self._sample_equity(fill.timestamp_ms, portfolio)

    def record_equity_point(
        self,
        timestamp_ms: int,
        portfolio: Portfolio,
        prices: dict[str, Decimal],
    ) -> None:
        """
        Record an equity snapshot at the given timestamp.

        Updates portfolio mark prices before computing equity so that
        position values reflect the supplied prices.

        Args:
            timestamp_ms: The point-in-time for this snapshot.
            portfolio: The portfolio to snapshot.
            prices: Current asset prices for mark-to-market.
        """
        portfolio.update_mark_prices(prices)
        self._sample_equity(timestamp_ms, portfolio)

    def calculate_metrics(self) -> dict[str, float]:
        """
        Calculate all performance metrics.

        Returns:
            Dictionary with the following keys:
            - total_return_pct
            - annualized_return_pct
            - sharpe_ratio
            - sortino_ratio
            - max_drawdown_pct
            - max_drawdown_duration_ms
            - win_rate
            - profit_factor
            - expectancy
            - num_trades
            - num_winning_trades
            - num_losing_trades
            - avg_trade_pnl
            - total_fees
            - fees_pct_of_volume
        """
        metrics: dict[str, float] = {}

        # ---- Return metrics from equity curve ----
        metrics.update(self._compute_return_metrics())

        # ---- Risk metrics from equity curve ----
        metrics.update(self._compute_risk_metrics())

        # ---- Trade-level metrics ----
        metrics.update(self._compute_trade_metrics())

        logger.info("metrics_calculated", **{k: f"{v:.6f}" for k, v in metrics.items()})
        return metrics

    def get_equity_curve(self) -> list[EquityPoint]:
        """Return the full equity curve."""
        return list(self._equity_curve)

    def get_trade_log(self) -> list[TradeRecord]:
        """Return all trade records."""
        return list(self._trade_log)

    # ------------------------------------------------------------------
    # Fill / trade tracking internals
    # ------------------------------------------------------------------

    def _process_fill_for_trades(self, fill: Fill) -> None:
        """
        Track a fill and, when it reduces an open position, emit a
        TradeRecord.

        Logic:
        - If no open tracker exists for this asset, or the fill is on the
          *same* side as the tracker (increasing position), accumulate.
        - If the fill is on the *opposite* side (closing), produce one or
          more TradeRecords for the reduced quantity.
        """
        asset_id = fill.asset_id
        fill_side = fill.side.value  # "buy" or "sell"
        tracker = self._open_trackers.get(asset_id)

        if tracker is None:
            # No open tracker -- start a new one for this entry side.
            self._open_trackers[asset_id] = _OpenTracker(
                asset_id=asset_id,
                side=fill_side,
                entry_fills=[fill],
                total_quantity=fill.quantity,
                total_cost=fill.price * fill.quantity,
                total_fees=fill.fees,
                first_entry_time_ms=fill.timestamp_ms,
            )
            return

        if fill_side == tracker.side:
            # Same direction -- accumulate into the open tracker.
            tracker.entry_fills.append(fill)
            tracker.total_quantity += fill.quantity
            tracker.total_cost += fill.price * fill.quantity
            tracker.total_fees += fill.fees
            if tracker.first_entry_time_ms == 0:
                tracker.first_entry_time_ms = fill.timestamp_ms
            return

        # Opposite direction -- closing (fully or partially).
        close_qty = fill.quantity
        avg_entry = tracker.avg_entry_price
        entry_time = tracker.first_entry_time_ms

        if close_qty >= tracker.total_quantity:
            # Full close (possibly overflows into new position on other side).
            matched_qty = tracker.total_quantity
            remaining_qty = close_qty - matched_qty

            # Compute realized P&L for this trade.
            if tracker.side == "buy":
                realized = (fill.price - avg_entry) * matched_qty
            else:
                realized = (avg_entry - fill.price) * matched_qty

            # Proportionally attribute fees.
            entry_fees = tracker.total_fees
            exit_fees = (
                fill.fees * (matched_qty / fill.quantity)
                if fill.quantity > 0
                else Decimal("0")
            )
            total_fees = entry_fees + exit_fees

            self._trade_log.append(
                TradeRecord(
                    asset_id=asset_id,
                    side=tracker.side,
                    entry_price=avg_entry,
                    exit_price=fill.price,
                    quantity=matched_qty,
                    entry_time_ms=entry_time,
                    exit_time_ms=fill.timestamp_ms,
                    realized_pnl=realized,
                    fees=total_fees,
                    is_winner=realized > 0,
                )
            )

            # Remove the closed tracker.
            del self._open_trackers[asset_id]

            # If there is leftover quantity, it starts a new position.
            if remaining_qty > Decimal("0"):
                leftover_fees = fill.fees - exit_fees
                self._open_trackers[asset_id] = _OpenTracker(
                    asset_id=asset_id,
                    side=fill_side,
                    entry_fills=[fill],
                    total_quantity=remaining_qty,
                    total_cost=fill.price * remaining_qty,
                    total_fees=leftover_fees,
                    first_entry_time_ms=fill.timestamp_ms,
                )
        else:
            # Partial close -- reduce the tracker.
            if tracker.side == "buy":
                realized = (fill.price - avg_entry) * close_qty
            else:
                realized = (avg_entry - fill.price) * close_qty

            # Proportional entry fee attribution.
            fee_fraction = (
                close_qty / tracker.total_quantity
                if tracker.total_quantity > 0
                else Decimal("0")
            )
            attributed_entry_fees = tracker.total_fees * fee_fraction
            total_fees = attributed_entry_fees + fill.fees

            self._trade_log.append(
                TradeRecord(
                    asset_id=asset_id,
                    side=tracker.side,
                    entry_price=avg_entry,
                    exit_price=fill.price,
                    quantity=close_qty,
                    entry_time_ms=entry_time,
                    exit_time_ms=fill.timestamp_ms,
                    realized_pnl=realized,
                    fees=total_fees,
                    is_winner=realized > 0,
                )
            )

            # Reduce the tracker.
            tracker.total_cost -= avg_entry * close_qty
            tracker.total_quantity -= close_qty
            tracker.total_fees -= attributed_entry_fees

    # ------------------------------------------------------------------
    # Equity sampling
    # ------------------------------------------------------------------

    def _sample_equity(self, timestamp_ms: int, portfolio: Portfolio) -> None:
        """Record an equity point and update the last-sample timestamp."""
        equity = portfolio.total_value
        cash = portfolio.cash
        position_value = equity - cash

        self._equity_curve.append(
            EquityPoint(
                timestamp_ms=timestamp_ms,
                equity=equity,
                cash=cash,
                position_value=position_value,
            )
        )
        self._last_sample_ts = timestamp_ms

    # ------------------------------------------------------------------
    # Metric computation helpers
    # ------------------------------------------------------------------

    def _compute_return_metrics(self) -> dict[str, float]:
        """Compute total and annualized return from the equity curve."""
        if len(self._equity_curve) < 2:
            return {
                "total_return_pct": 0.0,
                "annualized_return_pct": 0.0,
            }

        initial = float(self._initial_cash)
        final = float(self._equity_curve[-1].equity)

        if initial == 0:
            return {
                "total_return_pct": 0.0,
                "annualized_return_pct": 0.0,
            }

        total_return = (final - initial) / initial
        total_return_pct = total_return * 100.0

        # Annualize using elapsed time.
        elapsed_ms = (
            self._equity_curve[-1].timestamp_ms
            - self._equity_curve[0].timestamp_ms
        )
        elapsed_years = elapsed_ms / (365.25 * 24 * 3600 * 1000)

        if elapsed_years > 0 and (1 + total_return) > 0:
            annualized = ((1 + total_return) ** (1 / elapsed_years)) - 1
            annualized_return_pct = annualized * 100.0
        else:
            annualized_return_pct = 0.0

        return {
            "total_return_pct": total_return_pct,
            "annualized_return_pct": annualized_return_pct,
        }

    def _compute_risk_metrics(self) -> dict[str, float]:
        """
        Compute Sharpe, Sortino, max drawdown, and max drawdown duration
        from the equity curve.
        """
        if len(self._equity_curve) < 2:
            return {
                "sharpe_ratio": 0.0,
                "sortino_ratio": 0.0,
                "max_drawdown_pct": 0.0,
                "max_drawdown_duration_ms": 0.0,
            }

        equity_values = np.array(
            [float(p.equity) for p in self._equity_curve], dtype=np.float64
        )
        timestamps = np.array(
            [p.timestamp_ms for p in self._equity_curve], dtype=np.int64
        )

        # Period-over-period returns (guard against zero equity).
        denominator = equity_values[:-1]
        returns = np.where(
            denominator == 0,
            0.0,
            np.diff(equity_values) / np.where(denominator == 0, 1.0, denominator),
        )

        # ---- Sharpe ratio ----
        # Prediction markets run 24/7, so use 365 days/year for annualization.
        std = float(np.std(returns))
        sharpe = (
            float(np.mean(returns)) / std * np.sqrt(365)
            if std > 0
            else 0.0
        )

        # ---- Sortino ratio ----
        downside = returns[returns < 0]
        downside_std = float(np.std(downside)) if len(downside) > 0 else 0.0
        sortino = (
            float(np.mean(returns)) / downside_std * np.sqrt(365)
            if downside_std > 0
            else 0.0
        )

        # ---- Max drawdown ----
        running_max = np.maximum.accumulate(equity_values)
        drawdowns = np.where(
            running_max == 0,
            0.0,
            (equity_values - running_max) / np.where(running_max == 0, 1.0, running_max),
        )
        max_drawdown = float(np.min(drawdowns)) if len(drawdowns) > 0 else 0.0
        max_drawdown_pct = max_drawdown * 100.0

        # ---- Max drawdown duration ----
        max_dd_duration_ms = self._compute_max_drawdown_duration(
            equity_values, running_max, timestamps
        )

        return {
            "sharpe_ratio": sharpe,
            "sortino_ratio": sortino,
            "max_drawdown_pct": max_drawdown_pct,
            "max_drawdown_duration_ms": float(max_dd_duration_ms),
        }

    @staticmethod
    def _compute_max_drawdown_duration(
        equity: np.ndarray,
        running_max: np.ndarray,
        timestamps: np.ndarray,
    ) -> int:
        """
        Find the longest contiguous period (in ms) where equity is below
        its running maximum.

        Returns:
            Duration in milliseconds of the longest drawdown period.
        """
        in_drawdown = equity < running_max
        max_duration = 0
        dd_start_idx: Optional[int] = None

        for i in range(len(in_drawdown)):
            if in_drawdown[i]:
                if dd_start_idx is None:
                    dd_start_idx = i
            else:
                if dd_start_idx is not None:
                    duration = int(timestamps[i] - timestamps[dd_start_idx])
                    max_duration = max(max_duration, duration)
                    dd_start_idx = None

        # Handle drawdown that extends to the end of the curve.
        if dd_start_idx is not None:
            duration = int(timestamps[-1] - timestamps[dd_start_idx])
            max_duration = max(max_duration, duration)

        return max_duration

    def _compute_trade_metrics(self) -> dict[str, float]:
        """Compute trade-level performance statistics."""
        trades = self._trade_log
        num_trades = len(trades)

        if num_trades == 0:
            return {
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "expectancy": 0.0,
                "num_trades": 0.0,
                "num_winning_trades": 0.0,
                "num_losing_trades": 0.0,
                "avg_trade_pnl": 0.0,
                "total_fees": 0.0,
                "fees_pct_of_volume": 0.0,
            }

        winning = [t for t in trades if t.realized_pnl > 0]
        losing = [t for t in trades if t.realized_pnl <= 0]

        num_winning = len(winning)
        num_losing = len(losing)
        win_rate = num_winning / num_trades

        gross_profit = sum(
            (t.realized_pnl for t in winning), Decimal("0")
        )
        gross_loss = abs(
            sum((t.realized_pnl for t in losing), Decimal("0"))
        )

        profit_factor = (
            float(gross_profit / gross_loss)
            if gross_loss > 0
            else float("inf")
        )

        avg_win = float(gross_profit / num_winning) if num_winning > 0 else 0.0
        avg_loss = float(gross_loss / num_losing) if num_losing > 0 else 0.0
        expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

        total_pnl = sum((t.realized_pnl for t in trades), Decimal("0"))
        avg_trade_pnl = float(total_pnl / num_trades)

        total_fees = sum((t.fees for t in trades), Decimal("0"))
        total_volume = sum(
            (t.entry_price * t.quantity for t in trades), Decimal("0")
        )
        fees_pct_of_volume = (
            float(total_fees / total_volume * 100)
            if total_volume > 0
            else 0.0
        )

        return {
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "expectancy": expectancy,
            "num_trades": float(num_trades),
            "num_winning_trades": float(num_winning),
            "num_losing_trades": float(num_losing),
            "avg_trade_pnl": avg_trade_pnl,
            "total_fees": float(total_fees),
            "fees_pct_of_volume": fees_pct_of_volume,
        }
