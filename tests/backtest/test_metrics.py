"""
Tests for MetricsCollector.

Covers record_fill pairing into TradeRecords, calculate_metrics expected keys,
equity curve recording, win_rate, and profit_factor with known inputs.
"""

from decimal import Decimal

import pytest

from backtest.models.order import Fill, OrderSide, FillReason
from backtest.models.portfolio import Portfolio
from backtest.services.metrics import MetricsCollector, TradeRecord, EquityPoint


# ======================================================================
# Helpers
# ======================================================================


def _make_fill(
    order_id: str,
    asset_id: str,
    side: OrderSide,
    price: str,
    qty: str,
    fees: str = "0",
    timestamp_ms: int = 1700000000000,
    is_maker: bool = True,
) -> Fill:
    return Fill(
        order_id=order_id,
        asset_id=asset_id,
        side=side,
        price=Decimal(price),
        quantity=Decimal(qty),
        fees=Decimal(fees),
        timestamp_ms=timestamp_ms,
        is_maker=is_maker,
        fill_reason=FillReason.IMMEDIATE,
    )


def _make_portfolio(initial_cash: str = "10000") -> Portfolio:
    return Portfolio(initial_cash=Decimal(initial_cash))


# ======================================================================
# record_fill pairs entries and exits into TradeRecords
# ======================================================================


class TestRecordFill:

    def test_entry_fill_does_not_create_trade_record(self):
        mc = MetricsCollector(initial_cash=Decimal("10000"), equity_sample_interval_ms=0)
        portfolio = _make_portfolio()

        entry = _make_fill("o1", "token-1", OrderSide.BUY, "0.50", "10")
        portfolio.apply_fill(entry)
        mc.record_fill(entry, portfolio)

        assert len(mc.get_trade_log()) == 0

    def test_exit_fill_creates_trade_record(self):
        mc = MetricsCollector(initial_cash=Decimal("10000"), equity_sample_interval_ms=0)
        portfolio = _make_portfolio()

        entry = _make_fill("o1", "token-1", OrderSide.BUY, "0.50", "10", timestamp_ms=1000)
        portfolio.apply_fill(entry)
        mc.record_fill(entry, portfolio)

        exit_fill = _make_fill("o2", "token-1", OrderSide.SELL, "0.60", "10", timestamp_ms=2000)
        portfolio.apply_fill(exit_fill)
        mc.record_fill(exit_fill, portfolio)

        trades = mc.get_trade_log()
        assert len(trades) == 1
        assert trades[0].side == "buy"
        assert trades[0].entry_price == Decimal("0.50")
        assert trades[0].exit_price == Decimal("0.60")
        assert trades[0].quantity == Decimal("10")
        assert trades[0].entry_time_ms == 1000
        assert trades[0].exit_time_ms == 2000

    def test_winning_trade_marked_is_winner(self):
        mc = MetricsCollector(initial_cash=Decimal("10000"), equity_sample_interval_ms=0)
        portfolio = _make_portfolio()

        entry = _make_fill("o1", "token-1", OrderSide.BUY, "0.40", "10", timestamp_ms=1000)
        portfolio.apply_fill(entry)
        mc.record_fill(entry, portfolio)

        exit_fill = _make_fill("o2", "token-1", OrderSide.SELL, "0.60", "10", timestamp_ms=2000)
        portfolio.apply_fill(exit_fill)
        mc.record_fill(exit_fill, portfolio)

        trades = mc.get_trade_log()
        assert trades[0].is_winner is True
        # PnL = (0.60 - 0.40) * 10 = 2.00
        assert trades[0].realized_pnl == Decimal("2.00")

    def test_losing_trade_marked_not_winner(self):
        mc = MetricsCollector(initial_cash=Decimal("10000"), equity_sample_interval_ms=0)
        portfolio = _make_portfolio()

        entry = _make_fill("o1", "token-1", OrderSide.BUY, "0.60", "10", timestamp_ms=1000)
        portfolio.apply_fill(entry)
        mc.record_fill(entry, portfolio)

        exit_fill = _make_fill("o2", "token-1", OrderSide.SELL, "0.40", "10", timestamp_ms=2000)
        portfolio.apply_fill(exit_fill)
        mc.record_fill(exit_fill, portfolio)

        trades = mc.get_trade_log()
        assert trades[0].is_winner is False
        assert trades[0].realized_pnl == Decimal("-2.00")

    def test_partial_close_creates_trade_record(self):
        mc = MetricsCollector(initial_cash=Decimal("10000"), equity_sample_interval_ms=0)
        portfolio = _make_portfolio()

        entry = _make_fill("o1", "token-1", OrderSide.BUY, "0.50", "20", timestamp_ms=1000)
        portfolio.apply_fill(entry)
        mc.record_fill(entry, portfolio)

        # Sell only 10 of 20
        exit_fill = _make_fill("o2", "token-1", OrderSide.SELL, "0.60", "10", timestamp_ms=2000)
        portfolio.apply_fill(exit_fill)
        mc.record_fill(exit_fill, portfolio)

        trades = mc.get_trade_log()
        assert len(trades) == 1
        assert trades[0].quantity == Decimal("10")
        assert trades[0].realized_pnl == Decimal("1.00")

    def test_fees_tracked_in_trade_record(self):
        mc = MetricsCollector(initial_cash=Decimal("10000"), equity_sample_interval_ms=0)
        portfolio = _make_portfolio()

        entry = _make_fill("o1", "token-1", OrderSide.BUY, "0.50", "10", fees="0.05", timestamp_ms=1000)
        portfolio.apply_fill(entry)
        mc.record_fill(entry, portfolio)

        exit_fill = _make_fill("o2", "token-1", OrderSide.SELL, "0.60", "10", fees="0.06", timestamp_ms=2000)
        portfolio.apply_fill(exit_fill)
        mc.record_fill(exit_fill, portfolio)

        trades = mc.get_trade_log()
        assert trades[0].fees == Decimal("0.11")


# ======================================================================
# calculate_metrics returns expected keys
# ======================================================================


class TestCalculateMetricsKeys:

    def test_all_expected_keys_present(self):
        mc = MetricsCollector(initial_cash=Decimal("10000"))
        portfolio = _make_portfolio()

        # Record at least two equity points for metrics to compute
        entry = _make_fill("o1", "token-1", OrderSide.BUY, "0.50", "10", timestamp_ms=1000)
        portfolio.apply_fill(entry)
        mc.record_fill(entry, portfolio)

        exit_fill = _make_fill("o2", "token-1", OrderSide.SELL, "0.60", "10", timestamp_ms=120000)
        portfolio.apply_fill(exit_fill)
        mc.record_fill(exit_fill, portfolio)

        metrics = mc.calculate_metrics()

        expected_keys = {
            "total_return_pct",
            "annualized_return_pct",
            "sharpe_ratio",
            "sortino_ratio",
            "max_drawdown_pct",
            "max_drawdown_duration_ms",
            "win_rate",
            "profit_factor",
            "expectancy",
            "num_trades",
            "num_winning_trades",
            "num_losing_trades",
            "avg_trade_pnl",
            "total_fees",
            "fees_pct_of_volume",
        }
        assert set(metrics.keys()) == expected_keys

    def test_metrics_with_no_trades(self):
        mc = MetricsCollector(initial_cash=Decimal("10000"))
        metrics = mc.calculate_metrics()

        assert metrics["num_trades"] == 0.0
        assert metrics["win_rate"] == 0.0
        assert metrics["profit_factor"] == 0.0


# ======================================================================
# Equity curve recording
# ======================================================================


class TestEquityCurveRecording:

    def test_equity_point_recorded_on_fill(self):
        mc = MetricsCollector(
            initial_cash=Decimal("10000"),
            equity_sample_interval_ms=0,  # Always sample
        )
        portfolio = _make_portfolio()

        fill = _make_fill("o1", "token-1", OrderSide.BUY, "0.50", "10", timestamp_ms=1000)
        portfolio.apply_fill(fill)
        mc.record_fill(fill, portfolio)

        curve = mc.get_equity_curve()
        assert len(curve) == 1
        assert curve[0].timestamp_ms == 1000

    def test_equity_point_respects_sample_interval(self):
        mc = MetricsCollector(
            initial_cash=Decimal("10000"),
            equity_sample_interval_ms=60_000,  # 1 min
        )
        portfolio = _make_portfolio()

        fill1 = _make_fill("o1", "token-1", OrderSide.BUY, "0.50", "10", timestamp_ms=1000)
        portfolio.apply_fill(fill1)
        mc.record_fill(fill1, portfolio)

        # Second fill only 10ms later — should NOT sample again
        fill2 = _make_fill("o2", "token-1", OrderSide.BUY, "0.50", "10", timestamp_ms=1010)
        portfolio.apply_fill(fill2)
        mc.record_fill(fill2, portfolio)

        curve = mc.get_equity_curve()
        assert len(curve) == 1

        # Third fill 61 seconds later — should sample
        fill3 = _make_fill("o3", "token-1", OrderSide.BUY, "0.50", "10", timestamp_ms=62000)
        portfolio.apply_fill(fill3)
        mc.record_fill(fill3, portfolio)

        curve = mc.get_equity_curve()
        assert len(curve) == 2

    def test_record_equity_point_explicit(self):
        mc = MetricsCollector(
            initial_cash=Decimal("10000"),
            equity_sample_interval_ms=0,
        )
        portfolio = _make_portfolio()

        mc.record_equity_point(
            timestamp_ms=5000,
            portfolio=portfolio,
            prices={},
        )
        curve = mc.get_equity_curve()
        assert len(curve) == 1
        assert curve[0].equity == Decimal("10000")
        assert curve[0].cash == Decimal("10000")


# ======================================================================
# win_rate and profit_factor with known inputs
# ======================================================================


class TestWinRateAndProfitFactor:

    def _run_known_trades(self):
        """Execute a known sequence of trades and return the MetricsCollector."""
        mc = MetricsCollector(initial_cash=Decimal("10000"), equity_sample_interval_ms=0)
        portfolio = _make_portfolio()

        # Trade 1: Buy at 0.40, sell at 0.60 => PnL = +2.00 (winner)
        e1 = _make_fill("o1", "t1", OrderSide.BUY, "0.40", "10", timestamp_ms=1000)
        portfolio.apply_fill(e1)
        mc.record_fill(e1, portfolio)
        x1 = _make_fill("o2", "t1", OrderSide.SELL, "0.60", "10", timestamp_ms=2000)
        portfolio.apply_fill(x1)
        mc.record_fill(x1, portfolio)

        # Trade 2: Buy at 0.50, sell at 0.55 => PnL = +0.50 (winner)
        e2 = _make_fill("o3", "t2", OrderSide.BUY, "0.50", "10", timestamp_ms=3000)
        portfolio.apply_fill(e2)
        mc.record_fill(e2, portfolio)
        x2 = _make_fill("o4", "t2", OrderSide.SELL, "0.55", "10", timestamp_ms=4000)
        portfolio.apply_fill(x2)
        mc.record_fill(x2, portfolio)

        # Trade 3: Buy at 0.60, sell at 0.50 => PnL = -1.00 (loser)
        e3 = _make_fill("o5", "t3", OrderSide.BUY, "0.60", "10", timestamp_ms=5000)
        portfolio.apply_fill(e3)
        mc.record_fill(e3, portfolio)
        x3 = _make_fill("o6", "t3", OrderSide.SELL, "0.50", "10", timestamp_ms=6000)
        portfolio.apply_fill(x3)
        mc.record_fill(x3, portfolio)

        # Trade 4: Buy at 0.70, sell at 0.40 => PnL = -3.00 (loser)
        e4 = _make_fill("o7", "t4", OrderSide.BUY, "0.70", "10", timestamp_ms=7000)
        portfolio.apply_fill(e4)
        mc.record_fill(e4, portfolio)
        x4 = _make_fill("o8", "t4", OrderSide.SELL, "0.40", "10", timestamp_ms=8000)
        portfolio.apply_fill(x4)
        mc.record_fill(x4, portfolio)

        return mc

    def test_num_trades(self):
        mc = self._run_known_trades()
        metrics = mc.calculate_metrics()
        assert metrics["num_trades"] == 4.0

    def test_num_winning_and_losing(self):
        mc = self._run_known_trades()
        metrics = mc.calculate_metrics()
        assert metrics["num_winning_trades"] == 2.0
        assert metrics["num_losing_trades"] == 2.0

    def test_win_rate(self):
        mc = self._run_known_trades()
        metrics = mc.calculate_metrics()
        # 2 winners / 4 trades = 0.5
        assert metrics["win_rate"] == pytest.approx(0.5)

    def test_profit_factor(self):
        mc = self._run_known_trades()
        metrics = mc.calculate_metrics()
        # Gross profit = 2.00 + 0.50 = 2.50
        # Gross loss = |(-1.00) + (-3.00)| = 4.00
        # Profit factor = 2.50 / 4.00 = 0.625
        assert metrics["profit_factor"] == pytest.approx(0.625)

    def test_avg_trade_pnl(self):
        mc = self._run_known_trades()
        metrics = mc.calculate_metrics()
        # Total PnL = 2.00 + 0.50 - 1.00 - 3.00 = -1.50
        # Avg = -1.50 / 4 = -0.375
        assert metrics["avg_trade_pnl"] == pytest.approx(-0.375)

    def test_expectancy(self):
        mc = self._run_known_trades()
        metrics = mc.calculate_metrics()
        # win_rate = 0.5
        # avg_win = 2.50 / 2 = 1.25
        # avg_loss = 4.00 / 2 = 2.00
        # expectancy = 0.5 * 1.25 - 0.5 * 2.00 = 0.625 - 1.0 = -0.375
        assert metrics["expectancy"] == pytest.approx(-0.375)


# ======================================================================
# Short-side trade records
# ======================================================================


class TestShortSideTradeRecords:

    def test_short_trade_record(self):
        mc = MetricsCollector(initial_cash=Decimal("10000"), equity_sample_interval_ms=0)
        portfolio = _make_portfolio()

        # Sell at 0.60 (open short), then buy at 0.50 (close short)
        entry = _make_fill("o1", "token-1", OrderSide.SELL, "0.60", "10", timestamp_ms=1000)
        portfolio.apply_fill(entry)
        mc.record_fill(entry, portfolio)

        close = _make_fill("o2", "token-1", OrderSide.BUY, "0.50", "10", timestamp_ms=2000)
        portfolio.apply_fill(close)
        mc.record_fill(close, portfolio)

        trades = mc.get_trade_log()
        assert len(trades) == 1
        assert trades[0].side == "sell"
        # Short PnL = (entry - exit) * qty = (0.60 - 0.50) * 10 = 1.00
        assert trades[0].realized_pnl == Decimal("1.00")
        assert trades[0].is_winner is True


# ======================================================================
# Profit factor edge case: no losers
# ======================================================================


class TestProfitFactorEdge:

    def test_profit_factor_infinity_when_no_losers(self):
        mc = MetricsCollector(initial_cash=Decimal("10000"), equity_sample_interval_ms=0)
        portfolio = _make_portfolio()

        # Space timestamps far enough apart to avoid annualized return overflow
        entry = _make_fill("o1", "token-1", OrderSide.BUY, "0.40", "10", timestamp_ms=1_000_000_000_000)
        portfolio.apply_fill(entry)
        mc.record_fill(entry, portfolio)

        # 30 days later
        exit_fill = _make_fill(
            "o2", "token-1", OrderSide.SELL, "0.60", "10",
            timestamp_ms=1_000_000_000_000 + 30 * 86_400_000,
        )
        portfolio.apply_fill(exit_fill)
        mc.record_fill(exit_fill, portfolio)

        metrics = mc.calculate_metrics()
        assert metrics["profit_factor"] == float("inf")
