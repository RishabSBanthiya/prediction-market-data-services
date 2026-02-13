"""
Tests for BacktestConfig, FeeSchedule, and BacktestResult models.

Covers creation, validation, fee calculation, factory methods,
and BacktestResult.summary() formatting.
"""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from backtest.models.config import BacktestConfig, FeeSchedule, BacktestResult


# ======================================================================
# BacktestConfig creation
# ======================================================================


class TestBacktestConfigCreation:

    def test_valid_config_with_asset_ids(self):
        config = BacktestConfig(
            postgres_dsn="postgresql://localhost/test",
            start_time_ms=1000,
            end_time_ms=2000,
            asset_ids=["token-1", "token-2"],
        )
        assert config.start_time_ms == 1000
        assert config.end_time_ms == 2000
        assert config.initial_cash == 10000.0
        assert config.maker_fee_bps == 0
        assert config.taker_fee_bps == 0
        assert config.include_forward_filled is False

    def test_valid_config_with_listener_id(self):
        config = BacktestConfig(
            postgres_dsn="postgresql://localhost/test",
            start_time_ms=1000,
            end_time_ms=2000,
            listener_id="listener-1",
        )
        assert config.listener_id == "listener-1"
        assert config.asset_ids is None

    def test_valid_config_with_platform(self):
        config = BacktestConfig(
            postgres_dsn="postgresql://localhost/test",
            start_time_ms=1000,
            end_time_ms=2000,
            asset_ids=["t1"],
            platform="kalshi",
        )
        assert config.platform == "kalshi"

    def test_valid_config_with_custom_fees(self):
        config = BacktestConfig(
            postgres_dsn="postgresql://localhost/test",
            start_time_ms=1000,
            end_time_ms=2000,
            asset_ids=["t1"],
            maker_fee_bps=50,
            taker_fee_bps=150,
        )
        assert config.maker_fee_bps == 50
        assert config.taker_fee_bps == 150


# ======================================================================
# BacktestConfig validation
# ======================================================================


class TestBacktestConfigValidation:

    def test_start_must_be_before_end(self):
        with pytest.raises(ValidationError, match="start_time_ms.*must be less than"):
            BacktestConfig(
                postgres_dsn="postgresql://localhost/test",
                start_time_ms=2000,
                end_time_ms=1000,
                asset_ids=["t1"],
            )

    def test_start_equals_end_rejected(self):
        with pytest.raises(ValidationError, match="start_time_ms.*must be less than"):
            BacktestConfig(
                postgres_dsn="postgresql://localhost/test",
                start_time_ms=1000,
                end_time_ms=1000,
                asset_ids=["t1"],
            )

    def test_initial_cash_must_be_positive(self):
        with pytest.raises(ValidationError, match="initial_cash must be positive"):
            BacktestConfig(
                postgres_dsn="postgresql://localhost/test",
                start_time_ms=1000,
                end_time_ms=2000,
                asset_ids=["t1"],
                initial_cash=0,
            )

    def test_negative_initial_cash_rejected(self):
        with pytest.raises(ValidationError, match="initial_cash must be positive"):
            BacktestConfig(
                postgres_dsn="postgresql://localhost/test",
                start_time_ms=1000,
                end_time_ms=2000,
                asset_ids=["t1"],
                initial_cash=-100,
            )

    def test_must_specify_asset_ids_or_listener_id(self):
        with pytest.raises(ValidationError, match="Must specify either asset_ids or listener_id"):
            BacktestConfig(
                postgres_dsn="postgresql://localhost/test",
                start_time_ms=1000,
                end_time_ms=2000,
            )

    def test_invalid_platform_rejected(self):
        with pytest.raises(ValidationError, match="platform must be"):
            BacktestConfig(
                postgres_dsn="postgresql://localhost/test",
                start_time_ms=1000,
                end_time_ms=2000,
                asset_ids=["t1"],
                platform="unknown",
            )

    def test_negative_maker_fee_rejected(self):
        with pytest.raises(ValidationError, match="Fee rates cannot be negative"):
            BacktestConfig(
                postgres_dsn="postgresql://localhost/test",
                start_time_ms=1000,
                end_time_ms=2000,
                asset_ids=["t1"],
                maker_fee_bps=-10,
            )

    def test_negative_taker_fee_rejected(self):
        with pytest.raises(ValidationError, match="Fee rates cannot be negative"):
            BacktestConfig(
                postgres_dsn="postgresql://localhost/test",
                start_time_ms=1000,
                end_time_ms=2000,
                asset_ids=["t1"],
                taker_fee_bps=-10,
            )

    def test_max_events_must_be_positive(self):
        with pytest.raises(ValidationError, match="max_events_in_memory must be positive"):
            BacktestConfig(
                postgres_dsn="postgresql://localhost/test",
                start_time_ms=1000,
                end_time_ms=2000,
                asset_ids=["t1"],
                max_events_in_memory=0,
            )


# ======================================================================
# FeeSchedule
# ======================================================================


class TestFeeSchedule:

    def test_calculate_fee_maker(self, fee_schedule):
        # maker_fee_bps = 0, so fee = 0
        fee = fee_schedule.calculate_fee(
            quantity=Decimal("100"),
            price=Decimal("0.50"),
            is_maker=True,
        )
        assert fee == Decimal("0")

    def test_calculate_fee_taker(self, fee_schedule):
        # taker_fee_bps = 100, notional = 100 * 0.50 = 50
        # fee = 50 * 100 / 10000 = 0.50
        fee = fee_schedule.calculate_fee(
            quantity=Decimal("100"),
            price=Decimal("0.50"),
            is_maker=False,
        )
        assert fee == Decimal("0.50")

    def test_calculate_fee_with_both_nonzero(self):
        schedule = FeeSchedule(maker_fee_bps=50, taker_fee_bps=150)

        maker_fee = schedule.calculate_fee(
            quantity=Decimal("100"),
            price=Decimal("0.60"),
            is_maker=True,
        )
        # notional = 60, fee = 60 * 50 / 10000 = 0.30
        assert maker_fee == Decimal("0.30")

        taker_fee = schedule.calculate_fee(
            quantity=Decimal("100"),
            price=Decimal("0.60"),
            is_maker=False,
        )
        # fee = 60 * 150 / 10000 = 0.90
        assert taker_fee == Decimal("0.90")

    def test_calculate_fee_zero_quantity(self):
        schedule = FeeSchedule(maker_fee_bps=100, taker_fee_bps=100)
        fee = schedule.calculate_fee(
            quantity=Decimal("0"),
            price=Decimal("0.50"),
            is_maker=False,
        )
        assert fee == Decimal("0")


# ======================================================================
# FeeSchedule factory methods
# ======================================================================


class TestFeeScheduleFactory:

    def test_polymarket_factory(self):
        schedule = FeeSchedule.polymarket()
        assert schedule.maker_fee_bps == 0
        assert schedule.taker_fee_bps == 0

    def test_kalshi_factory(self):
        schedule = FeeSchedule.kalshi()
        assert schedule.maker_fee_bps == 50
        assert schedule.taker_fee_bps == 150


# ======================================================================
# BacktestResult.summary()
# ======================================================================


class TestBacktestResultSummary:

    def _make_result(self) -> BacktestResult:
        config = BacktestConfig(
            postgres_dsn="postgresql://localhost/test",
            start_time_ms=1000,
            end_time_ms=2000,
            asset_ids=["t1"],
            initial_cash=10000.0,
            maker_fee_bps=0,
            taker_fee_bps=100,
        )
        return BacktestResult(
            config=config,
            strategy_name="TestStrategy",
            total_return=0.15,
            sharpe_ratio=1.5,
            sortino_ratio=2.0,
            max_drawdown=0.05,
            win_rate=0.6,
            profit_factor=2.0,
            num_trades=100,
            num_winning_trades=60,
            num_losing_trades=40,
            avg_win=10.0,
            avg_loss=5.0,
            total_fees_paid=50.0,
            equity_curve=[(1000, 10000.0), (2000, 11500.0)],
            drawdown_curve=[(1000, 0.0), (2000, -0.02)],
            final_equity=11500.0,
        )

    def test_summary_returns_string(self):
        result = self._make_result()
        summary = result.summary()
        assert isinstance(summary, str)

    def test_summary_contains_strategy_name(self):
        result = self._make_result()
        summary = result.summary()
        assert "TestStrategy" in summary

    def test_summary_contains_total_return(self):
        result = self._make_result()
        summary = result.summary()
        assert "+15.00%" in summary

    def test_summary_contains_win_rate(self):
        result = self._make_result()
        summary = result.summary()
        assert "60.00%" in summary

    def test_summary_contains_sharpe_ratio(self):
        result = self._make_result()
        summary = result.summary()
        assert "1.500" in summary

    def test_summary_contains_num_trades(self):
        result = self._make_result()
        summary = result.summary()
        assert "100" in summary

    def test_summary_handles_none_sharpe(self):
        result = self._make_result()
        result.sharpe_ratio = None
        summary = result.summary()
        assert "N/A" in summary
