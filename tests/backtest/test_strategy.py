"""
Tests for Strategy abstract base class and BacktestContext.

Covers abstract instantiation, dependency injection,
submit_order/cancel_order/get_open_orders before and after injection.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from models.orderbook import OrderbookSnapshot

from backtest.core.strategy import Strategy, BacktestContext
from backtest.models.order import Order, OrderSide, OrderType
from backtest.models.portfolio import Portfolio


# ======================================================================
# Concrete mock strategy for testing
# ======================================================================


class MockStrategy(Strategy):
    """Minimal concrete strategy for testing."""

    def __init__(self):
        super().__init__(name="MockStrategy")
        self.orderbook_count = 0

    def on_orderbook(self, snapshot: OrderbookSnapshot, is_forward_filled: bool) -> None:
        self.orderbook_count += 1


# ======================================================================
# Strategy is abstract
# ======================================================================


class TestStrategyAbstract:

    def test_cannot_instantiate_strategy_directly(self):
        with pytest.raises(TypeError):
            Strategy(name="Abstract")

    def test_mock_strategy_can_be_instantiated(self):
        s = MockStrategy()
        assert s.name == "MockStrategy"


# ======================================================================
# submit_order / cancel_order / get_open_orders before injection
# ======================================================================


class TestStrategyBeforeInjection:

    def test_submit_order_raises_before_injection(self):
        s = MockStrategy()
        order = Order(
            asset_id="token-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
        )
        with pytest.raises(RuntimeError, match="Execution engine not available"):
            s.submit_order(order)

    def test_cancel_order_raises_before_injection(self):
        s = MockStrategy()
        with pytest.raises(RuntimeError, match="Execution engine not available"):
            s.cancel_order("order-1")

    def test_get_open_orders_raises_before_injection(self):
        s = MockStrategy()
        with pytest.raises(RuntimeError, match="Execution engine not available"):
            s.get_open_orders()

    def test_portfolio_raises_before_injection(self):
        s = MockStrategy()
        with pytest.raises(RuntimeError, match="Portfolio not available"):
            _ = s.portfolio


# ======================================================================
# _inject_dependencies() enables all methods
# ======================================================================


class TestStrategyAfterInjection:

    @pytest.fixture
    def injected_strategy(self):
        s = MockStrategy()
        mock_portfolio = MagicMock(spec=Portfolio)
        mock_engine = MagicMock()
        mock_engine.submit_order.return_value = "order-42"
        mock_engine.cancel_order.return_value = True
        mock_engine.get_open_orders.return_value = []
        s._inject_dependencies(mock_portfolio, mock_engine)
        return s, mock_portfolio, mock_engine

    def test_portfolio_accessible_after_injection(self, injected_strategy):
        s, mock_portfolio, _ = injected_strategy
        assert s.portfolio is mock_portfolio

    def test_submit_order_delegates_to_engine(self, injected_strategy):
        s, _, mock_engine = injected_strategy
        order = Order(
            asset_id="token-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
        )
        result = s.submit_order(order)
        assert result == "order-42"
        mock_engine.submit_order.assert_called_once_with(order)

    def test_cancel_order_delegates_to_engine(self, injected_strategy):
        s, _, mock_engine = injected_strategy
        result = s.cancel_order("order-1")
        assert result is True
        mock_engine.cancel_order.assert_called_once_with("order-1")

    def test_get_open_orders_delegates_to_engine(self, injected_strategy):
        s, _, mock_engine = injected_strategy
        result = s.get_open_orders(asset_id="token-1")
        assert result == []
        mock_engine.get_open_orders.assert_called_once_with("token-1")


# ======================================================================
# Optional lifecycle hooks
# ======================================================================


class TestStrategyLifecycleHooks:
    """on_start, on_end, on_trade, on_fill, on_market_close are optional."""

    def test_on_start_does_not_raise(self):
        s = MockStrategy()
        ctx = BacktestContext(
            start_time_ms=1000,
            end_time_ms=2000,
            initial_cash=10000.0,
            platform=None,
            markets={},
        )
        s.on_start(ctx)  # Should not raise

    def test_on_end_does_not_raise(self):
        s = MockStrategy()
        ctx = BacktestContext(
            start_time_ms=1000,
            end_time_ms=2000,
            initial_cash=10000.0,
            platform=None,
            markets={},
        )
        s.on_end(ctx)  # Should not raise

    def test_on_trade_does_not_raise(self):
        from models.trade import Trade
        s = MockStrategy()
        trade = Trade(
            listener_id="l1", asset_id="t1", market="m1",
            timestamp=1000, price=0.5, size=10, side="buy",
            raw_payload={},
        )
        s.on_trade(trade)  # Should not raise

    def test_on_fill_does_not_raise(self):
        from backtest.models.order import Fill, FillReason
        s = MockStrategy()
        fill = Fill(
            order_id="o1", asset_id="t1", side=OrderSide.BUY,
            price=Decimal("0.5"), quantity=Decimal("10"),
            timestamp_ms=1000, is_maker=True, fill_reason=FillReason.IMMEDIATE,
        )
        s.on_fill(fill)  # Should not raise


# ======================================================================
# BacktestContext
# ======================================================================


class TestBacktestContext:

    def test_backtest_context_creation(self):
        ctx = BacktestContext(
            start_time_ms=1000,
            end_time_ms=2000,
            initial_cash=10000.0,
            platform="polymarket",
            markets={"token-1": MagicMock()},
        )
        assert ctx.start_time_ms == 1000
        assert ctx.end_time_ms == 2000
        assert ctx.initial_cash == 10000.0
        assert ctx.platform == "polymarket"
        assert "token-1" in ctx.markets
