"""
Tests for ExecutionEngine.

Covers market order BUY/SELL fills (L2 walk), limit order immediate execution,
limit order resting and queue-fill, cancel_order, FOK rejection,
insufficient funds rejection, and process_trade queue advancement.
"""

from decimal import Decimal

import pytest

from models.orderbook import OrderbookSnapshot, OrderLevel
from models.trade import Trade

from backtest.models.order import (
    Order, OrderSide, OrderType, OrderStatus, TimeInForce, FillReason,
)
from backtest.models.config import FeeSchedule
from backtest.models.portfolio import Portfolio
from backtest.services.execution_engine import ExecutionEngine


# ======================================================================
# Helpers
# ======================================================================


def _make_snapshot(
    asset_id: str = "token-yes-1",
    bids=None,
    asks=None,
    timestamp: int = 1700000000000,
) -> OrderbookSnapshot:
    """Build an OrderbookSnapshot with computed metrics."""
    if bids is None:
        bids = [
            OrderLevel(price="0.55", size="100"),
            OrderLevel(price="0.54", size="200"),
        ]
    if asks is None:
        asks = [
            OrderLevel(price="0.56", size="150"),
            OrderLevel(price="0.57", size="250"),
        ]
    snap = OrderbookSnapshot(
        listener_id="listener-1",
        asset_id=asset_id,
        market="condition-1",
        timestamp=timestamp,
        bids=bids,
        asks=asks,
        raw_payload={},
    )
    snap.compute_metrics()
    return snap


def _make_trade(
    asset_id: str = "token-yes-1",
    price: float = 0.55,
    size: float = 10.0,
    side: str = "buy",
    timestamp: int = 1700000000000,
) -> Trade:
    return Trade(
        listener_id="listener-1",
        asset_id=asset_id,
        market="condition-1",
        timestamp=timestamp,
        price=price,
        size=size,
        side=side,
        raw_payload={},
    )


def _make_engine(
    initial_cash: Decimal = Decimal("10000"),
    maker_bps: int = 0,
    taker_bps: int = 0,
    fill_probability: float = 1.0,
) -> tuple[ExecutionEngine, Portfolio]:
    portfolio = Portfolio(initial_cash=initial_cash)
    fee_schedule = FeeSchedule(maker_fee_bps=maker_bps, taker_fee_bps=taker_bps)
    engine = ExecutionEngine(
        portfolio=portfolio,
        fee_schedule=fee_schedule,
        fill_probability=fill_probability,
    )
    return engine, portfolio


# ======================================================================
# Market order BUY fills at ask prices (L2 walk)
# ======================================================================


class TestMarketOrderBuy:

    def test_market_buy_fills_at_best_ask(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
        )
        order_id = engine.submit_order(order)

        assert order.status == OrderStatus.FILLED
        assert order.filled_quantity == Decimal("10")
        # All 10 from first ask level at 0.56
        assert order.avg_fill_price == Decimal("0.56")

    def test_market_buy_walks_multiple_levels(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot(
            asks=[
                OrderLevel(price="0.56", size="50"),
                OrderLevel(price="0.57", size="100"),
            ],
        )
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("80"),
        )
        engine.submit_order(order)

        assert order.status == OrderStatus.FILLED
        assert order.filled_quantity == Decimal("80")
        # 50 at 0.56, 30 at 0.57 => avg = (50*0.56 + 30*0.57) / 80
        expected_avg = (Decimal("50") * Decimal("0.56") + Decimal("30") * Decimal("0.57")) / Decimal("80")
        assert order.avg_fill_price == expected_avg

    def test_market_buy_partial_fill_when_insufficient_liquidity(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot(
            asks=[OrderLevel(price="0.56", size="5")],
        )
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
        )
        engine.submit_order(order)

        # Should fill what is available (5) unless FOK
        assert order.filled_quantity == Decimal("5")

    def test_market_buy_updates_cash(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
        )
        engine.submit_order(order)

        # Cost = 10 * 0.56 = 5.60 (no fees)
        assert portfolio.cash == Decimal("10000") - Decimal("5.60")


# ======================================================================
# Market order SELL fills at bid prices
# ======================================================================


class TestMarketOrderSell:

    def test_market_sell_fills_at_best_bid(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        # First buy some tokens
        buy_order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
        )
        engine.submit_order(buy_order)

        # Now sell
        sell_order = Order(
            asset_id="token-yes-1",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
        )
        engine.submit_order(sell_order)

        assert sell_order.status == OrderStatus.FILLED
        assert sell_order.filled_quantity == Decimal("10")
        # Sells walk bids: best bid is 0.55
        assert sell_order.avg_fill_price == Decimal("0.55")


# ======================================================================
# Limit order — immediately marketable
# ======================================================================


class TestLimitOrderImmediatelyMarketable:

    def test_buy_limit_at_ask_fills_immediately(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.56"),  # >= best ask
            quantity=Decimal("10"),
        )
        engine.submit_order(order)

        assert order.status == OrderStatus.FILLED
        assert order.filled_quantity == Decimal("10")

    def test_buy_limit_above_ask_fills_at_actual_ask_price(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.58"),  # well above best ask
            quantity=Decimal("10"),
        )
        engine.submit_order(order)

        assert order.status == OrderStatus.FILLED
        # Should fill at actual ask price (0.56), not limit price
        assert order.avg_fill_price == Decimal("0.56")


# ======================================================================
# Limit order — resting and queue fill
# ======================================================================


class TestLimitOrderResting:

    def test_buy_limit_below_ask_rests_in_queue(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.54"),  # below best ask (0.56)
            quantity=Decimal("10"),
        )
        engine.submit_order(order)

        assert order.status == OrderStatus.PENDING
        assert order.filled_quantity == Decimal("0")

    def test_resting_limit_fills_when_orderbook_crosses(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        # Place limit buy below ask
        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.54"),
            quantity=Decimal("10"),
        )
        engine.submit_order(order)
        assert order.status == OrderStatus.PENDING

        # Now orderbook changes — asks drop to 0.54
        new_snap = _make_snapshot(
            asks=[
                OrderLevel(price="0.54", size="100"),
                OrderLevel(price="0.55", size="200"),
            ],
            timestamp=1700000001000,
        )
        fills = engine.process_orderbook_update(new_snap)

        assert len(fills) == 1
        assert order.status == OrderStatus.FILLED
        assert order.avg_fill_price == Decimal("0.54")

    def test_resting_limit_fills_via_trade_queue_advancement(self):
        engine, portfolio = _make_engine()
        # Orderbook with 100 shares at bid 0.55
        snap = _make_snapshot(
            bids=[OrderLevel(price="0.55", size="100")],
            asks=[OrderLevel(price="0.56", size="150")],
        )
        engine.process_orderbook_update(snap)

        # Place a limit buy at 0.54 — not marketable, rests in queue
        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.54"),
            quantity=Decimal("10"),
        )
        engine.submit_order(order)
        assert order.status == OrderStatus.PENDING

        # bids at 0.54 had size 200 in the original snapshot => size_ahead = 200
        # Process trades that advance the queue: 200+ shares trade at 0.54
        for i in range(21):
            trade = _make_trade(price=0.54, size=10.0, timestamp=1700000001000 + i * 100)
            fills = engine.process_trade(trade)
            if fills:
                break

        # After 210 volume traded at 0.54 (> size_ahead 200), order should fill
        assert order.status == OrderStatus.FILLED


# ======================================================================
# cancel_order()
# ======================================================================


class TestCancelOrder:

    def test_cancel_pending_order(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.50"),
            quantity=Decimal("10"),
        )
        order_id = engine.submit_order(order)
        assert engine.cancel_order(order_id) is True
        assert order.status == OrderStatus.CANCELLED

    def test_cancel_already_filled_returns_false(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
        )
        order_id = engine.submit_order(order)
        assert order.status == OrderStatus.FILLED
        assert engine.cancel_order(order_id) is False

    def test_cancel_nonexistent_order_returns_false(self):
        engine, _ = _make_engine()
        assert engine.cancel_order("nonexistent") is False


# ======================================================================
# FOK rejection
# ======================================================================


class TestFOKRejection:

    def test_fok_rejected_when_insufficient_liquidity(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot(
            asks=[OrderLevel(price="0.56", size="5")],
        )
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            time_in_force=TimeInForce.FOK,
        )
        engine.submit_order(order)

        # Only 5 available, FOK requires full 10
        assert order.status == OrderStatus.REJECTED
        assert order.filled_quantity == Decimal("0")

    def test_fok_fills_when_sufficient_liquidity(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot(
            asks=[OrderLevel(price="0.56", size="100")],
        )
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            time_in_force=TimeInForce.FOK,
        )
        engine.submit_order(order)

        assert order.status == OrderStatus.FILLED
        assert order.filled_quantity == Decimal("10")


# ======================================================================
# Insufficient funds rejection
# ======================================================================


class TestInsufficientFundsRejection:

    def test_buy_rejected_when_insufficient_funds(self):
        engine, portfolio = _make_engine(initial_cash=Decimal("1"))
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("100"),
        )
        engine.submit_order(order)

        # 100 * 1.0 (worst case market) = 100 > 1 available
        assert order.status == OrderStatus.REJECTED

    def test_limit_buy_rejected_when_insufficient_funds(self):
        engine, portfolio = _make_engine(initial_cash=Decimal("5"))
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.55"),
            quantity=Decimal("100"),
        )
        engine.submit_order(order)

        # 100 * 0.55 = 55 > 5 available
        assert order.status == OrderStatus.REJECTED


# ======================================================================
# Sell rejected when insufficient position
# ======================================================================


class TestInsufficientPositionRejection:

    def test_sell_rejected_when_no_position(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
        )
        engine.submit_order(order)

        assert order.status == OrderStatus.REJECTED


# ======================================================================
# process_trade() advances queue positions
# ======================================================================


class TestProcessTrade:

    def test_process_trade_returns_fills_for_queue_orders(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot(
            bids=[OrderLevel(price="0.55", size="10")],
            asks=[OrderLevel(price="0.56", size="150")],
        )
        engine.process_orderbook_update(snap)

        # Place a limit buy at 0.55 (at the bid = not marketable since ask is 0.56)
        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.55"),
            quantity=Decimal("5"),
        )
        engine.submit_order(order)
        assert order.status == OrderStatus.PENDING

        # Process enough trades at 0.55 to advance past size_ahead (10)
        trade = _make_trade(price=0.55, size=15.0, timestamp=1700000001000)
        fills = engine.process_trade(trade)

        assert len(fills) == 1
        assert fills[0].price == Decimal("0.55")
        assert fills[0].is_maker is True
        assert fills[0].fill_reason == FillReason.QUEUE_REACHED

    def test_process_trade_no_fills_for_unrelated_asset(self):
        engine, portfolio = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.50"),
            quantity=Decimal("10"),
        )
        engine.submit_order(order)

        # Trade on a different asset
        trade = _make_trade(asset_id="token-other", price=0.50, size=1000.0)
        fills = engine.process_trade(trade)
        assert len(fills) == 0


# ======================================================================
# Fee integration
# ======================================================================


class TestExecutionEngineFees:

    def test_market_buy_taker_fee_applied(self):
        engine, portfolio = _make_engine(taker_bps=100)  # 1%
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("100"),
        )
        engine.submit_order(order)

        # Notional = 100 * 0.56 = 56, fee = 56 * 100/10000 = 0.56
        fills = portfolio.get_fills()
        assert len(fills) == 1
        assert fills[0].fees == Decimal("0.56")
        # Cash should be: 10000 - 56 - 0.56 = 9943.44
        assert portfolio.cash == Decimal("9943.44")


# ======================================================================
# get_open_orders and get_order_status
# ======================================================================


class TestOpenOrdersAndStatus:

    def test_get_open_orders_returns_pending(self):
        engine, _ = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.50"),
            quantity=Decimal("10"),
        )
        engine.submit_order(order)

        open_orders = engine.get_open_orders()
        assert len(open_orders) == 1
        assert open_orders[0].order_id == order.order_id

    def test_get_open_orders_filtered_by_asset(self):
        engine, _ = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order1 = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.50"),
            quantity=Decimal("10"),
        )
        engine.submit_order(order1)

        assert len(engine.get_open_orders(asset_id="token-yes-1")) == 1
        assert len(engine.get_open_orders(asset_id="other")) == 0

    def test_get_order_status(self):
        engine, _ = _make_engine()
        snap = _make_snapshot()
        engine.process_orderbook_update(snap)

        order = Order(
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.50"),
            quantity=Decimal("10"),
        )
        order_id = engine.submit_order(order)
        assert engine.get_order_status(order_id) == OrderStatus.PENDING

    def test_get_order_status_unknown_returns_rejected(self):
        engine, _ = _make_engine()
        assert engine.get_order_status("unknown") == OrderStatus.REJECTED
