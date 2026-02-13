"""
Tests for QueueSimulator.

Covers add_order size_ahead estimation, process_trade queue advancement,
fill generation when queue position reached, remove_order,
and fill_probability < 1.0 behavior.
"""

import random
from decimal import Decimal

import pytest

from models.orderbook import OrderbookSnapshot, OrderLevel
from models.trade import Trade

from backtest.models.order import Order, OrderSide, OrderType
from backtest.services.queue_simulator import QueueSimulator


# ======================================================================
# Helpers
# ======================================================================


def _make_snapshot(
    asset_id: str = "token-1",
    bids=None,
    asks=None,
) -> OrderbookSnapshot:
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
        listener_id="l1",
        asset_id=asset_id,
        market="cond-1",
        timestamp=1700000000000,
        bids=bids,
        asks=asks,
        raw_payload={},
    )
    snap.compute_metrics()
    return snap


def _make_order(
    order_id: str,
    asset_id: str = "token-1",
    side: OrderSide = OrderSide.BUY,
    price: str = "0.55",
    quantity: str = "10",
) -> Order:
    order = Order(
        asset_id=asset_id,
        side=side,
        order_type=OrderType.LIMIT,
        price=Decimal(price),
        quantity=Decimal(quantity),
    )
    order.order_id = order_id
    return order


def _make_trade(
    asset_id: str = "token-1",
    price: float = 0.55,
    size: float = 10.0,
    timestamp: int = 1700000001000,
) -> Trade:
    return Trade(
        listener_id="l1",
        asset_id=asset_id,
        market="cond-1",
        timestamp=timestamp,
        price=price,
        size=size,
        side="buy",
        raw_payload={},
    )


# ======================================================================
# add_order estimates size_ahead
# ======================================================================


class TestAddOrder:

    def test_buy_order_size_ahead_from_bids_at_price(self):
        """Buy at 0.55: size ahead = sum of bids at >= 0.55 = 100."""
        sim = QueueSimulator()
        snap = _make_snapshot(
            bids=[
                OrderLevel(price="0.55", size="100"),
                OrderLevel(price="0.54", size="200"),
            ],
        )
        order = _make_order("o1", price="0.55")
        sim.add_order(order, snap)

        entry = sim.get_queue_position("o1")
        assert entry is not None
        assert entry.size_ahead == Decimal("100")

    def test_buy_order_size_ahead_includes_better_prices(self):
        """Buy at 0.54: bids at 0.55 (100) + 0.54 (200) = 300."""
        sim = QueueSimulator()
        snap = _make_snapshot(
            bids=[
                OrderLevel(price="0.55", size="100"),
                OrderLevel(price="0.54", size="200"),
            ],
        )
        order = _make_order("o1", price="0.54")
        sim.add_order(order, snap)

        entry = sim.get_queue_position("o1")
        assert entry.size_ahead == Decimal("300")

    def test_sell_order_size_ahead_from_asks_at_price(self):
        """Sell at 0.56: size ahead = sum of asks at <= 0.56 = 150."""
        sim = QueueSimulator()
        snap = _make_snapshot(
            asks=[
                OrderLevel(price="0.56", size="150"),
                OrderLevel(price="0.57", size="250"),
            ],
        )
        order = _make_order("o1", side=OrderSide.SELL, price="0.56")
        sim.add_order(order, snap)

        entry = sim.get_queue_position("o1")
        assert entry.size_ahead == Decimal("150")

    def test_sell_order_size_ahead_includes_better_prices(self):
        """Sell at 0.57: asks at 0.56 (150) + 0.57 (250) = 400."""
        sim = QueueSimulator()
        snap = _make_snapshot(
            asks=[
                OrderLevel(price="0.56", size="150"),
                OrderLevel(price="0.57", size="250"),
            ],
        )
        order = _make_order("o1", side=OrderSide.SELL, price="0.57")
        sim.add_order(order, snap)

        entry = sim.get_queue_position("o1")
        assert entry.size_ahead == Decimal("400")

    def test_duplicate_add_does_not_overwrite(self):
        sim = QueueSimulator()
        snap = _make_snapshot()
        order = _make_order("o1")
        sim.add_order(order, snap)

        # Add again — should be ignored
        sim.add_order(order, snap)
        assert len(sim.get_all_entries()) == 1


# ======================================================================
# process_trade advances cumulative volume
# ======================================================================


class TestProcessTradeAdvancement:

    def test_trade_at_order_price_advances_volume(self):
        sim = QueueSimulator()
        snap = _make_snapshot(
            bids=[OrderLevel(price="0.55", size="50")],
        )
        order = _make_order("o1", price="0.55")
        sim.add_order(order, snap)

        trade = _make_trade(price=0.55, size=20.0)
        sim.process_trade(trade)

        entry = sim.get_queue_position("o1")
        assert entry.cumulative_volume_at_price == Decimal("20")

    def test_trade_at_different_price_does_not_advance(self):
        sim = QueueSimulator()
        snap = _make_snapshot(
            bids=[OrderLevel(price="0.55", size="50")],
        )
        order = _make_order("o1", price="0.55")
        sim.add_order(order, snap)

        # Trade at 0.60 should NOT advance a buy order at 0.55
        # (for buy orders, trade price must be <= order price)
        trade = _make_trade(price=0.60, size=1000.0)
        sim.process_trade(trade)

        entry = sim.get_queue_position("o1")
        assert entry.cumulative_volume_at_price == Decimal("0")

    def test_trade_on_different_asset_does_not_advance(self):
        sim = QueueSimulator()
        snap = _make_snapshot(asset_id="token-1")
        order = _make_order("o1", asset_id="token-1", price="0.55")
        sim.add_order(order, snap)

        trade = _make_trade(asset_id="token-2", price=0.55, size=1000.0)
        sim.process_trade(trade)

        entry = sim.get_queue_position("o1")
        assert entry.cumulative_volume_at_price == Decimal("0")


# ======================================================================
# process_trade returns fill when queue position reached
# ======================================================================


class TestProcessTradeFillGeneration:

    def test_fill_returned_when_volume_exceeds_size_ahead(self):
        sim = QueueSimulator()
        snap = _make_snapshot(
            bids=[OrderLevel(price="0.55", size="20")],
        )
        order = _make_order("o1", price="0.55")
        sim.add_order(order, snap)

        # size_ahead = 20, one trade of 25 exceeds it
        trade = _make_trade(price=0.55, size=25.0)
        filled = sim.process_trade(trade)

        assert "o1" in filled

    def test_no_fill_before_size_ahead_reached(self):
        sim = QueueSimulator()
        snap = _make_snapshot(
            bids=[OrderLevel(price="0.55", size="100")],
        )
        order = _make_order("o1", price="0.55")
        sim.add_order(order, snap)

        trade = _make_trade(price=0.55, size=50.0)
        filled = sim.process_trade(trade)
        assert len(filled) == 0

    def test_cumulative_trades_eventually_fill(self):
        sim = QueueSimulator()
        snap = _make_snapshot(
            bids=[OrderLevel(price="0.55", size="30")],
        )
        order = _make_order("o1", price="0.55")
        sim.add_order(order, snap)

        # First trade: 20, not enough
        trade1 = _make_trade(price=0.55, size=20.0, timestamp=1700000001000)
        assert len(sim.process_trade(trade1)) == 0

        # Second trade: 15, cumulative = 35 >= 30
        trade2 = _make_trade(price=0.55, size=15.0, timestamp=1700000002000)
        filled = sim.process_trade(trade2)
        assert "o1" in filled

    def test_sell_order_fills_when_trade_at_or_above_price(self):
        sim = QueueSimulator()
        snap = _make_snapshot(
            asks=[OrderLevel(price="0.56", size="10")],
        )
        order = _make_order("o1", side=OrderSide.SELL, price="0.56")
        sim.add_order(order, snap)

        # Trade at 0.56 or above advances sell orders
        trade = _make_trade(price=0.56, size=15.0)
        filled = sim.process_trade(trade)
        assert "o1" in filled


# ======================================================================
# remove_order
# ======================================================================


class TestRemoveOrder:

    def test_remove_existing_order(self):
        sim = QueueSimulator()
        snap = _make_snapshot()
        order = _make_order("o1")
        sim.add_order(order, snap)

        sim.remove_order("o1")
        assert sim.get_queue_position("o1") is None

    def test_remove_nonexistent_order_does_not_raise(self):
        sim = QueueSimulator()
        sim.remove_order("nonexistent")  # Should not raise


# ======================================================================
# fill_probability < 1.0
# ======================================================================


class TestFillProbability:

    def test_fill_probability_validation_rejects_out_of_range(self):
        with pytest.raises(ValueError, match="fill_probability must be in"):
            QueueSimulator(fill_probability=1.5)

        with pytest.raises(ValueError, match="fill_probability must be in"):
            QueueSimulator(fill_probability=-0.1)

    def test_fill_probability_zero_never_fills(self):
        """With fill_probability=0.0, orders should never fill even when queue reached."""
        random.seed(42)
        sim = QueueSimulator(fill_probability=0.0)
        snap = _make_snapshot(
            bids=[OrderLevel(price="0.55", size="5")],
        )
        order = _make_order("o1", price="0.55")
        sim.add_order(order, snap)

        trade = _make_trade(price=0.55, size=100.0)
        filled = sim.process_trade(trade)
        assert len(filled) == 0

    def test_fill_probability_one_always_fills(self):
        """With fill_probability=1.0, orders always fill when queue reached."""
        sim = QueueSimulator(fill_probability=1.0)
        snap = _make_snapshot(
            bids=[OrderLevel(price="0.55", size="5")],
        )
        order = _make_order("o1", price="0.55")
        sim.add_order(order, snap)

        trade = _make_trade(price=0.55, size=10.0)
        filled = sim.process_trade(trade)
        assert "o1" in filled

    def test_fill_probability_partial_is_probabilistic(self):
        """With fill_probability=0.5, approximately half should fill over many trials."""
        random.seed(12345)
        fill_count = 0
        trials = 200

        for i in range(trials):
            sim = QueueSimulator(fill_probability=0.5)
            snap = _make_snapshot(
                bids=[OrderLevel(price="0.55", size="5")],
            )
            order = _make_order(f"o{i}", price="0.55")
            sim.add_order(order, snap)

            trade = _make_trade(price=0.55, size=10.0, timestamp=1700000001000 + i)
            filled = sim.process_trade(trade)
            if filled:
                fill_count += 1

        # Expect roughly 100 fills out of 200 with some variance
        assert 60 < fill_count < 140, f"Expected ~100 fills, got {fill_count}"


# ======================================================================
# get_all_entries
# ======================================================================


class TestGetAllEntries:

    def test_get_all_entries_returns_copy(self):
        sim = QueueSimulator()
        snap = _make_snapshot()
        order = _make_order("o1")
        sim.add_order(order, snap)

        entries = sim.get_all_entries()
        assert "o1" in entries
        # Modifying the copy should not affect internal state
        entries.pop("o1")
        assert sim.get_queue_position("o1") is not None
