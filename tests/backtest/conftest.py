"""
Shared fixtures for backtest test suite.
"""

import sys
from decimal import Decimal
from pathlib import Path

import pytest

# Ensure src/ is on the Python path so that absolute imports like
# ``from models.orderbook import ...`` resolve correctly.
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from models.orderbook import OrderbookSnapshot, OrderLevel
from models.trade import Trade

from backtest.models.order import (
    Order,
    Fill,
    OrderSide,
    OrderType,
    OrderStatus,
    TimeInForce,
    FillReason,
)
from backtest.models.config import BacktestConfig, FeeSchedule
from backtest.models.portfolio import Portfolio
from backtest.models.market_pair import MarketPair, MarketPairRegistry


# ---------------------------------------------------------------------------
# OrderbookSnapshot / Trade fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_orderbook_snapshot() -> OrderbookSnapshot:
    """Return an OrderbookSnapshot with realistic bids/asks."""
    snapshot = OrderbookSnapshot(
        listener_id="listener-1",
        asset_id="token-yes-1",
        market="condition-1",
        timestamp=1700000000000,
        bids=[
            OrderLevel(price="0.55", size="100"),
            OrderLevel(price="0.54", size="200"),
            OrderLevel(price="0.53", size="300"),
        ],
        asks=[
            OrderLevel(price="0.56", size="150"),
            OrderLevel(price="0.57", size="250"),
            OrderLevel(price="0.58", size="350"),
        ],
    )
    snapshot.compute_metrics()
    return snapshot


@pytest.fixture
def sample_trade() -> Trade:
    """Return a Trade object."""
    return Trade(
        listener_id="listener-1",
        asset_id="token-yes-1",
        market="condition-1",
        timestamp=1700000000000,
        price=0.55,
        size=10.0,
        side="buy",
        raw_payload={"source": "test"},
    )


# ---------------------------------------------------------------------------
# Order / Fill fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_order_buy_limit() -> Order:
    """Return a BUY LIMIT Order."""
    return Order(
        asset_id="token-yes-1",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        price=Decimal("0.55"),
        quantity=Decimal("10"),
    )


@pytest.fixture
def sample_order_sell_market() -> Order:
    """Return a SELL MARKET Order."""
    return Order(
        asset_id="token-yes-1",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=Decimal("10"),
    )


@pytest.fixture
def sample_fill() -> Fill:
    """Return a Fill object."""
    return Fill(
        order_id="order_1",
        asset_id="token-yes-1",
        side=OrderSide.BUY,
        price=Decimal("0.55"),
        quantity=Decimal("10"),
        fees=Decimal("0"),
        timestamp_ms=1700000000000,
        is_maker=True,
        fill_reason=FillReason.IMMEDIATE,
    )


# ---------------------------------------------------------------------------
# Fee / Portfolio / Config fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fee_schedule() -> FeeSchedule:
    """Return a FeeSchedule with maker=0 bps, taker=100 bps."""
    return FeeSchedule(maker_fee_bps=0, taker_fee_bps=100)


@pytest.fixture
def portfolio() -> Portfolio:
    """Return a Portfolio with 10 000 initial cash."""
    return Portfolio(initial_cash=Decimal("10000"))


@pytest.fixture
def market_pair_registry() -> MarketPairRegistry:
    """Return a registry with one yes/no pair."""
    registry = MarketPairRegistry()
    pair = MarketPair(
        condition_id="condition-1",
        question="Will team X win?",
        yes_token_id="token-yes-1",
        no_token_id="token-no-1",
        platform="polymarket",
    )
    registry.register(pair)
    return registry


@pytest.fixture
def backtest_config() -> BacktestConfig:
    """Return a valid BacktestConfig."""
    return BacktestConfig(
        postgres_dsn="postgresql://localhost:5432/test",
        start_time_ms=1700000000000,
        end_time_ms=1700001000000,
        asset_ids=["token-yes-1", "token-no-1"],
        initial_cash=10000.0,
        maker_fee_bps=0,
        taker_fee_bps=100,
    )
