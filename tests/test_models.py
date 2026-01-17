import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from models import (
    ListenerConfig, ListenerFilters, Market, MarketState,
    OrderbookSnapshot, OrderLevel, Trade
)


def test_listener_filters_defaults():
    filters = ListenerFilters()
    assert filters.series_ids == []
    assert filters.tag_ids == []
    assert filters.min_liquidity is None


def test_listener_config():
    config = ListenerConfig(
        id="test-id",
        name="test-listener",
        filters=ListenerFilters(tag_ids=[100639]),
        discovery_interval_seconds=60,
    )
    assert config.name == "test-listener"
    assert config.filters.tag_ids == [100639]


def test_market():
    market = Market(
        condition_id="0x123",
        token_id="token123",
        question="Test market?",
    )
    assert market.state == MarketState.DISCOVERED
    assert market.is_active is True


def test_orderbook_compute_metrics():
    snapshot = OrderbookSnapshot(
        listener_id="listener1",
        asset_id="asset1",
        market="market1",
        timestamp=1234567890,
        bids=[OrderLevel(price="0.55", size="100"), OrderLevel(price="0.54", size="200")],
        asks=[OrderLevel(price="0.56", size="150"), OrderLevel(price="0.57", size="250")],
        raw_payload={},
    )
    snapshot.compute_metrics()
    assert snapshot.best_bid == 0.55
    assert snapshot.best_ask == 0.56
    assert abs(snapshot.spread - 0.01) < 0.0001
    assert snapshot.bid_depth == 300
    assert snapshot.ask_depth == 400


def test_trade():
    trade = Trade(
        listener_id="listener1",
        asset_id="asset1",
        market="market1",
        timestamp=1234567890,
        price=0.55,
        size=100,
        side="BUY",
        raw_payload={},
    )
    assert trade.price == 0.55
    assert trade.side == "BUY"
