import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

from core.listener import Listener, ListenerState
from core.events import OrderbookEvent, TradeEvent, MarketDiscoveredEvent, MarketClosedEvent
from models import ListenerConfig, ListenerFilters, Market, MarketState, OrderbookSnapshot, OrderLevel, Trade


@pytest.fixture
def listener_config():
    return ListenerConfig(
        id="test-listener-1",
        name="test-listener",
        filters=ListenerFilters(tag_ids=[100639]),
        discovery_interval_seconds=60,
        is_active=True,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def mock_discovery():
    discovery = AsyncMock()
    discovery.discover_markets = AsyncMock(return_value=[])
    discovery.get_market_details = AsyncMock(return_value=None)
    discovery.close = AsyncMock()
    return discovery


@pytest.fixture
def mock_websocket():
    websocket = AsyncMock()
    websocket.connect = AsyncMock()
    websocket.disconnect = AsyncMock()
    websocket.subscribe = AsyncMock()
    websocket.unsubscribe = AsyncMock()

    async def empty_events():
        while True:
            await asyncio.sleep(0.1)
            return
            yield {}

    websocket.events = empty_events
    return websocket


@pytest.fixture
def mock_writer():
    writer = AsyncMock()
    writer.start = AsyncMock()
    writer.stop = AsyncMock()
    writer.write_orderbook = AsyncMock()
    writer.write_trade = AsyncMock()
    writer.write_market = AsyncMock()
    writer.write_state_transition = AsyncMock()
    writer.flush = AsyncMock()
    return writer


@pytest.fixture
def mock_logger():
    logger = MagicMock()
    logger.info = MagicMock()
    logger.error = MagicMock()
    logger.warning = MagicMock()
    return logger


@pytest.fixture
def listener(listener_config, mock_discovery, mock_websocket, mock_writer, mock_logger):
    return Listener(
        config=listener_config,
        discovery=mock_discovery,
        websocket=mock_websocket,
        writer=mock_writer,
        logger=mock_logger,
    )


def test_listener_state_initialization():
    state = ListenerState()
    assert state.is_running is False
    assert state.subscribed_markets == {}
    assert state.last_discovery_at is None
    assert state.events_processed == 0
    assert state.errors_count == 0


def test_listener_config_access(listener, listener_config):
    assert listener.config == listener_config
    assert listener.listener_id == "test-listener-1"


def test_listener_state_access(listener):
    assert listener.state.is_running is False


@pytest.mark.asyncio
async def test_listener_start_sets_running(listener):
    await listener.start()
    assert listener.state.is_running is True
    await listener.stop()


@pytest.mark.asyncio
async def test_listener_stop_clears_running(listener):
    await listener.start()
    await listener.stop()
    assert listener.state.is_running is False


@pytest.mark.asyncio
async def test_handle_market_discovered(listener, mock_writer, mock_websocket):
    market = Market(
        condition_id="cond-123",
        token_id="token-456",
        question="Test market?",
    )
    market.listener_id = listener.listener_id

    await listener._handle_market_discovered(market)

    assert market.state == MarketState.TRACKING
    mock_writer.write_market.assert_called_once()
    mock_writer.write_state_transition.assert_called_once()
    mock_websocket.subscribe.assert_called_once_with(["token-456"])
    assert "token-456" in listener.state.subscribed_markets


@pytest.mark.asyncio
async def test_handle_market_closed(listener, mock_writer, mock_websocket):
    market = Market(
        condition_id="cond-123",
        token_id="token-456",
        question="Test market?",
        state=MarketState.TRACKING,
    )
    listener.state.subscribed_markets["token-456"] = market

    await listener._handle_market_closed(market, MarketState.CLOSED.value)

    mock_writer.write_state_transition.assert_called_once()
    mock_websocket.unsubscribe.assert_called_once_with(["token-456"])
    assert "token-456" not in listener.state.subscribed_markets


def test_parse_websocket_event_book(listener):
    raw = {
        "event_type": "book",
        "asset_id": "asset-123",
        "market": "market-456",
        "timestamp": 1705420800000,
        "bids": [{"price": "0.55", "size": "100"}],
        "asks": [{"price": "0.56", "size": "200"}],
        "hash": "abc123",
    }

    event = listener._parse_websocket_event(raw)

    assert isinstance(event, OrderbookEvent)
    assert event.data.asset_id == "asset-123"
    assert event.data.market == "market-456"
    assert event.data.best_bid == 0.55
    assert event.data.best_ask == 0.56


def test_parse_websocket_event_trade(listener):
    raw = {
        "event_type": "last_trade_price",
        "asset_id": "asset-123",
        "market": "market-456",
        "timestamp": 1705420800000,
        "price": "0.55",
        "size": "100",
        "side": "BUY",
    }

    event = listener._parse_websocket_event(raw)

    assert isinstance(event, TradeEvent)
    assert event.data.asset_id == "asset-123"
    assert event.data.price == 0.55
    assert event.data.side == "BUY"


def test_parse_websocket_event_unknown(listener):
    raw = {"event_type": "unknown_type"}
    event = listener._parse_websocket_event(raw)
    assert event is None


@pytest.mark.asyncio
async def test_handle_orderbook_event(listener, mock_writer):
    # Must add market to subscribed_markets first (listener validates this)
    market = Market(
        condition_id="market-456",
        token_id="asset-123",
        question="Test market?",
    )
    listener.state.subscribed_markets["asset-123"] = market

    snapshot = OrderbookSnapshot(
        listener_id="test",
        asset_id="asset-123",
        market="market-456",
        timestamp=1705420800000,
        bids=[OrderLevel(price="0.55", size="100")],
        asks=[OrderLevel(price="0.56", size="200")],
        raw_payload={},
    )
    event = OrderbookEvent(data=snapshot)

    await listener._handle_event(event)

    mock_writer.write_orderbook.assert_called_once_with(snapshot)


@pytest.mark.asyncio
async def test_handle_trade_event(listener, mock_writer):
    # Must add market to subscribed_markets first (listener validates this)
    market = Market(
        condition_id="market-456",
        token_id="asset-123",
        question="Test market?",
    )
    listener.state.subscribed_markets["asset-123"] = market

    trade = Trade(
        listener_id="test",
        asset_id="asset-123",
        market="market-456",
        timestamp=1705420800000,
        price=0.55,
        size=100.0,
        side="BUY",
        raw_payload={},
    )
    event = TradeEvent(data=trade)

    await listener._handle_event(event)

    mock_writer.write_trade.assert_called_once_with(trade)


@pytest.mark.asyncio
async def test_discover_and_sync_new_markets(listener, mock_discovery, mock_writer, mock_websocket):
    market = Market(
        condition_id="cond-123",
        token_id="token-456",
        question="Test market?",
    )
    mock_discovery.discover_markets.return_value = [market]

    await listener._discover_and_sync_markets()

    # New behavior: markets are handled inline (write to DB, subscribe) instead of via events
    mock_writer.write_market.assert_called_once()
    mock_writer.write_state_transition.assert_called_once()
    mock_websocket.subscribe.assert_called_once_with(["token-456"])
    assert "token-456" in listener.state.subscribed_markets


@pytest.mark.asyncio
async def test_discover_and_sync_removed_markets(listener, mock_discovery):
    market = Market(
        condition_id="cond-123",
        token_id="token-456",
        question="Test market?",
    )
    listener.state.subscribed_markets["token-456"] = market
    mock_discovery.discover_markets.return_value = []

    await listener._discover_and_sync_markets()

    event = await asyncio.wait_for(listener._event_queue.get(), timeout=1.0)
    assert isinstance(event, MarketClosedEvent)
    assert event.market.token_id == "token-456"
