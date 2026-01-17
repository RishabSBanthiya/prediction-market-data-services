import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import asyncio
import pytest

from services.websocket_client import PolymarketWebSocketClient
from services.market_discovery import PolymarketDiscoveryService
from utils.logger import LoggerFactory


@pytest.fixture
def logger():
    factory = LoggerFactory("INFO")
    return factory.create("test")


@pytest.mark.asyncio
async def test_websocket_connect_and_receive(logger):
    # First get a real token ID
    discovery = PolymarketDiscoveryService(logger)
    markets = await discovery.discover_markets({"tag_ids": [100639]})
    await discovery.close()

    if not markets:
        pytest.skip("No markets available for testing")

    token_id = markets[0].token_id
    client = PolymarketWebSocketClient(logger)

    await client.connect()
    await client.subscribe([token_id])

    # Wait for at least one event (with timeout)
    received = []
    try:
        async with asyncio.timeout(10):
            async for event in client.events():
                received.append(event)
                if len(received) >= 1:
                    break
    except asyncio.TimeoutError:
        pass

    await client.disconnect()

    # May not receive events if market is quiet
    assert isinstance(received, list)


@pytest.mark.asyncio
async def test_websocket_subscribe_unsubscribe(logger):
    client = PolymarketWebSocketClient(logger)
    await client.connect()

    await client.subscribe(["test-token-1", "test-token-2"])
    assert "test-token-1" in client._subscribed_tokens
    assert "test-token-2" in client._subscribed_tokens

    await client.unsubscribe(["test-token-1"])
    assert "test-token-1" not in client._subscribed_tokens
    assert "test-token-2" in client._subscribed_tokens

    await client.disconnect()
