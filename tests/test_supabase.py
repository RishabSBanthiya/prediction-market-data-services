import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from supabase import create_client

from config import Config
from models import OrderbookSnapshot, OrderLevel, Trade, Market, MarketState
from services.supabase_writer import SupabaseWriter
from utils.logger import LoggerFactory


@pytest.fixture
def supabase_client():
    config = Config()
    return create_client(config.supabase_url, config.supabase_key)


@pytest.fixture
def logger():
    factory = LoggerFactory("INFO")
    return factory.create("test")


@pytest.fixture
def test_listener_id(supabase_client):
    result = supabase_client.table("listeners").insert({
        "name": "test-listener-pytest",
        "filters": {"tag_ids": [100639]},
        "discovery_interval_seconds": 60,
    }).execute()
    listener_id = result.data[0]["id"]
    yield listener_id
    supabase_client.table("listeners").delete().eq("id", listener_id).execute()


@pytest.mark.asyncio
async def test_write_and_read_orderbook(supabase_client, logger, test_listener_id):
    writer = SupabaseWriter(supabase_client, test_listener_id, logger)

    snapshot = OrderbookSnapshot(
        listener_id=test_listener_id,
        asset_id="test-asset",
        market="test-market",
        timestamp=1234567890000,
        bids=[OrderLevel(price="0.55", size="100")],
        asks=[OrderLevel(price="0.56", size="100")],
        raw_payload={"test": True},
    )
    snapshot.compute_metrics()

    await writer.write_orderbook(snapshot)
    await writer.flush()

    result = supabase_client.table("orderbook_snapshots").select("*").eq(
        "listener_id", test_listener_id
    ).execute()

    assert len(result.data) >= 1
    row = result.data[0]
    assert row["asset_id"] == "test-asset"
    assert row["best_bid"] == 0.55

    supabase_client.table("orderbook_snapshots").delete().eq(
        "listener_id", test_listener_id
    ).execute()


@pytest.mark.asyncio
async def test_write_market(supabase_client, logger, test_listener_id):
    writer = SupabaseWriter(supabase_client, test_listener_id, logger)

    market = Market(
        condition_id="test-condition-123",
        token_id="test-token-123",
        question="Test market?",
        state=MarketState.TRACKING,
    )

    await writer.write_market(market)

    result = supabase_client.table("markets").select("*").eq(
        "condition_id", "test-condition-123"
    ).execute()

    assert len(result.data) == 1
    assert result.data[0]["question"] == "Test market?"

    supabase_client.table("markets").delete().eq(
        "condition_id", "test-condition-123"
    ).execute()
