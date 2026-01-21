"""
Focused test: subscribe to a few specific markets and verify orderbook events are written.
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datetime import datetime, timezone
from supabase import create_client
from config import Config
from core.listener import Listener, ListenerState
from services.polymarket_discovery import PolymarketDiscoveryService
from services.polymarket_websocket_client import PolymarketWebSocketClient
from services.supabase_writer import SupabaseWriter
from utils.logger import LoggerFactory
from models import ListenerConfig, ListenerFilters


async def main():
    config = Config()
    logger_factory = LoggerFactory('INFO')
    logger = logger_factory.create('test')

    supabase = create_client(config.supabase_url, config.supabase_key)

    # Check counts before
    before_snapshots = supabase.table('orderbook_snapshots').select('count', count='exact').execute()
    before_trades = supabase.table('trades').select('count', count='exact').execute()
    print(f"Before: {before_snapshots.count} snapshots, {before_trades.count} trades")

    # Get real listener UUID
    listener_row = supabase.table("listeners").select("*").eq("name", "nba-listener").single().execute()
    listener_uuid = listener_row.data["id"]
    print(f"Using listener UUID: {listener_uuid}")

    # Create a config that only finds a few specific markets (moneyline games only)
    # Using a custom filter to limit results
    test_config = ListenerConfig(
        id=listener_uuid,
        name="test-orderbook-listener",
        filters=ListenerFilters(
            series_ids=["10345"],  # NBA
        ),
        discovery_interval_seconds=300,  # Long interval - we only want initial discovery
        is_active=True,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )

    # Create services
    discovery = PolymarketDiscoveryService(logger)
    websocket = PolymarketWebSocketClient(logger)
    writer = SupabaseWriter(supabase, listener_uuid, logger)

    # Create listener
    listener = Listener(
        config=test_config,
        discovery=discovery,
        websocket=websocket,
        writer=writer,
        logger=logger,
    )

    print("\nStarting listener (will process many markets, but let's watch for orderbooks)...")
    await listener.start()

    # Run for 60 seconds, checking progress
    print("Running for 60 seconds (subscriptions + orderbook collection)...")
    for i in range(60):
        await asyncio.sleep(1)
        if i % 5 == 0:
            current_snapshots = supabase.table('orderbook_snapshots').select('count', count='exact').execute()
            current_trades = supabase.table('trades').select('count', count='exact').execute()
            print(f"  t={i}s: {len(listener.state.subscribed_markets)} markets subscribed, "
                  f"{listener.state.events_processed} events processed, "
                  f"snapshots: {current_snapshots.count}, trades: {current_trades.count}")

    print("\nStopping...")
    await listener.stop()

    # Check counts after
    after_snapshots = supabase.table('orderbook_snapshots').select('count', count='exact').execute()
    after_trades = supabase.table('trades').select('count', count='exact').execute()
    print(f"\nAfter: {after_snapshots.count} snapshots, {after_trades.count} trades")
    print(f"New snapshots: {after_snapshots.count - before_snapshots.count}")
    print(f"New trades: {after_trades.count - before_trades.count}")


if __name__ == "__main__":
    asyncio.run(main())
