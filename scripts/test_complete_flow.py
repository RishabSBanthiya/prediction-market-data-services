"""
Complete flow test: discover markets, subscribe, receive orderbook events, write to Supabase.
Uses a limited number of markets to avoid queue flooding.
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datetime import datetime, timezone
from supabase import create_client
from config import Config
from core.listener import Listener
from core.events import OrderbookEvent, TradeEvent
from services.polymarket_discovery import PolymarketDiscoveryService
from services.polymarket_websocket_client import PolymarketWebSocketClient
from services.supabase_writer import SupabaseWriter
from utils.logger import LoggerFactory
from models import ListenerConfig, ListenerFilters, Market


class LimitedDiscoveryService(PolymarketDiscoveryService):
    """Discovery service that limits the number of markets returned."""

    def __init__(self, logger, max_markets=10):
        super().__init__(logger)
        self.max_markets = max_markets

    async def discover_markets(self, filters: dict) -> list[Market]:
        all_markets = await super().discover_markets(filters)
        # Return only the first N markets
        limited = all_markets[:self.max_markets]
        self._logger.info("discovery_limited", total=len(all_markets), returned=len(limited))
        return limited


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

    # Create config
    test_config = ListenerConfig(
        id=listener_uuid,
        name="test-limited-listener",
        filters=ListenerFilters(series_ids=["10345"]),
        discovery_interval_seconds=600,  # Long interval - don't rediscover
        is_active=True,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )

    # Create services - LIMITED discovery
    discovery = LimitedDiscoveryService(logger, max_markets=10)
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

    print("\nStarting listener with limited discovery (max 10 markets)...")
    await listener.start()

    # Run for 30 seconds
    print("Running for 30 seconds...")
    for i in range(30):
        await asyncio.sleep(1)
        if i % 5 == 0:
            current_snapshots = supabase.table('orderbook_snapshots').select('count', count='exact').execute()
            current_trades = supabase.table('trades').select('count', count='exact').execute()
            print(f"  t={i}s: {len(listener.state.subscribed_markets)} markets, "
                  f"{listener.state.events_processed} events, "
                  f"snapshots: {current_snapshots.count} (+{current_snapshots.count - before_snapshots.count}), "
                  f"trades: {current_trades.count} (+{current_trades.count - before_trades.count})")

    print("\nStopping...")
    await listener.stop()

    # Final check
    after_snapshots = supabase.table('orderbook_snapshots').select('count', count='exact').execute()
    after_trades = supabase.table('trades').select('count', count='exact').execute()
    print(f"\n=== Final Results ===")
    print(f"Snapshots: {before_snapshots.count} -> {after_snapshots.count} (+{after_snapshots.count - before_snapshots.count})")
    print(f"Trades: {before_trades.count} -> {after_trades.count} (+{after_trades.count - before_trades.count})")
    print(f"Events processed: {listener.state.events_processed}")
    print(f"Errors: {listener.state.errors_count}")


if __name__ == "__main__":
    asyncio.run(main())
