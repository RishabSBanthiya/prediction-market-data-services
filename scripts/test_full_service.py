"""
Test the full listener service by running it for a short period.
Uses the real listener config from the database.
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from supabase import create_client
from config import Config
from core.listener_factory import ListenerFactory
from core.listener_manager import ListenerManager
from services.config_loader import SupabaseConfigLoader
from utils.logger import LoggerFactory


async def main():
    config = Config()
    logger_factory = LoggerFactory('INFO')
    logger = logger_factory.create('test')

    supabase = create_client(config.supabase_url, config.supabase_key)

    # Check counts before
    before_snapshots = supabase.table('orderbook_snapshots').select('count', count='exact').execute()
    before_trades = supabase.table('trades').select('count', count='exact').execute()
    print(f"Before: {before_snapshots.count} snapshots, {before_trades.count} trades")

    # Load configs from database (this gets the real UUIDs)
    config_loader = SupabaseConfigLoader(supabase)
    configs = await config_loader.load_active_configs()
    print(f"Loaded {len(configs)} listener configs:")
    for c in configs:
        print(f"  - {c.name} (id={c.id})")

    factory = ListenerFactory(supabase, logger_factory)
    manager = ListenerManager(factory, config_loader, logger)

    print("\nStarting listener manager...")
    await manager.start()

    # Run for 30 seconds
    print("Running for 30 seconds...")
    for i in range(30):
        await asyncio.sleep(1)
        if i % 10 == 0:
            status = await manager.get_status()
            for s in status:
                print(f"  t={i}s: {s['name']} - {s['subscribed_markets']} markets, {s['events_processed']} events processed")

    print("\nStopping...")
    await manager.stop()

    # Check counts after
    after_snapshots = supabase.table('orderbook_snapshots').select('count', count='exact').execute()
    after_trades = supabase.table('trades').select('count', count='exact').execute()
    print(f"\nAfter: {after_snapshots.count} snapshots, {after_trades.count} trades")
    print(f"New snapshots: {after_snapshots.count - before_snapshots.count}")
    print(f"New trades: {after_trades.count - before_trades.count}")


if __name__ == "__main__":
    asyncio.run(main())
