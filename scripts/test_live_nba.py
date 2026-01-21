"""
Comprehensive test for live NBA games.
1. Clears Supabase tables
2. Tests market discovery (finds correct NBA markets)
3. Tests WebSocket events with forward-fill mechanism
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from supabase import create_client
from config import Config
from services.polymarket_discovery import PolymarketDiscoveryService
from services.polymarket_websocket_client import PolymarketWebSocketClient
from services.state_forward_filler import StateForwardFiller
from utils.logger import LoggerFactory
from models import OrderbookSnapshot, OrderLevel


def clear_tables(supabase):
    """Clear all data from tables (in correct order for foreign keys)."""
    print("\n=== CLEARING TABLES ===")
    tables = [
        "orderbook_snapshots",
        "trades",
        "market_state_history",
        "markets",
        "listeners",
    ]
    for table in tables:
        try:
            # Delete all rows
            supabase.table(table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            print(f"  Cleared: {table}")
        except Exception as e:
            print(f"  Error clearing {table}: {e}")
    print("  Done!\n")


async def test_market_discovery(logger):
    """Test that we find correct NBA markets."""
    print("\n=== TEST 1: MARKET DISCOVERY ===")
    discovery = PolymarketDiscoveryService(logger)

    try:
        markets = await discovery.discover_markets({"series_ids": ["10345"]})  # NBA series
        print(f"Found {len(markets)} NBA markets")

        if not markets:
            print("ERROR: No markets found! Are there active NBA games?")
            return []

        # Group by event to show games
        events = {}
        for m in markets:
            event_title = m.event_title or "Unknown Event"
            if event_title not in events:
                events[event_title] = []
            events[event_title].append(m)

        print(f"\nActive games ({len(events)}):")
        for event_title, event_markets in events.items():
            print(f"\n  {event_title}")
            for m in event_markets[:3]:  # Show first 3 markets per game
                print(f"    - {m.question}")
                print(f"      token: {m.token_id[:40]}...")

        return markets
    finally:
        await discovery.close()


async def test_websocket_events(markets, logger, duration_seconds=30):
    """Test WebSocket events are being received."""
    print(f"\n=== TEST 2: WEBSOCKET EVENTS ({duration_seconds}s) ===")

    if not markets:
        print("Skipping - no markets to test")
        return

    # Prefer main game outcome markets (more liquid) - look for "vs." without player names
    # and spreads/O-U which tend to be more active
    main_markets = []
    spread_markets = []
    ou_markets = []
    other_markets = []

    for m in markets:
        q = m.question or ""
        if " vs. " in q and ":" not in q:  # Main outcome (Team vs Team)
            main_markets.append(m)
        elif "Spread:" in q:
            spread_markets.append(m)
        elif "O/U" in q:
            ou_markets.append(m)
        else:
            other_markets.append(m)

    # Prioritize: main outcomes, then spreads, then O/U
    prioritized = main_markets + spread_markets + ou_markets + other_markets
    test_markets = prioritized[:15]  # Take top 15 most liquid

    print(f"Selected {len(test_markets)} markets (prioritizing main outcomes):")
    for m in test_markets[:5]:
        print(f"  - {m.question}")

    token_ids = [m.token_id for m in test_markets if m.token_id]

    print(f"Subscribing to {len(token_ids)} tokens...")

    ws = PolymarketWebSocketClient(logger)
    await ws.connect()
    await ws.subscribe(token_ids)

    stats = {
        "total": 0,
        "book": 0,
        "trade": 0,
        "other": 0,
        "by_asset": {},
    }

    async def collect_events():
        async for event in ws.events():
            stats["total"] += 1
            event_type = event.get("event_type", "unknown")
            asset_id = event.get("asset_id", "")[:20]

            if event_type == "book":
                stats["book"] += 1
                stats["by_asset"][asset_id] = stats["by_asset"].get(asset_id, 0) + 1
                bids = event.get('bids', [])
                asks = event.get('asks', [])
                best_bid = bids[0].get('price') if bids else 'N/A'
                best_ask = asks[0].get('price') if asks else 'N/A'
                print(f"  BOOK #{stats['book']:3d}: {asset_id}... bid={best_bid} ask={best_ask}")
            elif event_type == "last_trade_price":
                stats["trade"] += 1
                print(f"  TRADE #{stats['trade']:3d}: price={event.get('price')}, size={event.get('size')}")
            else:
                stats["other"] += 1
                if stats["other"] <= 3:  # Show first 3 other events
                    print(f"  OTHER: type={event_type}, keys={list(event.keys())}")

    try:
        await asyncio.wait_for(collect_events(), timeout=duration_seconds)
    except asyncio.TimeoutError:
        pass

    await ws.disconnect()

    print(f"\n--- WebSocket Results ---")
    print(f"Total events: {stats['total']}")
    print(f"Book events:  {stats['book']}")
    print(f"Trade events: {stats['trade']}")
    print(f"Other events: {stats['other']}")
    print(f"Events per second: {stats['total'] / duration_seconds:.2f}")

    if stats["by_asset"]:
        print(f"\nEvents by asset (top 5):")
        sorted_assets = sorted(stats["by_asset"].items(), key=lambda x: x[1], reverse=True)
        for asset, count in sorted_assets[:5]:
            print(f"  {asset}...: {count} events")

    return stats


async def test_forward_filler(markets, logger, duration_seconds=10):
    """Test that forward-filler emits continuous snapshots."""
    print(f"\n=== TEST 3: FORWARD-FILL MECHANISM ({duration_seconds}s) ===")

    if not markets:
        print("Skipping - no markets to test")
        return

    # Prefer main game outcomes (more liquid)
    main_markets = [m for m in markets if " vs. " in (m.question or "") and ":" not in (m.question or "")]
    test_markets = (main_markets or markets)[:5]

    print(f"Testing with markets:")
    for m in test_markets:
        print(f"  - {m.question}")

    token_ids = [(m.token_id, m.condition_id) for m in test_markets if m.token_id]

    # Create forward-filler with 100ms interval
    filler = StateForwardFiller(
        listener_id="test-listener",
        logger=logger,
        emit_interval_ms=100,
    )

    filled_snapshots = []

    async def on_snapshot(snapshot: OrderbookSnapshot):
        filled_snapshots.append(snapshot)
        if len(filled_snapshots) % 10 == 0:
            print(f"  Forward-filled snapshot #{len(filled_snapshots)}: {snapshot.asset_id[:20]}... @ {snapshot.timestamp}")

    filler.set_snapshot_callback(on_snapshot)

    # Add tokens
    for token_id, condition_id in token_ids:
        filler.add_token(token_id, condition_id)

    print(f"Tracking {len(token_ids)} tokens")
    print("Simulating WebSocket events...")

    # Start the filler
    await filler.start()

    # Connect to WebSocket and feed events to filler
    ws = PolymarketWebSocketClient(logger)
    await ws.connect()
    await ws.subscribe([t[0] for t in token_ids])

    real_events = 0

    async def feed_events():
        nonlocal real_events
        async for event in ws.events():
            if event.get("event_type") == "book":
                real_events += 1
                # Parse and feed to filler
                bids = [OrderLevel(price=b["price"], size=b["size"]) for b in event.get("bids", [])]
                asks = [OrderLevel(price=a["price"], size=a["size"]) for a in event.get("asks", [])]
                snapshot = OrderbookSnapshot(
                    listener_id="test-listener",
                    asset_id=event.get("asset_id", ""),
                    market=event.get("market", ""),
                    timestamp=event.get("timestamp", 0),
                    bids=bids,
                    asks=asks,
                )
                snapshot.compute_metrics()
                filler.update_state(snapshot)
                print(f"  Real event #{real_events}: {event.get('asset_id', '')[:20]}...")

    try:
        await asyncio.wait_for(feed_events(), timeout=duration_seconds)
    except asyncio.TimeoutError:
        pass

    await filler.stop()
    await ws.disconnect()

    print(f"\n--- Forward-Fill Results ---")
    print(f"Real WebSocket events: {real_events}")
    print(f"Forward-filled snapshots: {len(filled_snapshots)}")

    if real_events > 0:
        ratio = len(filled_snapshots) / real_events
        print(f"Fill ratio: {ratio:.1f}x (expected ~{duration_seconds * 10 / max(1, real_events):.1f}x at 100ms interval)")

    # Check for continuous timestamps
    if len(filled_snapshots) >= 2:
        timestamps = [s.timestamp for s in filled_snapshots]
        gaps = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
        avg_gap = sum(gaps) / len(gaps) if gaps else 0
        print(f"Average gap between snapshots: {avg_gap:.0f}ms (target: 100ms)")


async def main():
    print("=" * 60)
    print("LIVE NBA FORWARD-FILL TEST")
    print("=" * 60)

    # Setup
    config = Config()
    supabase = create_client(config.supabase_url, config.supabase_key)
    logger_factory = LoggerFactory('WARNING')  # Less verbose
    logger = logger_factory.create('test')

    # Clear tables
    clear_tables(supabase)

    # Test 1: Market discovery
    markets = await test_market_discovery(logger)

    if not markets:
        print("\nNo markets found. Exiting.")
        return

    # Test 2: WebSocket events (30 seconds)
    ws_stats = await test_websocket_events(markets, logger, duration_seconds=30)

    # Test 3: Forward-fill mechanism (10 seconds)
    await test_forward_filler(markets, logger, duration_seconds=10)

    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)

    # Summary
    if ws_stats and ws_stats["book"] > 0:
        print(f"\nSUCCESS: Received {ws_stats['book']} book events in 30 seconds")
        print("The WebSocket is working and receiving live orderbook updates!")
    else:
        print(f"\nWARNING: Only received {ws_stats.get('book', 0) if ws_stats else 0} book events")
        print("This might indicate low market activity or connection issues.")


if __name__ == "__main__":
    asyncio.run(main())
