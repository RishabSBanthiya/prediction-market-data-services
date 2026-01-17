"""
Test with freshly discovered active markets.
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from services.market_discovery import PolymarketDiscoveryService
from services.websocket_client import PolymarketWebSocketClient
from utils.logger import LoggerFactory


async def main():
    logger_factory = LoggerFactory('INFO')  # Less verbose
    logger = logger_factory.create('test')

    # First discover some active markets
    print("Discovering active NBA markets...")
    discovery = PolymarketDiscoveryService(logger)
    markets = await discovery.discover_markets({"series_ids": ["10345"]})
    print(f"Found {len(markets)} markets")

    if not markets:
        print("No markets found!")
        await discovery.close()
        return

    # Take first 5 markets
    test_markets = markets[:5]
    token_ids = [m.token_id for m in test_markets if m.token_id]
    print(f"\nSelected markets:")
    for m in test_markets:
        print(f"  - {m.question}: {m.token_id[:30]}...")

    print(f"\nSubscribing to {len(token_ids)} tokens...")

    # Connect and subscribe
    ws = PolymarketWebSocketClient(logger)
    await ws.connect()
    await ws.subscribe(token_ids)
    print("Subscribed!")

    print("\nWaiting for events (20 seconds)...")
    event_count = 0
    book_count = 0
    trade_count = 0

    async def collect_events():
        nonlocal event_count, book_count, trade_count
        async for event in ws.events():
            event_count += 1
            event_type = event.get("event_type", "unknown")
            if event_type == "book":
                book_count += 1
                bids = event.get('bids', [])
                asks = event.get('asks', [])
                best_bid = bids[0].get('price') if bids else 'N/A'
                best_ask = asks[0].get('price') if asks else 'N/A'
                print(f"  BOOK: asset={event.get('asset_id', '')[:20]}... bid={best_bid} ask={best_ask}")
            elif event_type == "last_trade_price":
                trade_count += 1
                print(f"  TRADE: price={event.get('price')}, size={event.get('size')}")
            elif event_type == "price_change":
                print(f"  PRICE_CHANGE: {event}")
            else:
                print(f"  OTHER: type={event_type}, keys={list(event.keys())}")

    try:
        await asyncio.wait_for(collect_events(), timeout=20.0)
    except asyncio.TimeoutError:
        pass

    print(f"\n--- Results ---")
    print(f"Total events: {event_count}")
    print(f"Book events: {book_count}")
    print(f"Trade events: {trade_count}")

    await ws.disconnect()
    await discovery.close()
    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
