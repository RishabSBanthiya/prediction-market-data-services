"""
Minimal test: connect to WebSocket, subscribe to one market, see what events come in.
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from services.polymarket_websocket_client import PolymarketWebSocketClient
from utils.logger import LoggerFactory


async def main():
    logger_factory = LoggerFactory('DEBUG')
    logger = logger_factory.create('test')

    ws = PolymarketWebSocketClient(logger)

    print("Connecting to WebSocket...")
    await ws.connect()
    print("Connected!")

    # Subscribe to a known active market (Lakers spread)
    # Using a token_id from the previous test output
    test_token = "99709118976848402352541021958986778898177010738493743012245871102808183876601"  # Spread: Lakers (-4.5)
    print(f"Subscribing to token: {test_token}")
    await ws.subscribe([test_token])
    print("Subscribed!")

    print("\nWaiting for events (15 seconds)...")
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
                print(f"  BOOK EVENT: best_bid={event.get('bids', [{}])[0].get('price') if event.get('bids') else 'N/A'}, "
                      f"best_ask={event.get('asks', [{}])[0].get('price') if event.get('asks') else 'N/A'}")
            elif event_type == "last_trade_price":
                trade_count += 1
                print(f"  TRADE EVENT: price={event.get('price')}, size={event.get('size')}")
            else:
                print(f"  OTHER EVENT: type={event_type}")

    try:
        await asyncio.wait_for(collect_events(), timeout=15.0)
    except asyncio.TimeoutError:
        pass

    print(f"\n--- Results ---")
    print(f"Total events: {event_count}")
    print(f"Book events: {book_count}")
    print(f"Trade events: {trade_count}")

    await ws.disconnect()
    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
