#!/usr/bin/env python3
"""
Test script for Kalshi integration with live API.

Usage:
    # Test market discovery only (no auth required)
    python scripts/test_kalshi_live.py --discovery

    # Test WebSocket connection (requires auth)
    python scripts/test_kalshi_live.py --websocket

    # Test both
    python scripts/test_kalshi_live.py --all
"""
import asyncio
import argparse
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from config import Config
from utils.logger import LoggerFactory


async def test_discovery():
    """Test Kalshi market discovery via REST API."""
    print("\n=== Testing Kalshi Market Discovery ===\n")

    from services.kalshi_discovery import KalshiDiscoveryService

    logger = LoggerFactory("DEBUG").create("test_discovery")
    discovery = KalshiDiscoveryService(logger)

    try:
        # Test fetching open markets (no filters)
        print("Fetching open markets (limit 10)...")
        markets = await discovery.discover_markets({"status": "open"})
        print(f"Found {len(markets)} open markets\n")

        if markets:
            print("Sample markets:")
            for m in markets[:5]:
                print(f"  - {m.token_id}: {m.question[:60] if m.question else 'N/A'}...")
                print(f"    Volume: {m.volume}, Liquidity: {m.liquidity}")
            print()

        # Test fetching specific series if available
        print("Testing series filter (KXELECTION)...")
        election_markets = await discovery.discover_markets({
            "series_tickers": ["KXELECTION"],
            "status": "open",
        })
        print(f"Found {len(election_markets)} election markets\n")

        if election_markets:
            print("Sample election markets:")
            for m in election_markets[:3]:
                print(f"  - {m.token_id}: {m.question[:60] if m.question else 'N/A'}...")

        print("\n[SUCCESS] Market discovery working correctly!")
        return True

    except Exception as e:
        print(f"\n[ERROR] Discovery failed: {e}")
        return False

    finally:
        await discovery.close()


async def test_websocket():
    """Test Kalshi WebSocket connection."""
    print("\n=== Testing Kalshi WebSocket Connection ===\n")

    config = Config()

    if not config.kalshi_api_key:
        print("[ERROR] KALSHI_API_KEY not set in environment")
        print("Please set KALSHI_API_KEY and KALSHI_PRIVATE_KEY_PATH")
        return False

    if not config.kalshi_private_key and not config.kalshi_private_key_path:
        print("[ERROR] KALSHI_PRIVATE_KEY or KALSHI_PRIVATE_KEY_PATH not set")
        return False

    from services.kalshi_auth import KalshiAuthenticator
    from services.kalshi_discovery import KalshiDiscoveryService
    from services.kalshi_websocket_client import KalshiWebSocketClient

    logger = LoggerFactory("DEBUG").create("test_websocket")

    try:
        # Initialize authenticator
        print("Initializing authenticator...")
        auth = KalshiAuthenticator(
            api_key=config.kalshi_api_key,
            private_key_path=config.kalshi_private_key_path,
            private_key_pem=config.kalshi_private_key,
        )
        print(f"  API Key: {auth.api_key[:10]}...")

        # First discover a market to subscribe to
        print("\nDiscovering markets for subscription...")
        discovery = KalshiDiscoveryService(logger)
        markets = await discovery.discover_markets({"status": "open"})
        await discovery.close()

        if not markets:
            print("[ERROR] No markets found to subscribe to")
            return False

        ticker = markets[0].token_id
        print(f"  Will subscribe to: {ticker}")
        print(f"  Market: {markets[0].question[:60] if markets[0].question else 'N/A'}...")

        # Connect to WebSocket
        print("\nConnecting to WebSocket...")
        client = KalshiWebSocketClient(logger, auth)
        await client.connect()
        print("  Connected!")

        # Subscribe to market
        print(f"\nSubscribing to {ticker}...")
        await client.subscribe([ticker])
        print("  Subscribed!")

        # Receive events for 30 seconds
        print("\nReceiving events (30 seconds)...")
        event_count = 0
        try:
            async with asyncio.timeout(30):
                async for event in client.events():
                    event_count += 1
                    event_type = event.get("event_type", "unknown")
                    asset_id = event.get("asset_id", "")[:20]
                    print(f"  [{event_count}] {event_type}: {asset_id}")

                    if event_type == "book":
                        bids = len(event.get("bids", []))
                        asks = len(event.get("asks", []))
                        print(f"       Bids: {bids}, Asks: {asks}")
                    elif event_type == "last_trade_price":
                        price = event.get("price", "?")
                        size = event.get("size", "?")
                        print(f"       Price: {price}, Size: {size}")

                    if event_count >= 10:
                        print("\n  (Stopping after 10 events)")
                        break
        except asyncio.TimeoutError:
            print(f"\n  (Timeout reached, received {event_count} events)")

        await client.disconnect()
        print("\n[SUCCESS] WebSocket connection working correctly!")
        return True

    except Exception as e:
        print(f"\n[ERROR] WebSocket test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    parser = argparse.ArgumentParser(description="Test Kalshi integration")
    parser.add_argument("--discovery", action="store_true", help="Test market discovery")
    parser.add_argument("--websocket", action="store_true", help="Test WebSocket connection")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    args = parser.parse_args()

    # Default to discovery if no args
    if not args.discovery and not args.websocket and not args.all:
        args.discovery = True

    results = []

    if args.discovery or args.all:
        results.append(("Discovery", await test_discovery()))

    if args.websocket or args.all:
        results.append(("WebSocket", await test_websocket()))

    print("\n" + "=" * 50)
    print("Summary:")
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"  {name}: {status}")
    print("=" * 50)

    # Exit with error if any test failed
    if not all(passed for _, passed in results):
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
