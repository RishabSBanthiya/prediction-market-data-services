#!/usr/bin/env python3
"""
Seed script for creating Polymarket BTC 15-minute listener configuration.

Usage:
    python scripts/seed_btc_15m_listener.py

This will create a Polymarket listener for BTC 15-minute up/down markets.
"""
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from config import Config


def seed_btc_15m_listener_supabase():
    """Seed BTC 15m listener using Supabase."""
    from supabase import create_client

    config = Config()
    supabase = create_client(config.supabase_url, config.supabase_key)

    listener_data = {
        "name": "btc-15m-listener",
        "description": "Tracks BTC 15-minute up/down prediction markets",
        "platform": "polymarket",
        "filters": {
            "series_ids": ["10192"],  # BTC Up or Down 15m series
        },
        "discovery_interval_seconds": 60,
        "emit_interval_ms": 100,
        "enable_forward_fill": False,
        "is_active": True,
    }

    result = supabase.table("listeners").upsert(
        listener_data,
        on_conflict="name"
    ).execute()

    print(f"Seeded Polymarket BTC 15m listener: {result.data}")
    return result.data


def seed_btc_15m_listener_postgres():
    """Seed BTC 15m listener using PostgreSQL."""
    import json
    import psycopg2

    config = Config()
    conn = psycopg2.connect(config.postgres_dsn)
    cur = conn.cursor()

    listener_data = {
        "name": "btc-15m-listener",
        "description": "Tracks BTC 15-minute up/down prediction markets",
        "platform": "polymarket",
        "filters": {
            "series_ids": ["10192"],  # BTC Up or Down 15m series
        },
        "discovery_interval_seconds": 60,
        "emit_interval_ms": 100,
        "enable_forward_fill": False,
        "is_active": True,
    }

    cur.execute(
        """
        INSERT INTO listeners (name, description, platform, filters, discovery_interval_seconds,
                               emit_interval_ms, enable_forward_fill, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (name) DO UPDATE SET
            description = EXCLUDED.description,
            platform = EXCLUDED.platform,
            filters = EXCLUDED.filters,
            discovery_interval_seconds = EXCLUDED.discovery_interval_seconds,
            emit_interval_ms = EXCLUDED.emit_interval_ms,
            enable_forward_fill = EXCLUDED.enable_forward_fill,
            is_active = EXCLUDED.is_active,
            updated_at = NOW()
        RETURNING id, name
        """,
        (
            listener_data["name"],
            listener_data["description"],
            listener_data["platform"],
            json.dumps(listener_data["filters"]),
            listener_data["discovery_interval_seconds"],
            listener_data["emit_interval_ms"],
            listener_data["enable_forward_fill"],
            listener_data["is_active"],
        )
    )

    result = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    print(f"Seeded Polymarket BTC 15m listener: id={result[0]}, name={result[1]}")
    return result


def verify_discovery():
    """Verify that discovery returns both Up and Down tokens."""
    import asyncio
    import structlog
    from services.polymarket_discovery import PolymarketDiscoveryService

    async def run():
        logger = structlog.get_logger()
        discovery = PolymarketDiscoveryService(logger)

        filters = {"series_ids": ["10192"]}
        markets = await discovery.discover_markets(filters)
        await discovery.close()

        print(f"\n✓ Discovery found {len(markets)} markets (tokens)")

        from collections import defaultdict
        by_question = defaultdict(list)
        for m in markets:
            by_question[m.question].append(m.outcome)

        two_tokens = sum(1 for outcomes in by_question.values() if len(outcomes) == 2)
        one_token = sum(1 for outcomes in by_question.values() if len(outcomes) == 1)

        print(f"  Questions with both Up+Down: {two_tokens}")
        print(f"  Questions with single token: {one_token}")

        if two_tokens > 0:
            print("\n  Sample markets with both tokens:")
            for q, outcomes in list(by_question.items())[:2]:
                print(f"    {q[:50]}... -> {outcomes}")

        return two_tokens > 0

    return asyncio.run(run())


def main():
    config = Config()

    print("=" * 60)
    print("SEED POLYMARKET BTC 15M LISTENER")
    print("=" * 60)
    print(f"\nDatabase mode: {config.db_mode}")

    if config.db_mode == "postgres":
        seed_btc_15m_listener_postgres()
    else:
        seed_btc_15m_listener_supabase()

    print("\n--- Verifying Discovery ---")
    if verify_discovery():
        print("\n✓ Listener is correctly configured to capture BOTH Up and Down tokens!")
    else:
        print("\n⚠ Discovery returned single-token markets only")

    print("\n" + "=" * 60)
    print("NEXT STEPS")
    print("=" * 60)
    print("""
1. Start the service to collect data:
   python src/main.py

2. After collecting data, run the comparison:
   python scripts/compare_polymarket_kalshi.py
""")


if __name__ == "__main__":
    main()
