#!/usr/bin/env python3
"""
Seed script for creating Kalshi listener configurations.

Usage:
    python scripts/seed_kalshi_listener.py

This will create a default Kalshi listener for election markets.
Customize the filters as needed for your use case.
"""
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from config import Config


def seed_kalshi_listener_supabase():
    """Seed Kalshi listener using Supabase."""
    from supabase import create_client

    config = Config()
    supabase = create_client(config.supabase_url, config.supabase_key)

    listener_data = {
        "name": "kalshi-elections",
        "description": "Tracks Kalshi 15m btc markets",
        "platform": "kalshi",
        "filters": {
            "series_tickers": ["kxbtc15m"],
            "status": "open",
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

    print(f"Seeded Kalshi listener: {result.data}")
    return result.data


def seed_kalshi_listener_postgres():
    """Seed Kalshi listener using PostgreSQL."""
    import json
    import psycopg2

    config = Config()
    conn = psycopg2.connect(config.postgres_dsn)
    cur = conn.cursor()

    listener_data = {
        "name": "kalshi-elections",
        "description": "Tracks Kalshi election prediction markets",
        "platform": "kalshi",
        "filters": {
            "series_tickers": ["KXELECTION"],
            "status": "open",
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

    print(f"Seeded Kalshi listener: id={result[0]}, name={result[1]}")
    return result


def main():
    config = Config()

    print("Seeding Kalshi listener configuration...")
    print(f"Database mode: {config.db_mode}")

    if config.db_mode == "postgres":
        seed_kalshi_listener_postgres()
    else:
        seed_kalshi_listener_supabase()

    print("\nKalshi listener seeded successfully!")
    print("\nTo start capturing Kalshi data, ensure you have:")
    print("  1. KALSHI_API_KEY set in your environment")
    print("  2. KALSHI_PRIVATE_KEY_PATH pointing to your PEM file")
    print("  3. Run the database migration: migrations/003_add_platform_column.sql")
    print("  4. Start the service: python src/main.py")


if __name__ == "__main__":
    main()
