#!/usr/bin/env python3
"""
Setup script for local PostgreSQL database.

Usage:
    python scripts/setup_local_db.py [--reset] [--seed]

Options:
    --reset     Drop and recreate all tables
    --seed      Insert sample listener configuration
"""
import argparse
import sys
from pathlib import Path

import psycopg2


def get_connection(dsn: str = None):
    if dsn is None:
        dsn = "postgresql://polymarket:polymarket@localhost:5432/polymarket"
    return psycopg2.connect(dsn)


def run_migrations(conn, reset: bool = False):
    migrations_dir = Path(__file__).parent.parent / "migrations"
    cursor = conn.cursor()

    if reset:
        print("Dropping existing tables...")
        cursor.execute("""
            DROP TABLE IF EXISTS orderbook_snapshots CASCADE;
            DROP TABLE IF EXISTS trades CASCADE;
            DROP TABLE IF EXISTS market_state_history CASCADE;
            DROP TABLE IF EXISTS markets CASCADE;
            DROP TABLE IF EXISTS listeners CASCADE;
        """)
        conn.commit()
        print("Tables dropped.")

    migration_files = sorted(migrations_dir.glob("*.sql"))
    for migration_file in migration_files:
        print(f"Running migration: {migration_file.name}")
        sql = migration_file.read_text()
        try:
            cursor.execute(sql)
            conn.commit()
            print(f"  ✓ {migration_file.name}")
        except psycopg2.Error as e:
            conn.rollback()
            if "already exists" in str(e) or "duplicate" in str(e).lower():
                print(f"  ⊘ {migration_file.name} (already applied)")
            else:
                print(f"  ✗ {migration_file.name}: {e}")
                raise

    cursor.close()


def seed_listener(conn):
    cursor = conn.cursor()
    print("Seeding sample listener...")

    cursor.execute("""
        INSERT INTO listeners (name, description, filters, discovery_interval_seconds, emit_interval_ms, enable_forward_fill, is_active)
        VALUES (
            'nba-test',
            'NBA markets for local testing',
            '{"series_ids": ["10345"], "min_liquidity": 1000}',
            60,
            100,
            false,
            true
        )
        ON CONFLICT (name) DO UPDATE SET
            filters = EXCLUDED.filters,
            is_active = EXCLUDED.is_active
        RETURNING id, name;
    """)

    row = cursor.fetchone()
    conn.commit()
    cursor.close()

    if row:
        print(f"  ✓ Created listener: {row[1]} (id: {row[0]})")


def main():
    parser = argparse.ArgumentParser(description="Setup local PostgreSQL database")
    parser.add_argument("--reset", action="store_true", help="Drop and recreate all tables")
    parser.add_argument("--seed", action="store_true", help="Insert sample listener configuration")
    parser.add_argument("--dsn", type=str, help="PostgreSQL DSN (default: postgresql://polymarket:polymarket@localhost:5432/polymarket)")
    args = parser.parse_args()

    try:
        conn = get_connection(args.dsn)
        print("Connected to PostgreSQL")
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        print("\nMake sure PostgreSQL is running:")
        print("  docker compose up -d")
        sys.exit(1)

    try:
        run_migrations(conn, reset=args.reset)
        if args.seed:
            seed_listener(conn)
        print("\nDatabase setup complete!")
    except Exception as e:
        print(f"\nError during setup: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
