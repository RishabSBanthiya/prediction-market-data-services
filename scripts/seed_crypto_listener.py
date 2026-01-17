import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from supabase import create_client
from config import Config


def seed_crypto_15min_listener():
    config = Config()
    supabase = create_client(config.supabase_url, config.supabase_key)

    listener_data = {
        "name": "crypto-15min-listener",
        "description": "Tracks 15-minute crypto price prediction markets (BTC, ETH, etc.)",
        "filters": {
            "series_ids": ["10192"],  # BTC Up or Down 15m
        },
        "discovery_interval_seconds": 30,  # Check frequently for short-lived markets
        "enable_forward_fill": False,  # Only store real events
        "is_active": True,
    }

    result = supabase.table("listeners").upsert(
        listener_data,
        on_conflict="name"
    ).execute()

    print(f"Seeded listener: {result.data}")
    return result.data


if __name__ == "__main__":
    seed_crypto_15min_listener()
