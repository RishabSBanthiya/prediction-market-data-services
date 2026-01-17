import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from supabase import create_client
from config import Config


def seed_nba_listener():
    config = Config()
    supabase = create_client(config.supabase_url, config.supabase_key)

    listener_data = {
        "name": "nba-listener",
        "description": "Tracks NBA sports betting markets",
        "filters": {
            "tag_ids": [100639],
        },
        "discovery_interval_seconds": 60,
        "is_active": True,
    }

    result = supabase.table("listeners").upsert(
        listener_data,
        on_conflict="name"
    ).execute()

    print(f"Seeded listener: {result.data}")
    return result.data


if __name__ == "__main__":
    seed_nba_listener()
