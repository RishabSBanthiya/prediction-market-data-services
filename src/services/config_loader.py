import json
from datetime import datetime, timezone
from typing import Optional

from models import ListenerConfig, Platform


class SupabaseConfigLoader:
    def __init__(self, client):
        self._client = client

    async def load_active_configs(self) -> list[ListenerConfig]:
        response = self._client.table("listeners").select("*").eq("is_active", True).execute()
        return [self._parse_config(row) for row in response.data]

    async def load_config(self, listener_id: str) -> Optional[ListenerConfig]:
        response = self._client.table("listeners").select("*").eq("id", listener_id).execute()
        if response.data:
            return self._parse_config(response.data[0])
        return None

    def _parse_config(self, row: dict) -> ListenerConfig:
        filters_data = row.get("filters", {})
        platform_str = row.get("platform", "polymarket")
        return ListenerConfig(
            id=str(row["id"]),
            name=row["name"],
            platform=Platform(platform_str),
            description=row.get("description"),
            filters=filters_data,  # Keep as dict, validated by discovery service
            discovery_interval_seconds=row.get("discovery_interval_seconds", 60),
            emit_interval_ms=row.get("emit_interval_ms", 100),
            enable_forward_fill=row.get("enable_forward_fill", False),
            is_active=row.get("is_active", True),
            created_at=self._parse_datetime(row.get("created_at")),
            updated_at=self._parse_datetime(row.get("updated_at")),
        )

    def _parse_datetime(self, value) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(value.replace("Z", "+00:00"))


class PostgresConfigLoader:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool = None

    async def _ensure_pool(self):
        if self._pool is None:
            import asyncpg
            self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5)

    async def load_active_configs(self) -> list[ListenerConfig]:
        await self._ensure_pool()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM listeners WHERE is_active = true"
            )
        return [self._parse_config(dict(row)) for row in rows]

    async def load_config(self, listener_id: str) -> Optional[ListenerConfig]:
        await self._ensure_pool()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM listeners WHERE id = $1", listener_id
            )
        if row:
            return self._parse_config(dict(row))
        return None

    def _parse_config(self, row: dict) -> ListenerConfig:
        filters_data = row.get("filters", {})
        if isinstance(filters_data, str):
            filters_data = json.loads(filters_data)
        platform_str = row.get("platform", "polymarket")
        return ListenerConfig(
            id=str(row["id"]),
            name=row["name"],
            platform=Platform(platform_str),
            description=row.get("description"),
            filters=filters_data,  # Keep as dict, validated by discovery service
            discovery_interval_seconds=row.get("discovery_interval_seconds", 60),
            emit_interval_ms=row.get("emit_interval_ms", 100),
            enable_forward_fill=row.get("enable_forward_fill", False),
            is_active=row.get("is_active", True),
            created_at=self._parse_datetime(row.get("created_at")),
            updated_at=self._parse_datetime(row.get("updated_at")),
        )

    def _parse_datetime(self, value) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
