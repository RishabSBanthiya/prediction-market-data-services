from datetime import datetime, timezone
from typing import Optional

from models import ListenerConfig, ListenerFilters


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
        return ListenerConfig(
            id=str(row["id"]),
            name=row["name"],
            description=row.get("description"),
            filters=ListenerFilters(**filters_data),
            discovery_interval_seconds=row.get("discovery_interval_seconds", 60),
            emit_interval_ms=row.get("emit_interval_ms", 100),  # Default 100ms
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
