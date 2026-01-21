from datetime import datetime
from enum import Enum
from typing import Optional, Union
from pydantic import BaseModel, Field


class Platform(str, Enum):
    """Supported prediction market platforms."""
    POLYMARKET = "polymarket"
    KALSHI = "kalshi"


class ListenerFilters(BaseModel):
    series_ids: list[str] = Field(default_factory=list)
    tag_ids: list[int] = Field(default_factory=list)
    slug_patterns: list[str] = Field(default_factory=list)
    condition_ids: list[str] = Field(default_factory=list)
    min_liquidity: Optional[float] = None
    min_volume: Optional[float] = None


class ListenerConfig(BaseModel):
    id: str
    name: str
    platform: Platform = Platform.POLYMARKET
    description: Optional[str] = None
    filters: dict  # Platform-specific filters (validated by discovery service)
    discovery_interval_seconds: int = 60
    emit_interval_ms: int = 100  # Forward-fill emission interval (milliseconds)
    enable_forward_fill: bool = False  # Set to True to emit forward-filled snapshots
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
