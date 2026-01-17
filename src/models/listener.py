from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


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
    description: Optional[str] = None
    filters: ListenerFilters
    discovery_interval_seconds: int = 60
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
