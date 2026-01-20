from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel


class MarketState(str, Enum):
    DISCOVERED = "discovered"
    TRACKING = "tracking"
    CLOSED = "closed"
    RESOLVED = "resolved"


class Market(BaseModel):
    id: Optional[str] = None
    listener_id: Optional[str] = None
    condition_id: str
    token_id: str
    market_slug: Optional[str] = None
    event_slug: Optional[str] = None
    question: Optional[str] = None
    outcome: Optional[str] = None
    outcome_index: Optional[int] = None
    event_id: Optional[str] = None
    event_title: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    series_id: Optional[str] = None
    tags: Optional[list] = None
    description: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    game_start_time: Optional[datetime] = None
    outcome_prices: Optional[dict] = None
    volume: Optional[float] = None
    liquidity: Optional[float] = None
    is_active: bool = True
    is_closed: bool = False
    state: MarketState = MarketState.DISCOVERED
