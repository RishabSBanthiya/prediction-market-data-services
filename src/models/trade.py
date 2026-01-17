from typing import Optional
from pydantic import BaseModel


class Trade(BaseModel):
    listener_id: str
    asset_id: str
    market: str
    timestamp: int
    price: float
    size: float
    side: str
    fee_rate_bps: Optional[int] = None
    raw_payload: dict
