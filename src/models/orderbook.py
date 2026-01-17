from typing import Optional
from pydantic import BaseModel


class OrderLevel(BaseModel):
    price: str
    size: str


class OrderbookSnapshot(BaseModel):
    listener_id: str
    asset_id: str
    market: str
    timestamp: int
    bids: list[OrderLevel]
    asks: list[OrderLevel]
    hash: Optional[str] = None
    raw_payload: Optional[dict] = None

    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    spread: Optional[float] = None
    mid_price: Optional[float] = None
    bid_depth: Optional[float] = None
    ask_depth: Optional[float] = None

    # Forward-fill metadata
    is_forward_filled: bool = False  # True if this is a forward-filled copy
    source_timestamp: Optional[int] = None  # Original event timestamp if forward-filled

    def compute_metrics(self) -> None:
        if self.bids:
            self.best_bid = float(self.bids[0].price)
            self.bid_depth = sum(float(b.size) for b in self.bids)
        if self.asks:
            self.best_ask = float(self.asks[0].price)
            self.ask_depth = sum(float(a.size) for a in self.asks)
        if self.best_bid and self.best_ask:
            self.spread = self.best_ask - self.best_bid
            self.mid_price = (self.best_bid + self.best_ask) / 2
