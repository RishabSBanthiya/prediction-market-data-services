from dataclasses import dataclass
from typing import Union

from models.orderbook import OrderbookSnapshot
from models.trade import Trade
from models.market import Market


@dataclass
class OrderbookEvent:
    data: OrderbookSnapshot


@dataclass
class TradeEvent:
    data: Trade


@dataclass
class MarketDiscoveredEvent:
    market: Market


@dataclass
class MarketClosedEvent:
    market: Market
    new_state: str


@dataclass
class ConnectionLostEvent:
    reason: str


@dataclass
class ShutdownEvent:
    pass


ListenerEvent = Union[
    OrderbookEvent,
    TradeEvent,
    MarketDiscoveredEvent,
    MarketClosedEvent,
    ConnectionLostEvent,
    ShutdownEvent,
]
