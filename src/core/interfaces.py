from abc import ABC, abstractmethod
from typing import AsyncIterator, Optional, Callable

from models import Market, OrderbookSnapshot, Trade


class IMarketDiscovery(ABC):
    @abstractmethod
    async def discover_markets(self, filters: dict) -> list[Market]:
        pass

    @abstractmethod
    async def get_market_details(self, condition_id: str) -> list[Market]:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class IWebSocketClient(ABC):
    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        pass

    @abstractmethod
    async def subscribe(self, token_ids: list[str]) -> None:
        pass

    @abstractmethod
    async def unsubscribe(self, token_ids: list[str]) -> None:
        pass

    @abstractmethod
    def events(self) -> AsyncIterator[dict]:
        pass


class IDataWriter(ABC):
    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

    @abstractmethod
    async def write_orderbook(self, snapshot: OrderbookSnapshot) -> None:
        pass

    @abstractmethod
    async def write_trade(self, trade: Trade) -> None:
        pass

    @abstractmethod
    async def write_market(self, market: Market) -> None:
        pass

    @abstractmethod
    async def write_state_transition(
        self, market_id: str, old_state: Optional[str], new_state: str, metadata: dict
    ) -> None:
        pass

    @abstractmethod
    async def flush(self) -> None:
        pass


class IConnectionManager(ABC):
    @abstractmethod
    async def connect_with_retry(self) -> None:
        pass

    @abstractmethod
    async def handle_disconnect(self) -> None:
        pass

    @abstractmethod
    def on_disconnect(self, callback: Callable) -> None:
        pass
