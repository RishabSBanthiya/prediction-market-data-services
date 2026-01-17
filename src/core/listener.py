import asyncio
from datetime import datetime
from typing import Optional

from core.interfaces import IMarketDiscovery, IWebSocketClient, IDataWriter
from core.events import (
    OrderbookEvent, TradeEvent, MarketDiscoveredEvent,
    MarketClosedEvent, ShutdownEvent, ListenerEvent
)
from models import ListenerConfig, Market, MarketState, OrderbookSnapshot, OrderLevel, Trade


class ListenerState:
    def __init__(self):
        self.is_running: bool = False
        self.subscribed_markets: dict[str, Market] = {}
        self.last_discovery_at: Optional[datetime] = None
        self.events_processed: int = 0
        self.errors_count: int = 0


class Listener:
    def __init__(
        self,
        config: ListenerConfig,
        discovery: IMarketDiscovery,
        websocket: IWebSocketClient,
        writer: IDataWriter,
        logger,
    ):
        self._config = config
        self._discovery = discovery
        self._websocket = websocket
        self._writer = writer
        self._logger = logger
        self._state = ListenerState()
        self._event_queue: asyncio.Queue[ListenerEvent] = asyncio.Queue()
        self._discovery_task: Optional[asyncio.Task] = None
        self._processor_task: Optional[asyncio.Task] = None
        self._websocket_task: Optional[asyncio.Task] = None

    @property
    def config(self) -> ListenerConfig:
        return self._config

    @property
    def state(self) -> ListenerState:
        return self._state

    @property
    def listener_id(self) -> str:
        return self._config.id

    async def start(self) -> None:
        self._logger.info("listener_starting", name=self._config.name)
        self._state.is_running = True
        self._discovery_task = asyncio.create_task(self._run_discovery_loop())
        self._processor_task = asyncio.create_task(self._run_event_processor())
        self._websocket_task = asyncio.create_task(self._run_websocket_listener())

    async def stop(self) -> None:
        self._logger.info("listener_stopping", name=self._config.name)
        self._state.is_running = False
        await self._event_queue.put(ShutdownEvent())
        for task in [self._discovery_task, self._processor_task, self._websocket_task]:
            if task:
                task.cancel()
        await self._websocket.disconnect()
        await self._discovery.close()
        await self._writer.flush()

    async def _run_discovery_loop(self) -> None:
        while self._state.is_running:
            try:
                await self._discover_and_sync_markets()
                self._state.last_discovery_at = datetime.utcnow()
            except Exception as e:
                self._logger.error("discovery_error", error=str(e))
                self._state.errors_count += 1
            await asyncio.sleep(self._config.discovery_interval_seconds)

    async def _discover_and_sync_markets(self) -> None:
        discovered = await self._discovery.discover_markets(self._config.filters.model_dump())
        discovered_by_token = {m.token_id: m for m in discovered}
        current_tokens = set(self._state.subscribed_markets.keys())
        discovered_tokens = set(discovered_by_token.keys())

        new_tokens = discovered_tokens - current_tokens
        removed_tokens = current_tokens - discovered_tokens

        for token_id in new_tokens:
            market = discovered_by_token[token_id]
            market.listener_id = self.listener_id
            await self._event_queue.put(MarketDiscoveredEvent(market=market))

        for token_id in removed_tokens:
            market = self._state.subscribed_markets[token_id]
            await self._event_queue.put(MarketClosedEvent(market=market, new_state=MarketState.CLOSED.value))

    async def _run_websocket_listener(self) -> None:
        await self._websocket.connect()
        async for raw_event in self._websocket.events():
            if not self._state.is_running:
                break
            event = self._parse_websocket_event(raw_event)
            if event:
                await self._event_queue.put(event)

    def _parse_websocket_event(self, raw: dict) -> Optional[ListenerEvent]:
        event_type = raw.get("event_type")
        if event_type == "book":
            bids = [OrderLevel(price=b["price"], size=b["size"]) for b in raw.get("bids", [])]
            asks = [OrderLevel(price=a["price"], size=a["size"]) for a in raw.get("asks", [])]
            snapshot = OrderbookSnapshot(
                listener_id=self.listener_id,
                asset_id=raw.get("asset_id", ""),
                market=raw.get("market", ""),
                timestamp=raw.get("timestamp", 0),
                bids=bids,
                asks=asks,
                hash=raw.get("hash"),
                raw_payload=raw,
            )
            snapshot.compute_metrics()
            return OrderbookEvent(data=snapshot)
        elif event_type == "last_trade_price":
            trade = Trade(
                listener_id=self.listener_id,
                asset_id=raw.get("asset_id", ""),
                market=raw.get("market", ""),
                timestamp=raw.get("timestamp", 0),
                price=float(raw.get("price", 0)),
                size=float(raw.get("size", 0)),
                side=raw.get("side", ""),
                fee_rate_bps=raw.get("fee_rate_bps"),
                raw_payload=raw,
            )
            return TradeEvent(data=trade)
        return None

    async def _run_event_processor(self) -> None:
        while self._state.is_running:
            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self._handle_event(event)
                self._state.events_processed += 1
            except Exception as e:
                self._logger.error("event_processing_error", error=str(e))
                self._state.errors_count += 1

    async def _handle_event(self, event: ListenerEvent) -> None:
        if isinstance(event, OrderbookEvent):
            await self._writer.write_orderbook(event.data)
        elif isinstance(event, TradeEvent):
            await self._writer.write_trade(event.data)
        elif isinstance(event, MarketDiscoveredEvent):
            await self._handle_market_discovered(event.market)
        elif isinstance(event, MarketClosedEvent):
            await self._handle_market_closed(event.market, event.new_state)
        elif isinstance(event, ShutdownEvent):
            self._state.is_running = False

    async def _handle_market_discovered(self, market: Market) -> None:
        self._logger.info("market_discovered", question=market.question, token_id=market.token_id)
        market.state = MarketState.TRACKING
        await self._writer.write_market(market)
        await self._writer.write_state_transition(
            market_id=market.condition_id,
            old_state=None,
            new_state=MarketState.TRACKING.value,
            metadata={"question": market.question},
        )
        await self._websocket.subscribe([market.token_id])
        self._state.subscribed_markets[market.token_id] = market

    async def _handle_market_closed(self, market: Market, new_state: str) -> None:
        self._logger.info("market_closed", question=market.question, token_id=market.token_id)
        await self._writer.write_state_transition(
            market_id=market.condition_id,
            old_state=market.state.value if market.state else None,
            new_state=new_state,
            metadata={"final_prices": market.outcome_prices},
        )
        await self._websocket.unsubscribe([market.token_id])
        self._state.subscribed_markets.pop(market.token_id, None)
