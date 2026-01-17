import asyncio
import json
import ssl
import certifi
from typing import Optional, AsyncIterator, Callable

import websockets

from core.interfaces import IWebSocketClient, IConnectionManager


class PolymarketWebSocketClient(IWebSocketClient):
    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    PING_INTERVAL = 5

    def __init__(self, logger):
        self._logger = logger
        self._ws = None
        self._subscribed_tokens: set[str] = set()
        self._message_queue: asyncio.Queue[dict] = asyncio.Queue()
        self._running = False
        self._receive_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._connection_manager = ConnectionManager(self, logger)

    async def connect(self) -> None:
        await self._connection_manager.connect_with_retry()

    async def _do_connect(self) -> None:
        self._logger.info("websocket_connecting", url=self.WS_URL)
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        self._ws = await websockets.connect(self.WS_URL, ssl=ssl_context)
        self._running = True
        self._receive_task = asyncio.create_task(self._receive_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())
        if self._subscribed_tokens:
            await self._send_subscription(list(self._subscribed_tokens))
        self._logger.info("websocket_connected")

    async def disconnect(self) -> None:
        self._running = False
        if self._receive_task:
            self._receive_task.cancel()
        if self._ping_task:
            self._ping_task.cancel()
        if self._ws:
            await self._ws.close()
            self._ws = None
        self._logger.info("websocket_disconnected")

    async def subscribe(self, token_ids: list[str]) -> None:
        new_tokens = set(token_ids) - self._subscribed_tokens
        if not new_tokens:
            return
        await self._send_subscription(list(new_tokens))
        self._subscribed_tokens.update(new_tokens)
        self._logger.info("websocket_subscribed", count=len(new_tokens))

    async def unsubscribe(self, token_ids: list[str]) -> None:
        tokens_to_remove = set(token_ids) & self._subscribed_tokens
        if not tokens_to_remove:
            return
        if self._ws:
            msg = {"assets_ids": list(tokens_to_remove), "type": "market", "action": "unsubscribe"}
            await self._ws.send(json.dumps(msg))
        self._subscribed_tokens -= tokens_to_remove
        self._logger.info("websocket_unsubscribed", count=len(tokens_to_remove))

    async def events(self) -> AsyncIterator[dict]:
        while self._running:
            try:
                event = await asyncio.wait_for(self._message_queue.get(), timeout=1.0)
                yield event
            except asyncio.TimeoutError:
                continue

    async def _send_subscription(self, token_ids: list[str]) -> None:
        if not self._ws:
            return
        msg = {"assets_ids": token_ids, "type": "market"}
        await self._ws.send(json.dumps(msg))

    async def _receive_loop(self) -> None:
        while self._running and self._ws:
            try:
                message = await self._ws.recv()
                data = json.loads(message)
                await self._message_queue.put(data)
            except websockets.ConnectionClosed as e:
                self._logger.warning("websocket_connection_closed", code=e.code)
                await self._connection_manager.handle_disconnect()
                break
            except Exception as e:
                self._logger.error("websocket_receive_error", error=str(e))

    async def _ping_loop(self) -> None:
        while self._running and self._ws:
            try:
                await asyncio.sleep(self.PING_INTERVAL)
                await self._ws.ping()
            except Exception as e:
                self._logger.warning("websocket_ping_failed", error=str(e))


class ConnectionManager(IConnectionManager):
    def __init__(self, client: PolymarketWebSocketClient, logger):
        self._client = client
        self._logger = logger
        self._disconnect_callbacks: list[Callable] = []
        self._reconnect_delay = 1
        self._max_delay = 60

    async def connect_with_retry(self) -> None:
        while True:
            try:
                await self._client._do_connect()
                self._reconnect_delay = 1
                return
            except Exception as e:
                self._logger.error("websocket_connection_failed", error=str(e))
                self._logger.info("websocket_retry", delay=self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_delay)

    async def handle_disconnect(self) -> None:
        for callback in self._disconnect_callbacks:
            try:
                await callback("Connection lost")
            except Exception:
                pass
        await self.connect_with_retry()

    def on_disconnect(self, callback: Callable) -> None:
        self._disconnect_callbacks.append(callback)
