"""
Kalshi WebSocket Client for real-time orderbook and trade data.

Key differences from Polymarket:
- Requires RSA authentication before subscribing
- Uses snapshot + delta model for orderbook (must maintain state)
- Timestamps in seconds (converted to ms internally)
- Prices in cents (0-100), converted to decimals

The client normalizes all events to match the Polymarket schema so the
existing Listener class can process them without changes.
"""
import asyncio
import json
import logging
import ssl
import time
import certifi
from dataclasses import dataclass, field
from typing import Optional, AsyncIterator, Callable

import websockets

from core.interfaces import IWebSocketClient, IConnectionManager
from services.kalshi_auth import KalshiAuthenticator

# Reduce websockets library logging noise
logging.getLogger("websockets").setLevel(logging.WARNING)


@dataclass
class KalshiOrderbookState:
    """
    Maintains orderbook state from Kalshi snapshots and deltas.

    Kalshi orderbooks have yes/no sides instead of bids/asks.
    We normalize to the standard bids/asks format:
    - YES levels become bids (buying YES)
    - NO levels at price P become asks at price (100-P)
    """

    ticker: str
    sequence: int = 0
    # yes side: price_cents -> quantity
    yes_levels: dict[int, int] = field(default_factory=dict)
    # no side: price_cents -> quantity
    no_levels: dict[int, int] = field(default_factory=dict)

    def apply_snapshot(self, yes_levels: list, no_levels: list, seq: int) -> None:
        """Apply full orderbook snapshot."""
        self.sequence = seq
        self.yes_levels = {level[0]: level[1] for level in yes_levels}
        self.no_levels = {level[0]: level[1] for level in no_levels}

    def apply_delta(self, price: int, delta: int, side: str, seq: int) -> bool:
        """
        Apply incremental delta update.

        Returns True if update was applied, False if stale.
        """
        if seq <= self.sequence:
            return False  # Stale update

        self.sequence = seq
        levels = self.yes_levels if side == "yes" else self.no_levels
        current = levels.get(price, 0)
        new_qty = current + delta

        if new_qty <= 0:
            levels.pop(price, None)
        else:
            levels[price] = new_qty

        return True

    def to_normalized_event(self, timestamp_ms: int) -> dict:
        """
        Convert to normalized event format matching Polymarket schema.

        Kalshi yes/no -> bids/asks mapping:
        - YES bids at price P (in cents) -> bids at price P/100
        - NO bids at price P -> YES asks at (100-P)/100
        """
        # Convert yes_levels to bids (buying YES)
        # Sorted descending by price (best bid first)
        bids = []
        for price_cents, qty in sorted(self.yes_levels.items(), reverse=True):
            bids.append({
                "price": f"{price_cents / 100:.2f}",
                "size": str(qty),
            })

        # Convert no_levels to asks (selling YES = buying NO)
        # NO at price P means YES ask at (100-P)
        # Sorted ascending by price (best ask first)
        asks = []
        for price_cents, qty in sorted(self.no_levels.items()):
            yes_price = 100 - price_cents
            asks.append({
                "price": f"{yes_price / 100:.2f}",
                "size": str(qty),
            })

        return {
            "event_type": "book",
            "asset_id": self.ticker,
            "market": self.ticker,
            "timestamp": timestamp_ms,
            "bids": bids,
            "asks": asks,
        }


class KalshiWebSocketClient(IWebSocketClient):
    """
    WebSocket client for Kalshi real-time data.

    Handles:
    - RSA authentication on connect
    - Orderbook snapshot/delta reconstruction
    - Event normalization to Polymarket format
    - Auto-reconnection with exponential backoff
    """

    WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    PING_INTERVAL = 10  # Kalshi sends pings every 10 seconds

    def __init__(self, logger, authenticator: KalshiAuthenticator):
        self._logger = logger
        self._authenticator = authenticator
        self._ws = None
        self._subscribed_tickers: set[str] = set()
        self._subscription_ids: dict[str, int] = {}  # ticker -> sid
        self._message_queue: asyncio.Queue[dict] = asyncio.Queue()
        self._running = False
        self._authenticated = False
        self._receive_task: Optional[asyncio.Task] = None
        self._orderbook_state: dict[str, KalshiOrderbookState] = {}
        self._connection_manager = KalshiConnectionManager(self, logger)
        self._msg_id_counter = 1

    async def connect(self) -> None:
        """Connect to WebSocket with retry logic."""
        await self._connection_manager.connect_with_retry()

    async def _do_connect(self) -> None:
        """Internal connection logic."""
        self._logger.info("kalshi_websocket_connecting", url=self.WS_URL)
        ssl_context = ssl.create_default_context(cafile=certifi.where())

        # Generate authentication headers for WebSocket handshake
        auth_headers = self._authenticator.generate_ws_headers()
        self._logger.debug("kalshi_ws_auth_headers", api_key=auth_headers.get("KALSHI-ACCESS-KEY", "")[:8] + "...")

        self._ws = await websockets.connect(
            self.WS_URL,
            ssl=ssl_context,
            additional_headers=auth_headers,
        )
        self._running = True
        self._authenticated = True

        self._receive_task = asyncio.create_task(self._receive_loop())

        # Resubscribe to existing tickers on reconnect
        if self._subscribed_tickers:
            self._logger.info(
                "kalshi_resubscribing",
                count=len(self._subscribed_tickers),
            )
            await self._send_subscriptions(list(self._subscribed_tickers))

        self._logger.info("kalshi_websocket_connected")

    async def disconnect(self) -> None:
        """Disconnect and cleanup."""
        self._running = False
        self._authenticated = False
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
            self._ws = None
        self._logger.info("kalshi_websocket_disconnected")

    async def subscribe(self, token_ids: list[str]) -> None:
        """Subscribe to market tickers."""
        new_tickers = set(token_ids) - self._subscribed_tickers
        if not new_tickers:
            return

        await self._send_subscriptions(list(new_tickers))
        self._subscribed_tickers.update(new_tickers)

        # Initialize orderbook state for each ticker
        for ticker in new_tickers:
            self._orderbook_state[ticker] = KalshiOrderbookState(ticker=ticker)

        self._logger.info("kalshi_subscribed", count=len(new_tickers))

    async def unsubscribe(self, token_ids: list[str]) -> None:
        """Unsubscribe from market tickers."""
        tickers_to_remove = set(token_ids) & self._subscribed_tickers
        if not tickers_to_remove:
            return

        await self._send_unsubscriptions(list(tickers_to_remove))
        self._subscribed_tickers -= tickers_to_remove

        # Clean up orderbook state
        for ticker in tickers_to_remove:
            self._orderbook_state.pop(ticker, None)
            self._subscription_ids.pop(ticker, None)

        self._logger.info("kalshi_unsubscribed", count=len(tickers_to_remove))

    async def events(self) -> AsyncIterator[dict]:
        """Yield normalized events compatible with existing Listener."""
        while self._running:
            try:
                event = await asyncio.wait_for(
                    self._message_queue.get(),
                    timeout=1.0,
                )
                yield event
            except asyncio.TimeoutError:
                continue

    async def _send_subscriptions(self, tickers: list[str]) -> None:
        """Send subscription messages for orderbook_delta and trade channels."""
        if not self._ws or not self._authenticated:
            return

        for ticker in tickers:
            # Subscribe to orderbook_delta channel
            msg = {
                "id": self._msg_id_counter,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_ticker": ticker,
                },
            }
            self._msg_id_counter += 1
            await self._ws.send(json.dumps(msg))

            # Subscribe to trade channel
            msg = {
                "id": self._msg_id_counter,
                "cmd": "subscribe",
                "params": {
                    "channels": ["trade"],
                    "market_ticker": ticker,
                },
            }
            self._msg_id_counter += 1
            await self._ws.send(json.dumps(msg))

    async def _send_unsubscriptions(self, tickers: list[str]) -> None:
        """Send unsubscription messages."""
        if not self._ws or not self._authenticated:
            return

        for ticker in tickers:
            # Get subscription IDs for this ticker
            for channel in ["orderbook_delta", "trade"]:
                msg = {
                    "id": self._msg_id_counter,
                    "cmd": "unsubscribe",
                    "params": {
                        "channels": [channel],
                        "market_ticker": ticker,
                    },
                }
                self._msg_id_counter += 1
                await self._ws.send(json.dumps(msg))

    async def _receive_loop(self) -> None:
        """Process incoming WebSocket messages."""
        self._logger.info("kalshi_receive_loop_started")

        while self._running and self._ws:
            try:
                async for message in self._ws:
                    if not self._running:
                        break

                    try:
                        data = json.loads(message)
                        await self._handle_message(data)
                    except json.JSONDecodeError:
                        pass

            except websockets.ConnectionClosed as e:
                self._logger.warning("kalshi_connection_closed", code=e.code)
                self._authenticated = False
                await self._connection_manager.handle_disconnect()
                break
            except Exception as e:
                self._logger.error("kalshi_receive_error", error=str(e))
                await asyncio.sleep(1)

    async def _handle_message(self, data: dict) -> None:
        """Route message to appropriate handler."""
        msg_type = data.get("type")

        if msg_type == "orderbook_snapshot":
            await self._handle_orderbook_snapshot(data)
        elif msg_type == "orderbook_delta":
            await self._handle_orderbook_delta(data)
        elif msg_type == "trade":
            await self._handle_trade(data)
        elif msg_type == "subscribed":
            # Track subscription ID
            sid = data.get("sid")
            msg = data.get("msg", {})
            channel = msg.get("channel")
            ticker = msg.get("market_ticker")
            self._logger.debug(
                "kalshi_subscription_confirmed",
                channel=channel,
                ticker=ticker,
                sid=sid,
            )
        elif msg_type == "error":
            self._logger.error("kalshi_ws_error", error=data.get("msg"))

    async def _handle_orderbook_snapshot(self, data: dict) -> None:
        """Process full orderbook snapshot."""
        msg = data.get("msg", {})
        ticker = msg.get("market_ticker")
        seq = data.get("seq", 0)

        if ticker not in self._orderbook_state:
            # Auto-initialize if we receive snapshot for unknown ticker
            self._orderbook_state[ticker] = KalshiOrderbookState(ticker=ticker)

        state = self._orderbook_state[ticker]
        state.apply_snapshot(
            yes_levels=msg.get("yes", []),
            no_levels=msg.get("no", []),
            seq=seq,
        )

        # Emit normalized event
        timestamp_ms = self._get_timestamp_ms(data)
        event = state.to_normalized_event(timestamp_ms)
        await self._message_queue.put(event)

        self._logger.debug(
            "kalshi_orderbook_snapshot",
            ticker=ticker,
            yes_levels=len(state.yes_levels),
            no_levels=len(state.no_levels),
        )

    async def _handle_orderbook_delta(self, data: dict) -> None:
        """Process incremental orderbook update."""
        msg = data.get("msg", {})
        ticker = msg.get("market_ticker")
        seq = data.get("seq", 0)

        if ticker not in self._orderbook_state:
            self._logger.warning(
                "kalshi_delta_without_snapshot",
                ticker=ticker,
            )
            return

        state = self._orderbook_state[ticker]
        applied = state.apply_delta(
            price=msg.get("price", 0),
            delta=msg.get("delta", 0),
            side=msg.get("side", "yes"),
            seq=seq,
        )

        if not applied:
            return  # Stale update

        # Emit normalized event
        timestamp_ms = self._get_timestamp_ms(data)
        event = state.to_normalized_event(timestamp_ms)
        await self._message_queue.put(event)

    async def _handle_trade(self, data: dict) -> None:
        """Process trade event."""
        msg = data.get("msg", {})
        ticker = msg.get("market_ticker")

        # Convert to normalized trade event
        # Kalshi timestamps are in seconds
        timestamp_ms = self._get_timestamp_ms(data)

        # Kalshi provides yes_price in cents
        yes_price_cents = msg.get("yes_price", 0)
        price_decimal = yes_price_cents / 100

        event = {
            "event_type": "last_trade_price",
            "asset_id": ticker,
            "market": ticker,
            "timestamp": timestamp_ms,
            "price": str(price_decimal),
            "size": str(msg.get("count", 0)),
            "side": msg.get("taker_side", "").upper(),  # Normalize to BUY/SELL style
        }

        await self._message_queue.put(event)

        self._logger.debug(
            "kalshi_trade",
            ticker=ticker,
            price=price_decimal,
            size=msg.get("count"),
        )

    def _get_timestamp_ms(self, data: dict) -> int:
        """Extract timestamp from message, converting to milliseconds if needed."""
        # Try ts field (Kalshi uses seconds)
        ts = data.get("ts") or data.get("msg", {}).get("ts")
        if ts:
            # Kalshi timestamps are in seconds
            return int(ts * 1000)
        # Fallback to current time
        return int(time.time() * 1000)


class KalshiConnectionManager(IConnectionManager):
    """Connection manager with exponential backoff for Kalshi WebSocket."""

    def __init__(self, client: KalshiWebSocketClient, logger):
        self._client = client
        self._logger = logger
        self._disconnect_callbacks: list[Callable] = []
        self._reconnect_delay = 1
        self._max_delay = 60

    async def connect_with_retry(self) -> None:
        """Connect with exponential backoff on failure."""
        while True:
            try:
                await self._client._do_connect()
                self._reconnect_delay = 1  # Reset on success
                return
            except Exception as e:
                self._logger.error("kalshi_connection_failed", error=str(e))
                self._logger.info("kalshi_retry", delay=self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_delay)

    async def handle_disconnect(self) -> None:
        """Handle disconnection and reconnect."""
        for callback in self._disconnect_callbacks:
            try:
                await callback("Connection lost")
            except Exception:
                pass
        await self.connect_with_retry()

    def on_disconnect(self, callback: Callable) -> None:
        """Register a disconnect callback."""
        self._disconnect_callbacks.append(callback)
