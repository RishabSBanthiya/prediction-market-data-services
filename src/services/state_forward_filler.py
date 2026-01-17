"""
StateForwardFiller: Emits continuous orderbook snapshots by forward-filling
the last known state between WebSocket events.

When a WebSocket event arrives, the state is updated immediately.
Between events, copies of the last state are emitted at regular intervals
with updated timestamps, creating a continuous stream for backtesting.
"""
import asyncio
import time
from typing import Optional, Callable, Awaitable
from dataclasses import dataclass, field
from copy import deepcopy

from models import OrderbookSnapshot


@dataclass
class TokenState:
    """Holds the last known orderbook state for a token."""
    token_id: str
    condition_id: str
    last_snapshot: Optional[OrderbookSnapshot] = None
    last_update_ms: int = 0


class StateForwardFiller:
    """
    Maintains last known orderbook state per token and emits continuous
    snapshots at a configurable interval.

    - Real WebSocket events update state immediately and trigger emission
    - Between events, forward-fills by emitting copies with current timestamp
    """

    def __init__(
        self,
        listener_id: str,
        logger,
        emit_interval_ms: int = 100,
    ):
        self._listener_id = listener_id
        self._logger = logger
        self._emit_interval_ms = emit_interval_ms
        self._tokens: dict[str, TokenState] = {}
        self._running = False
        self._emit_task: Optional[asyncio.Task] = None
        self._snapshot_callback: Optional[Callable[[OrderbookSnapshot], Awaitable[None]]] = None

    def set_snapshot_callback(self, callback: Callable[[OrderbookSnapshot], Awaitable[None]]) -> None:
        """Set callback for emitting snapshots."""
        self._snapshot_callback = callback

    def add_token(self, token_id: str, condition_id: str) -> None:
        """Start tracking a token for forward-filling."""
        if token_id not in self._tokens:
            self._tokens[token_id] = TokenState(token_id=token_id, condition_id=condition_id)
            self._logger.info("forward_filler_token_added", token_id=token_id[:20])

    def remove_token(self, token_id: str) -> None:
        """Stop tracking a token."""
        if token_id in self._tokens:
            del self._tokens[token_id]
            self._logger.info("forward_filler_token_removed", token_id=token_id[:20])

    def update_state(self, snapshot: OrderbookSnapshot) -> None:
        """
        Update state when a real WebSocket event arrives.
        This should be called for every orderbook event from the WebSocket.
        """
        token_id = snapshot.asset_id
        if token_id in self._tokens:
            self._tokens[token_id].last_snapshot = snapshot
            self._tokens[token_id].last_update_ms = int(time.time() * 1000)

    async def start(self) -> None:
        """Start the forward-fill emission loop."""
        self._running = True
        self._emit_task = asyncio.create_task(self._emit_loop())
        self._logger.info(
            "forward_filler_started",
            interval_ms=self._emit_interval_ms,
            listener_id=self._listener_id
        )

    async def stop(self) -> None:
        """Stop the forward-fill emission loop."""
        self._running = False
        if self._emit_task:
            self._emit_task.cancel()
            try:
                await self._emit_task
            except asyncio.CancelledError:
                pass
        self._logger.info("forward_filler_stopped", listener_id=self._listener_id)

    async def _emit_loop(self) -> None:
        """
        Main loop that emits snapshots at regular intervals.
        For each tracked token with state, emit a copy with current timestamp.
        """
        interval_seconds = self._emit_interval_ms / 1000.0

        while self._running:
            loop_start = time.time()

            for token_id, token_state in list(self._tokens.items()):
                if token_state.last_snapshot is None:
                    # No state yet for this token, skip
                    continue

                # Create a forward-filled copy with current timestamp
                filled_snapshot = self._create_forward_filled_snapshot(token_state.last_snapshot)

                if self._snapshot_callback:
                    try:
                        await self._snapshot_callback(filled_snapshot)
                    except Exception as e:
                        self._logger.error(
                            "forward_fill_emit_error",
                            token_id=token_id[:20],
                            error=str(e)
                        )

            # Sleep for remaining interval time
            elapsed = time.time() - loop_start
            sleep_time = max(0, interval_seconds - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    def _create_forward_filled_snapshot(self, original: OrderbookSnapshot) -> OrderbookSnapshot:
        """Create a copy of the snapshot with current timestamp, marked as forward-filled."""
        # Deep copy to avoid mutating original
        filled = OrderbookSnapshot(
            listener_id=original.listener_id,
            asset_id=original.asset_id,
            market=original.market,
            timestamp=int(time.time() * 1000),  # Current time in ms
            bids=deepcopy(original.bids),
            asks=deepcopy(original.asks),
            hash=original.hash,
            best_bid=original.best_bid,
            best_ask=original.best_ask,
            spread=original.spread,
            mid_price=original.mid_price,
            raw_payload=None,  # Don't copy raw payload for filled snapshots
            is_forward_filled=True,  # Mark as forward-filled
            source_timestamp=original.timestamp,  # Original event timestamp
        )
        return filled

    @property
    def tracked_token_count(self) -> int:
        """Number of tokens being tracked."""
        return len(self._tokens)

    @property
    def tokens_with_state(self) -> int:
        """Number of tokens that have received at least one snapshot."""
        return sum(1 for t in self._tokens.values() if t.last_snapshot is not None)
