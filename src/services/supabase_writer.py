import asyncio
from typing import Optional

from core.interfaces import IDataWriter
from models import OrderbookSnapshot, Trade, Market


class SupabaseWriter(IDataWriter):
    BATCH_SIZE = 100
    FLUSH_INTERVAL = 1.0

    def __init__(self, client, listener_id: str, logger, platform: str = "polymarket"):
        self._client = client
        self._listener_id = listener_id
        self._logger = logger
        self._platform = platform
        self._orderbook_buffer: list[dict] = []
        self._trade_buffer: list[dict] = []
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
        self._schema_has_forward_fill: bool = True  # Will be set to False if columns missing
        self._schema_has_platform: bool = True  # Will be set to False if column missing
        self._known_markets: set[str] = set()  # Track markets written to DB

    async def start(self) -> None:
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
        await self.flush()

    async def write_orderbook(self, snapshot: OrderbookSnapshot) -> None:
        # Skip if market not yet written to DB
        if snapshot.asset_id not in self._known_markets:
            self._logger.debug(
                "orderbook_skipped_unknown_market",
                asset_id=snapshot.asset_id[:20] if snapshot.asset_id else "none",
            )
            return
        record = {
            "listener_id": self._listener_id,
            "asset_id": snapshot.asset_id,
            "market": snapshot.market,
            "timestamp": snapshot.timestamp,
            "bids": [{"price": b.price, "size": b.size} for b in snapshot.bids],
            "asks": [{"price": a.price, "size": a.size} for a in snapshot.asks],
            "best_bid": snapshot.best_bid,
            "best_ask": snapshot.best_ask,
            "spread": snapshot.spread,
            "mid_price": snapshot.mid_price,
            "bid_depth": snapshot.bid_depth,
            "ask_depth": snapshot.ask_depth,
            "hash": snapshot.hash,
            "raw_payload": snapshot.raw_payload,
        }
        # Only include forward-fill fields if schema supports them
        if self._schema_has_forward_fill:
            record["is_forward_filled"] = snapshot.is_forward_filled
            record["source_timestamp"] = snapshot.source_timestamp
        # Only include platform if schema supports it
        if self._schema_has_platform:
            record["platform"] = self._platform
        self._orderbook_buffer.append(record)
        if len(self._orderbook_buffer) >= self.BATCH_SIZE:
            await self._flush_orderbooks()

    async def write_trade(self, trade: Trade) -> None:
        # Skip if market not yet written to DB
        if trade.asset_id not in self._known_markets:
            self._logger.debug(
                "trade_skipped_unknown_market",
                asset_id=trade.asset_id[:20] if trade.asset_id else "none",
            )
            return
        record = {
            "listener_id": self._listener_id,
            "asset_id": trade.asset_id,
            "market": trade.market,
            "timestamp": trade.timestamp,
            "price": float(trade.price),
            "size": float(trade.size),
            "side": trade.side,
            "fee_rate_bps": trade.fee_rate_bps,
            "raw_payload": trade.raw_payload,
        }
        if self._schema_has_platform:
            record["platform"] = self._platform
        self._trade_buffer.append(record)
        if len(self._trade_buffer) >= self.BATCH_SIZE:
            await self._flush_trades()

    async def write_market(self, market: Market) -> None:
        try:
            data = {
                "listener_id": self._listener_id,
                "condition_id": market.condition_id,
                "token_id": market.token_id,
                "market_slug": market.market_slug,
                "event_slug": market.event_slug,
                "question": market.question,
                "outcome": market.outcome,
                "outcome_index": market.outcome_index,
                "event_id": market.event_id,
                "event_title": market.event_title,
                "category": market.category,
                "subcategory": market.subcategory,
                "series_id": market.series_id,
                "tags": market.tags,
                "description": market.description,
                "volume": float(market.volume) if market.volume else None,
                "liquidity": float(market.liquidity) if market.liquidity else None,
                "is_active": market.is_active,
                "is_closed": market.is_closed,
                "state": market.state.value if market.state else None,
            }
            if self._schema_has_platform:
                data["platform"] = self._platform
            self._client.table("markets").upsert(
                data, on_conflict="listener_id,token_id"
            ).execute()
            # Track this market as known so orderbooks/trades can be written
            self._known_markets.add(market.token_id)
        except Exception as e:
            error_str = str(e)
            if "platform" in error_str and self._schema_has_platform:
                self._schema_has_platform = False
                data.pop("platform", None)
                try:
                    self._client.table("markets").upsert(
                        data, on_conflict="listener_id,token_id"
                    ).execute()
                    self._known_markets.add(market.token_id)
                    return
                except Exception as retry_error:
                    self._logger.error("write_market_retry_failed", error=str(retry_error))
            self._logger.error("write_market_failed", error=error_str)

    async def write_state_transition(
        self, market_id: str, old_state: Optional[str], new_state: str, metadata: dict
    ) -> None:
        try:
            self._client.table("market_state_history").insert({
                "listener_id": self._listener_id,
                "condition_id": market_id,
                "previous_state": old_state,
                "new_state": new_state,
                "metadata": metadata,
            }).execute()
        except Exception as e:
            self._logger.error("write_state_transition_failed", error=str(e))

    async def flush(self) -> None:
        await self._flush_orderbooks()
        await self._flush_trades()

    async def _flush_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.FLUSH_INTERVAL)
            await self.flush()

    async def _flush_orderbooks(self) -> None:
        if not self._orderbook_buffer:
            return
        buffer = self._orderbook_buffer
        self._orderbook_buffer = []
        try:
            self._client.table("orderbook_snapshots").insert(buffer).execute()
            self._logger.debug("flushed_orderbooks", count=len(buffer))
        except Exception as e:
            error_str = str(e)
            needs_retry = False
            # Handle missing columns by retrying without them
            if "is_forward_filled" in error_str or "source_timestamp" in error_str:
                self._logger.warning("forward_fill_columns_missing", msg="Retrying without forward-fill columns")
                self._schema_has_forward_fill = False
                for record in buffer:
                    record.pop("is_forward_filled", None)
                    record.pop("source_timestamp", None)
                needs_retry = True
            if "platform" in error_str:
                self._logger.warning("platform_column_missing", msg="Retrying without platform column")
                self._schema_has_platform = False
                for record in buffer:
                    record.pop("platform", None)
                needs_retry = True

            # Handle FK constraint violations - drop records for unknown markets
            if "foreign key constraint" in error_str.lower() or "23503" in error_str:
                self._logger.warning(
                    "orderbook_fk_violation",
                    msg="Dropping records for unknown markets",
                    count=len(buffer),
                )
                # Don't retry - these records reference markets that don't exist
                return

            if needs_retry:
                try:
                    self._client.table("orderbook_snapshots").insert(buffer).execute()
                    self._logger.debug("flushed_orderbooks", count=len(buffer))
                    return
                except Exception as retry_error:
                    retry_str = str(retry_error)
                    # If retry also fails with FK violation, just drop the records
                    if "foreign key constraint" in retry_str.lower() or "23503" in retry_str:
                        self._logger.warning("orderbook_fk_violation_on_retry", count=len(buffer))
                        return
                    self._logger.error("flush_orderbooks_retry_failed", error=retry_str)
            else:
                self._logger.error("flush_orderbooks_failed", error=error_str)
            # Only re-add buffer for non-FK errors (transient failures)
            self._orderbook_buffer = buffer + self._orderbook_buffer

    async def _flush_trades(self) -> None:
        if not self._trade_buffer:
            return
        buffer = self._trade_buffer
        self._trade_buffer = []
        try:
            self._client.table("trades").insert(buffer).execute()
            self._logger.debug("flushed_trades", count=len(buffer))
        except Exception as e:
            error_str = str(e)

            # Handle FK constraint violations - drop records for unknown markets
            if "foreign key constraint" in error_str.lower() or "23503" in error_str:
                self._logger.warning(
                    "trades_fk_violation",
                    msg="Dropping records for unknown markets",
                    count=len(buffer),
                )
                return

            if "platform" in error_str and self._schema_has_platform:
                self._logger.warning("platform_column_missing_trades", msg="Retrying without platform column")
                self._schema_has_platform = False
                for record in buffer:
                    record.pop("platform", None)
                try:
                    self._client.table("trades").insert(buffer).execute()
                    self._logger.debug("flushed_trades", count=len(buffer))
                    return
                except Exception as retry_error:
                    retry_str = str(retry_error)
                    if "foreign key constraint" in retry_str.lower() or "23503" in retry_str:
                        self._logger.warning("trades_fk_violation_on_retry", count=len(buffer))
                        return
                    self._logger.error("flush_trades_retry_failed", error=retry_str)
            else:
                self._logger.error("flush_trades_failed", error=error_str)
            # Only re-add buffer for non-FK errors (transient failures)
            self._trade_buffer = buffer + self._trade_buffer
