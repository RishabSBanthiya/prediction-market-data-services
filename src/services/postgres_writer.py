import asyncio
import json
from typing import Optional

import asyncpg

from core.interfaces import IDataWriter
from models import OrderbookSnapshot, Trade, Market


class PostgresWriter(IDataWriter):
    BATCH_SIZE = 100
    FLUSH_INTERVAL = 1.0

    def __init__(self, dsn: str, listener_id: str, logger, platform: str = "polymarket"):
        self._dsn = dsn
        self._listener_id = listener_id
        self._logger = logger
        self._platform = platform
        self._pool: Optional[asyncpg.Pool] = None
        self._orderbook_buffer: list[dict] = []
        self._trade_buffer: list[dict] = []
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
        self._schema_has_platform: bool = True  # Will be set to False if column missing

    async def start(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=2, max_size=10)
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())
        self._logger.info("postgres_writer_started")

    async def stop(self) -> None:
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
        await self.flush()
        if self._pool:
            await self._pool.close()
        self._logger.info("postgres_writer_stopped")

    async def write_orderbook(self, snapshot: OrderbookSnapshot) -> None:
        record = {
            "listener_id": self._listener_id,
            "asset_id": snapshot.asset_id,
            "market": snapshot.market,
            "timestamp": snapshot.timestamp,
            "bids": json.dumps([{"price": b.price, "size": b.size} for b in snapshot.bids]),
            "asks": json.dumps([{"price": a.price, "size": a.size} for a in snapshot.asks]),
            "best_bid": snapshot.best_bid,
            "best_ask": snapshot.best_ask,
            "spread": snapshot.spread,
            "mid_price": snapshot.mid_price,
            "bid_depth": snapshot.bid_depth,
            "ask_depth": snapshot.ask_depth,
            "hash": snapshot.hash,
            "raw_payload": json.dumps(snapshot.raw_payload) if snapshot.raw_payload else None,
            "is_forward_filled": snapshot.is_forward_filled,
            "source_timestamp": snapshot.source_timestamp,
            "platform": self._platform,
        }
        self._orderbook_buffer.append(record)
        if len(self._orderbook_buffer) >= self.BATCH_SIZE:
            await self._flush_orderbooks()

    async def write_trade(self, trade: Trade) -> None:
        self._trade_buffer.append({
            "listener_id": self._listener_id,
            "asset_id": trade.asset_id,
            "market": trade.market,
            "timestamp": trade.timestamp,
            "price": float(trade.price),
            "size": float(trade.size),
            "side": trade.side,
            "fee_rate_bps": trade.fee_rate_bps,
            "raw_payload": json.dumps(trade.raw_payload) if trade.raw_payload else None,
            "platform": self._platform,
        })
        if len(self._trade_buffer) >= self.BATCH_SIZE:
            await self._flush_trades()

    async def write_market(self, market: Market) -> None:
        if not self._pool:
            return
        try:
            async with self._pool.acquire() as conn:
                if self._schema_has_platform:
                    await conn.execute(
                        """
                        INSERT INTO markets (
                            listener_id, condition_id, token_id, market_slug, event_slug,
                            question, outcome, outcome_index, event_id, event_title,
                            category, subcategory, series_id, tags, description,
                            volume, liquidity, is_active, is_closed, state, platform
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                        ON CONFLICT (listener_id, token_id) DO UPDATE SET
                            condition_id = EXCLUDED.condition_id,
                            market_slug = EXCLUDED.market_slug,
                            event_slug = EXCLUDED.event_slug,
                            question = EXCLUDED.question,
                            outcome = EXCLUDED.outcome,
                            outcome_index = EXCLUDED.outcome_index,
                            event_id = EXCLUDED.event_id,
                            event_title = EXCLUDED.event_title,
                            category = EXCLUDED.category,
                            subcategory = EXCLUDED.subcategory,
                            series_id = EXCLUDED.series_id,
                            tags = EXCLUDED.tags,
                            description = EXCLUDED.description,
                            volume = EXCLUDED.volume,
                            liquidity = EXCLUDED.liquidity,
                            is_active = EXCLUDED.is_active,
                            is_closed = EXCLUDED.is_closed,
                            state = EXCLUDED.state,
                            platform = EXCLUDED.platform,
                            updated_at = NOW()
                        """,
                        self._listener_id,
                        market.condition_id,
                        market.token_id,
                        market.market_slug,
                        market.event_slug,
                        market.question,
                        market.outcome,
                        market.outcome_index,
                        market.event_id,
                        market.event_title,
                        market.category,
                        market.subcategory,
                        market.series_id,
                        json.dumps(market.tags) if market.tags else None,
                        market.description,
                        float(market.volume) if market.volume else None,
                        float(market.liquidity) if market.liquidity else None,
                        market.is_active,
                        market.is_closed,
                        market.state.value if market.state else None,
                        self._platform,
                    )
                else:
                    await conn.execute(
                        """
                        INSERT INTO markets (
                            listener_id, condition_id, token_id, market_slug, event_slug,
                            question, outcome, outcome_index, event_id, event_title,
                            category, subcategory, series_id, tags, description,
                            volume, liquidity, is_active, is_closed, state
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                        ON CONFLICT (listener_id, token_id) DO UPDATE SET
                            condition_id = EXCLUDED.condition_id,
                            market_slug = EXCLUDED.market_slug,
                            event_slug = EXCLUDED.event_slug,
                            question = EXCLUDED.question,
                            outcome = EXCLUDED.outcome,
                            outcome_index = EXCLUDED.outcome_index,
                            event_id = EXCLUDED.event_id,
                            event_title = EXCLUDED.event_title,
                            category = EXCLUDED.category,
                            subcategory = EXCLUDED.subcategory,
                            series_id = EXCLUDED.series_id,
                            tags = EXCLUDED.tags,
                            description = EXCLUDED.description,
                            volume = EXCLUDED.volume,
                            liquidity = EXCLUDED.liquidity,
                            is_active = EXCLUDED.is_active,
                            is_closed = EXCLUDED.is_closed,
                            state = EXCLUDED.state,
                            updated_at = NOW()
                        """,
                        self._listener_id,
                        market.condition_id,
                        market.token_id,
                        market.market_slug,
                        market.event_slug,
                        market.question,
                        market.outcome,
                        market.outcome_index,
                        market.event_id,
                        market.event_title,
                        market.category,
                        market.subcategory,
                        market.series_id,
                        json.dumps(market.tags) if market.tags else None,
                        market.description,
                        float(market.volume) if market.volume else None,
                        float(market.liquidity) if market.liquidity else None,
                        market.is_active,
                        market.is_closed,
                        market.state.value if market.state else None,
                    )
        except Exception as e:
            error_str = str(e)
            if "platform" in error_str and self._schema_has_platform:
                self._schema_has_platform = False
                await self.write_market(market)  # Retry without platform
            else:
                self._logger.error("write_market_failed", error=error_str)

    async def write_state_transition(
        self, market_id: str, old_state: Optional[str], new_state: str, metadata: dict
    ) -> None:
        if not self._pool:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO market_state_history (listener_id, condition_id, previous_state, new_state, metadata)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    self._listener_id,
                    market_id,
                    old_state,
                    new_state,
                    json.dumps(metadata),
                )
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
        if not self._orderbook_buffer or not self._pool:
            return
        buffer = self._orderbook_buffer
        self._orderbook_buffer = []
        try:
            async with self._pool.acquire() as conn:
                if self._schema_has_platform:
                    await conn.executemany(
                        """
                        INSERT INTO orderbook_snapshots (
                            listener_id, asset_id, market, timestamp, bids, asks,
                            best_bid, best_ask, spread, mid_price, bid_depth, ask_depth,
                            hash, raw_payload, is_forward_filled, source_timestamp, platform
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                        """,
                        [
                            (
                                r["listener_id"], r["asset_id"], r["market"], r["timestamp"],
                                r["bids"], r["asks"], r["best_bid"], r["best_ask"],
                                r["spread"], r["mid_price"], r["bid_depth"], r["ask_depth"],
                                r["hash"], r["raw_payload"], r["is_forward_filled"], r["source_timestamp"],
                                r["platform"]
                            )
                            for r in buffer
                        ]
                    )
                else:
                    await conn.executemany(
                        """
                        INSERT INTO orderbook_snapshots (
                            listener_id, asset_id, market, timestamp, bids, asks,
                            best_bid, best_ask, spread, mid_price, bid_depth, ask_depth,
                            hash, raw_payload, is_forward_filled, source_timestamp
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                        """,
                        [
                            (
                                r["listener_id"], r["asset_id"], r["market"], r["timestamp"],
                                r["bids"], r["asks"], r["best_bid"], r["best_ask"],
                                r["spread"], r["mid_price"], r["bid_depth"], r["ask_depth"],
                                r["hash"], r["raw_payload"], r["is_forward_filled"], r["source_timestamp"]
                            )
                            for r in buffer
                        ]
                    )
            self._logger.debug("flushed_orderbooks", count=len(buffer))
        except Exception as e:
            error_str = str(e)
            # FK violations mean we got data for unknown markets - just drop those records
            if "foreign key constraint" in error_str:
                self._logger.warning("flush_orderbooks_fk_violation", dropped=len(buffer))
                # Don't re-add to buffer - these records will never succeed
            elif "platform" in error_str and self._schema_has_platform:
                self._logger.warning("platform_column_missing", msg="Retrying without platform")
                self._schema_has_platform = False
                self._orderbook_buffer = buffer + self._orderbook_buffer
                await self._flush_orderbooks()
            else:
                self._logger.error("flush_orderbooks_failed", error=error_str)
                self._orderbook_buffer = buffer + self._orderbook_buffer

    async def _flush_trades(self) -> None:
        if not self._trade_buffer or not self._pool:
            return
        buffer = self._trade_buffer
        self._trade_buffer = []
        try:
            async with self._pool.acquire() as conn:
                if self._schema_has_platform:
                    await conn.executemany(
                        """
                        INSERT INTO trades (
                            listener_id, asset_id, market, timestamp, price, size, side, fee_rate_bps, raw_payload, platform
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        """,
                        [
                            (
                                r["listener_id"], r["asset_id"], r["market"], r["timestamp"],
                                r["price"], r["size"], r["side"], r["fee_rate_bps"], r["raw_payload"],
                                r["platform"]
                            )
                            for r in buffer
                        ]
                    )
                else:
                    await conn.executemany(
                        """
                        INSERT INTO trades (
                            listener_id, asset_id, market, timestamp, price, size, side, fee_rate_bps, raw_payload
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        """,
                        [
                            (
                                r["listener_id"], r["asset_id"], r["market"], r["timestamp"],
                                r["price"], r["size"], r["side"], r["fee_rate_bps"], r["raw_payload"]
                            )
                            for r in buffer
                        ]
                    )
            self._logger.debug("flushed_trades", count=len(buffer))
        except Exception as e:
            error_str = str(e)
            if "foreign key constraint" in error_str:
                self._logger.warning("flush_trades_fk_violation", dropped=len(buffer))
            elif "platform" in error_str and self._schema_has_platform:
                self._logger.warning("platform_column_missing_trades", msg="Retrying without platform")
                self._schema_has_platform = False
                self._trade_buffer = buffer + self._trade_buffer
                await self._flush_trades()
            else:
                self._logger.error("flush_trades_failed", error=error_str)
                self._trade_buffer = buffer + self._trade_buffer
