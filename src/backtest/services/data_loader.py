"""
PostgreSQL data loader for backtesting.

Loads historical orderbook snapshots, trades, and market metadata from PostgreSQL
for backtesting analysis.
"""

import json
from typing import Optional

import asyncpg
import structlog

from backtest.core.interfaces import IDataLoader, BacktestDataset
from backtest.models.config import BacktestConfig
from models.orderbook import OrderbookSnapshot, OrderLevel
from models.trade import Trade
from models.market import Market


class PostgresDataLoader(IDataLoader):
    """Loads historical market data from PostgreSQL for backtesting."""

    def __init__(self, logger: Optional[structlog.BoundLogger] = None):
        """
        Initialize the data loader.

        Args:
            logger: Structured logger instance
        """
        self.logger = logger or structlog.get_logger(__name__)
        self._pool: Optional[asyncpg.Pool] = None

    async def load(self, config: BacktestConfig) -> BacktestDataset:
        """
        Load historical data based on backtest configuration.

        Args:
            config: Backtest configuration with filters and time range

        Returns:
            BacktestDataset with orderbooks, trades, and market metadata

        Raises:
            asyncpg.PostgresError: If database queries fail
            ValueError: If configuration is invalid
        """
        self.logger.info(
            "Loading backtest data",
            start_time_ms=config.start_time_ms,
            end_time_ms=config.end_time_ms,
            platform=config.platform,
            listener_id=config.listener_id,
            asset_ids=config.asset_ids,
            include_forward_filled=config.include_forward_filled,
        )

        # Create connection pool with graceful error handling
        try:
            self._pool = await asyncpg.create_pool(
                dsn=config.postgres_dsn,
                min_size=2,
                max_size=10,
                command_timeout=60,
            )
        except Exception as e:
            self.logger.error(
                "Failed to create database connection pool",
                error=str(e),
                dsn_host=config.postgres_dsn.split("@")[-1].split("/")[0] if "@" in config.postgres_dsn else "unknown",
            )
            raise ConnectionError(f"Could not connect to PostgreSQL database: {str(e)}") from e

        try:
            # Resolve asset_ids from listener_id if needed
            asset_ids = config.asset_ids
            if config.listener_id and not asset_ids:
                asset_ids = await self._get_asset_ids_for_listener(config.listener_id, config.platform)
                self.logger.info(
                    "Resolved asset_ids from listener",
                    listener_id=config.listener_id,
                    asset_count=len(asset_ids) if asset_ids else 0,
                )

            # Load data from database
            orderbooks = await self._load_orderbooks(
                start_time_ms=config.start_time_ms,
                end_time_ms=config.end_time_ms,
                platform=config.platform,
                asset_ids=asset_ids,
                listener_id=config.listener_id,
                include_forward_filled=config.include_forward_filled,
            )

            trades = await self._load_trades(
                start_time_ms=config.start_time_ms,
                end_time_ms=config.end_time_ms,
                platform=config.platform,
                asset_ids=asset_ids,
                listener_id=config.listener_id,
            )

            markets = await self._load_markets(
                platform=config.platform,
                asset_ids=asset_ids,
                listener_id=config.listener_id,
            )

            # Check for empty dataset
            if len(orderbooks) == 0 and len(trades) == 0:
                self.logger.warning(
                    "Empty dataset loaded - no orderbooks or trades found",
                    start_time_ms=config.start_time_ms,
                    end_time_ms=config.end_time_ms,
                    time_range_ms=(config.end_time_ms - config.start_time_ms),
                    platform=config.platform,
                    listener_id=config.listener_id,
                    asset_ids=asset_ids,
                    include_forward_filled=config.include_forward_filled,
                )

            # Detect data gaps in orderbooks
            if orderbooks:
                self._detect_data_gaps(orderbooks)

            # Validate timestamp ordering
            if orderbooks:
                self._validate_timestamps(orderbooks, "orderbook")
            if trades:
                self._validate_timestamps(trades, "trade")

            # Check memory safety
            total_events = len(orderbooks) + len(trades)
            if total_events > config.max_events_in_memory:
                self.logger.warning(
                    "Loaded events exceed safety limit",
                    total_events=total_events,
                    max_events=config.max_events_in_memory,
                    orderbooks=len(orderbooks),
                    trades=len(trades),
                )

            self.logger.info(
                "Data loaded successfully",
                orderbooks=len(orderbooks),
                trades=len(trades),
                markets=len(markets),
                time_range_ms=(config.end_time_ms - config.start_time_ms),
            )

            return BacktestDataset(
                orderbooks=orderbooks,
                trades=trades,
                markets=markets,
                start_time_ms=config.start_time_ms,
                end_time_ms=config.end_time_ms,
            )

        except Exception as e:
            self.logger.error("Failed to load backtest data", error=str(e))
            raise

    async def close(self) -> None:
        """Close the database connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self.logger.debug("Database connection pool closed")

    async def _get_asset_ids_for_listener(
        self, listener_id: str, platform: Optional[str]
    ) -> Optional[list[str]]:
        """
        Get all asset_ids (token_ids) for a given listener.

        Args:
            listener_id: Listener identifier
            platform: Optional platform filter

        Returns:
            List of asset_ids or None if no markets found
        """
        if not self._pool:
            self.logger.error("Database connection pool not initialized")
            return None

        query = """
            SELECT DISTINCT token_id
            FROM markets
            WHERE listener_id = $1
        """
        params = [listener_id]

        if platform:
            query += " AND platform = $2"
            params.append(platform)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        if not rows:
            return None

        return [row["token_id"] for row in rows if row["token_id"]]

    async def _load_orderbooks(
        self,
        start_time_ms: int,
        end_time_ms: int,
        platform: Optional[str],
        asset_ids: Optional[list[str]],
        listener_id: Optional[str],
        include_forward_filled: bool,
    ) -> list[OrderbookSnapshot]:
        """
        Load orderbook snapshots from database.

        Args:
            start_time_ms: Start of time range
            end_time_ms: End of time range
            platform: Optional platform filter
            asset_ids: Optional asset ID filter
            listener_id: Optional listener ID filter
            include_forward_filled: Whether to include forward-filled snapshots

        Returns:
            List of OrderbookSnapshot objects sorted by timestamp
        """
        if not self._pool:
            self.logger.error("Database connection pool not initialized")
            return []

        query = """
            SELECT
                listener_id, asset_id, market, timestamp,
                bids, asks, best_bid, best_ask,
                spread, mid_price, bid_depth, ask_depth,
                hash, is_forward_filled, source_timestamp, platform
            FROM orderbook_snapshots
            WHERE timestamp >= $1 AND timestamp <= $2
        """
        params = [start_time_ms, end_time_ms]
        param_idx = 3

        if not include_forward_filled:
            query += f" AND (is_forward_filled IS NULL OR is_forward_filled = false)"

        if platform:
            query += f" AND platform = ${param_idx}"
            params.append(platform)
            param_idx += 1

        if listener_id:
            query += f" AND listener_id = ${param_idx}"
            params.append(listener_id)
            param_idx += 1

        if asset_ids:
            query += f" AND asset_id = ANY(${param_idx})"
            params.append(asset_ids)
            param_idx += 1

        query += " ORDER BY timestamp ASC, asset_id ASC"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        total_rows = len(rows)
        orderbooks = []
        skipped_count = 0

        for idx, row in enumerate(rows):
            try:
                # Parse bids/asks from JSONB
                bids_data = row["bids"]
                asks_data = row["asks"]

                # Handle case where JSONB is returned as string
                if isinstance(bids_data, str):
                    bids_data = json.loads(bids_data)
                if isinstance(asks_data, str):
                    asks_data = json.loads(asks_data)

                # Convert to OrderLevel objects
                bids = [OrderLevel(**level) for level in (bids_data or [])]
                asks = [OrderLevel(**level) for level in (asks_data or [])]

                # Create snapshot (cast UUID fields to str for Pydantic)
                snapshot = OrderbookSnapshot(
                    listener_id=str(row["listener_id"]),
                    asset_id=str(row["asset_id"]),
                    market=str(row["market"]) if row["market"] else "",
                    timestamp=row["timestamp"],
                    bids=bids,
                    asks=asks,
                    best_bid=row["best_bid"],
                    best_ask=row["best_ask"],
                    spread=row["spread"],
                    mid_price=row["mid_price"],
                    bid_depth=row["bid_depth"],
                    ask_depth=row["ask_depth"],
                    hash=row["hash"],
                    is_forward_filled=row["is_forward_filled"],
                    source_timestamp=row["source_timestamp"],
                )

                # Compute metrics if missing
                if snapshot.best_bid is None or snapshot.best_ask is None:
                    snapshot.compute_metrics()

                orderbooks.append(snapshot)

                # Progress logging every 50,000 records
                if (idx + 1) % 50000 == 0:
                    self.logger.info(
                        f"Parsed {idx + 1}/{total_rows} orderbook records",
                        progress_pct=round((idx + 1) / total_rows * 100, 1),
                    )

            except Exception as e:
                skipped_count += 1
                self.logger.warning(
                    "Failed to parse orderbook snapshot",
                    asset_id=row.get("asset_id"),
                    timestamp=row.get("timestamp"),
                    error=str(e),
                )
                continue

        # Log summary of skipped records
        if skipped_count > 0:
            skip_pct = (skipped_count / total_rows * 100) if total_rows > 0 else 0
            log_level = "error" if skip_pct > 10 else "warning"

            log_msg = f"Skipped {skipped_count} of {total_rows} orderbook records ({skip_pct:.1f}%)"
            if log_level == "error":
                self.logger.error(
                    log_msg,
                    skipped=skipped_count,
                    total=total_rows,
                    skip_percentage=skip_pct,
                )
            else:
                self.logger.warning(
                    log_msg,
                    skipped=skipped_count,
                    total=total_rows,
                    skip_percentage=skip_pct,
                )

        return orderbooks

    async def _load_trades(
        self,
        start_time_ms: int,
        end_time_ms: int,
        platform: Optional[str],
        asset_ids: Optional[list[str]],
        listener_id: Optional[str],
    ) -> list[Trade]:
        """
        Load trades from database.

        Args:
            start_time_ms: Start of time range
            end_time_ms: End of time range
            platform: Optional platform filter
            asset_ids: Optional asset ID filter
            listener_id: Optional listener ID filter

        Returns:
            List of Trade objects sorted by timestamp
        """
        if not self._pool:
            self.logger.error("Database connection pool not initialized")
            return []

        query = """
            SELECT
                listener_id, asset_id, market, timestamp,
                price, size, side, fee_rate_bps, platform
            FROM trades
            WHERE timestamp >= $1 AND timestamp <= $2
        """
        params = [start_time_ms, end_time_ms]
        param_idx = 3

        if platform:
            query += f" AND platform = ${param_idx}"
            params.append(platform)
            param_idx += 1

        if listener_id:
            query += f" AND listener_id = ${param_idx}"
            params.append(listener_id)
            param_idx += 1

        if asset_ids:
            query += f" AND asset_id = ANY(${param_idx})"
            params.append(asset_ids)
            param_idx += 1

        query += " ORDER BY timestamp ASC, asset_id ASC"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        total_rows = len(rows)
        trades = []
        skipped_count = 0

        for idx, row in enumerate(rows):
            try:
                trade = Trade(
                    listener_id=str(row["listener_id"]),
                    asset_id=str(row["asset_id"]),
                    market=str(row["market"]) if row["market"] else "",
                    timestamp=row["timestamp"],
                    price=row["price"],
                    size=row["size"],
                    side=row["side"],
                    fee_rate_bps=row["fee_rate_bps"],
                    raw_payload={},  # Not stored in DB
                )
                trades.append(trade)

                # Progress logging every 50,000 records
                if (idx + 1) % 50000 == 0:
                    self.logger.info(
                        f"Parsed {idx + 1}/{total_rows} trade records",
                        progress_pct=round((idx + 1) / total_rows * 100, 1),
                    )

            except Exception as e:
                skipped_count += 1
                self.logger.warning(
                    "Failed to parse trade",
                    asset_id=row.get("asset_id"),
                    timestamp=row.get("timestamp"),
                    error=str(e),
                )
                continue

        # Log summary of skipped records
        if skipped_count > 0:
            skip_pct = (skipped_count / total_rows * 100) if total_rows > 0 else 0
            log_level = "error" if skip_pct > 10 else "warning"

            log_msg = f"Skipped {skipped_count} of {total_rows} trade records ({skip_pct:.1f}%)"
            if log_level == "error":
                self.logger.error(
                    log_msg,
                    skipped=skipped_count,
                    total=total_rows,
                    skip_percentage=skip_pct,
                )
            else:
                self.logger.warning(
                    log_msg,
                    skipped=skipped_count,
                    total=total_rows,
                    skip_percentage=skip_pct,
                )

        return trades

    async def _load_markets(
        self,
        platform: Optional[str],
        asset_ids: Optional[list[str]],
        listener_id: Optional[str],
    ) -> dict[str, Market]:
        """
        Load market metadata from database.

        Args:
            platform: Optional platform filter
            asset_ids: Optional asset ID filter
            listener_id: Optional listener ID filter

        Returns:
            Dictionary mapping token_id to Market objects
        """
        if not self._pool:
            self.logger.error("Database connection pool not initialized")
            return {}

        query = """
            SELECT
                listener_id, condition_id, token_id, market_slug,
                question, outcome, outcome_index, event_id,
                volume, liquidity, is_active, platform
            FROM markets
            WHERE 1=1
        """
        params = []
        param_idx = 1

        if platform:
            query += f" AND platform = ${param_idx}"
            params.append(platform)
            param_idx += 1

        if listener_id:
            query += f" AND listener_id = ${param_idx}"
            params.append(listener_id)
            param_idx += 1

        if asset_ids:
            query += f" AND token_id = ANY(${param_idx})"
            params.append(asset_ids)
            param_idx += 1

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        markets = {}
        for row in rows:
            try:
                market = Market(
                    listener_id=str(row["listener_id"]),
                    condition_id=str(row["condition_id"]) if row["condition_id"] else "",
                    token_id=str(row["token_id"]),
                    market_slug=row["market_slug"] or "",
                    question=row["question"] or "",
                    outcome=row["outcome"] or "",
                    outcome_index=row["outcome_index"],
                    event_id=str(row["event_id"]) if row["event_id"] else None,
                    volume=row["volume"],
                    liquidity=row["liquidity"],
                    is_active=row["is_active"],
                )
                markets[market.token_id] = market

            except Exception as e:
                self.logger.warning(
                    "Failed to parse market",
                    token_id=row.get("token_id"),
                    error=str(e),
                )
                continue

        return markets

    def _detect_data_gaps(self, orderbooks: list[OrderbookSnapshot]) -> None:
        """
        Detect gaps > 10 seconds between consecutive snapshots for the same asset.

        Args:
            orderbooks: List of orderbook snapshots sorted by timestamp
        """
        GAP_THRESHOLD_MS = 10_000  # 10 seconds

        # Group by asset_id
        asset_snapshots: dict[str, list[OrderbookSnapshot]] = {}
        for snapshot in orderbooks:
            if snapshot.asset_id not in asset_snapshots:
                asset_snapshots[snapshot.asset_id] = []
            asset_snapshots[snapshot.asset_id].append(snapshot)

        # Check for gaps within each asset
        total_gaps = 0
        for asset_id, snapshots in asset_snapshots.items():
            for i in range(1, len(snapshots)):
                prev_ts = snapshots[i - 1].timestamp
                curr_ts = snapshots[i].timestamp
                gap_ms = curr_ts - prev_ts

                if gap_ms > GAP_THRESHOLD_MS:
                    total_gaps += 1
                    gap_seconds = gap_ms / 1000.0
                    self.logger.warning(
                        "Data gap detected in orderbook snapshots",
                        asset_id=asset_id,
                        gap_start_ms=prev_ts,
                        gap_end_ms=curr_ts,
                        gap_duration_ms=gap_ms,
                        gap_duration_seconds=round(gap_seconds, 2),
                    )

        if total_gaps > 0:
            self.logger.warning(
                f"Found {total_gaps} data gaps > 10 seconds across all assets",
                total_gaps=total_gaps,
                assets_checked=len(asset_snapshots),
            )

    def _validate_timestamps(self, events: list, event_type: str) -> None:
        """
        Validate that timestamps are monotonically increasing within each asset.

        Args:
            events: List of events (orderbooks or trades) with timestamp and asset_id
            event_type: Type of event for logging ("orderbook" or "trade")
        """
        # Group by asset_id
        asset_events: dict[str, list] = {}
        for event in events:
            if event.asset_id not in asset_events:
                asset_events[event.asset_id] = []
            asset_events[event.asset_id].append(event)

        # Validate ordering within each asset
        total_violations = 0
        for asset_id, asset_event_list in asset_events.items():
            for i in range(1, len(asset_event_list)):
                prev_ts = asset_event_list[i - 1].timestamp
                curr_ts = asset_event_list[i].timestamp

                if curr_ts < prev_ts:
                    total_violations += 1
                    self.logger.warning(
                        f"Out-of-order timestamp detected in {event_type}",
                        asset_id=asset_id,
                        event_type=event_type,
                        prev_timestamp=prev_ts,
                        curr_timestamp=curr_ts,
                        index=i,
                    )

        if total_violations > 0:
            self.logger.warning(
                f"Found {total_violations} out-of-order timestamps in {event_type} events",
                total_violations=total_violations,
                event_type=event_type,
                assets_checked=len(asset_events),
            )
