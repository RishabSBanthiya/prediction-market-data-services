"""
Tests for PostgresWriter using local PostgreSQL database.

Run with: APP_ENV=local pytest tests/test_postgres.py -v
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import asyncio
import pytest
import asyncpg
from uuid import uuid4

from config import Config
from models import OrderbookSnapshot, OrderLevel, Trade, Market, MarketState
from services.postgres_writer import PostgresWriter
from utils.logger import LoggerFactory


@pytest.fixture(scope="module")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def config():
    """Load config from .env.local when APP_ENV=local."""
    os.environ["APP_ENV"] = "local"
    return Config()


@pytest.fixture(scope="module")
def logger():
    factory = LoggerFactory("INFO")
    return factory.create("test_postgres")


@pytest.fixture(scope="module")
async def db_pool(config):
    """Create a connection pool for test setup/teardown."""
    pool = await asyncpg.create_pool(config.postgres_dsn, min_size=1, max_size=5)
    yield pool
    await pool.close()


@pytest.fixture(scope="module")
async def setup_schema(db_pool):
    """Ensure database schema exists."""
    migrations_dir = os.path.join(os.path.dirname(__file__), "..", "migrations")

    async with db_pool.acquire() as conn:
        # Run initial schema
        schema_path = os.path.join(migrations_dir, "001_initial_schema.sql")
        if os.path.exists(schema_path):
            with open(schema_path) as f:
                await conn.execute(f.read())

        # Run foreign keys migration
        fk_path = os.path.join(migrations_dir, "002_add_foreign_keys.sql")
        if os.path.exists(fk_path):
            with open(fk_path) as f:
                await conn.execute(f.read())

    yield


@pytest.fixture
async def test_listener_id(db_pool, setup_schema):
    """Create a test listener and clean up after."""
    listener_id = None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO listeners (name, filters, discovery_interval_seconds)
            VALUES ($1, $2, $3)
            RETURNING id
            """,
            f"test-listener-{uuid4().hex[:8]}",
            '{"tag_ids": [100639]}',
            60,
        )
        listener_id = str(row["id"])

    yield listener_id

    # Cleanup: delete in order respecting foreign keys
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM orderbook_snapshots WHERE listener_id = $1", listener_id
        )
        await conn.execute("DELETE FROM trades WHERE listener_id = $1", listener_id)
        await conn.execute(
            "DELETE FROM market_state_history WHERE listener_id = $1", listener_id
        )
        await conn.execute("DELETE FROM markets WHERE listener_id = $1", listener_id)
        await conn.execute("DELETE FROM listeners WHERE id = $1", listener_id)


@pytest.fixture
async def postgres_writer(config, logger, test_listener_id):
    """Create and start a PostgresWriter instance."""
    writer = PostgresWriter(config.postgres_dsn, test_listener_id, logger)
    await writer.start()
    yield writer
    await writer.stop()


class TestPostgresWriterConnection:
    """Tests for PostgresWriter connection handling."""

    @pytest.mark.asyncio
    async def test_start_and_stop(self, config, logger, test_listener_id):
        """Test writer starts and stops cleanly."""
        writer = PostgresWriter(config.postgres_dsn, test_listener_id, logger)
        await writer.start()
        assert writer._pool is not None
        assert writer._running is True

        await writer.stop()
        assert writer._running is False


class TestPostgresWriterMarkets:
    """Tests for market write operations."""

    @pytest.mark.asyncio
    async def test_write_market(self, postgres_writer, db_pool, test_listener_id):
        """Test writing a market to the database."""
        market = Market(
            condition_id=f"condition-{uuid4().hex[:8]}",
            token_id=f"token-{uuid4().hex[:8]}",
            question="Will it rain tomorrow?",
            outcome="Yes",
            outcome_index=0,
            event_title="Weather Forecast",
            category="Weather",
            volume=1000.0,
            liquidity=500.0,
            state=MarketState.TRACKING,
        )

        await postgres_writer.write_market(market)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM markets WHERE token_id = $1 AND listener_id = $2",
                market.token_id,
                test_listener_id,
            )

        assert row is not None
        assert row["question"] == "Will it rain tomorrow?"
        assert row["outcome"] == "Yes"
        assert row["state"] == "tracking"
        assert float(row["volume"]) == 1000.0

    @pytest.mark.asyncio
    async def test_write_market_upsert(self, postgres_writer, db_pool, test_listener_id):
        """Test that writing the same market updates it."""
        token_id = f"token-{uuid4().hex[:8]}"
        condition_id = f"condition-{uuid4().hex[:8]}"

        market_v1 = Market(
            condition_id=condition_id,
            token_id=token_id,
            question="Original question?",
            state=MarketState.DISCOVERED,
        )
        await postgres_writer.write_market(market_v1)

        market_v2 = Market(
            condition_id=condition_id,
            token_id=token_id,
            question="Updated question?",
            state=MarketState.TRACKING,
        )
        await postgres_writer.write_market(market_v2)

        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM markets WHERE token_id = $1 AND listener_id = $2",
                token_id,
                test_listener_id,
            )

        assert len(rows) == 1
        assert rows[0]["question"] == "Updated question?"
        assert rows[0]["state"] == "tracking"


class TestPostgresWriterOrderbooks:
    """Tests for orderbook snapshot write operations."""

    @pytest.mark.asyncio
    async def test_write_orderbook(self, postgres_writer, db_pool, test_listener_id):
        """Test writing an orderbook snapshot."""
        # First create a market (FK requirement)
        token_id = f"token-{uuid4().hex[:8]}"
        market = Market(
            condition_id=f"condition-{uuid4().hex[:8]}",
            token_id=token_id,
            question="Test market?",
            state=MarketState.TRACKING,
        )
        await postgres_writer.write_market(market)

        snapshot = OrderbookSnapshot(
            listener_id=test_listener_id,
            asset_id=token_id,
            market="test-market-slug",
            timestamp=1700000000000,
            bids=[
                OrderLevel(price="0.55", size="100"),
                OrderLevel(price="0.54", size="200"),
            ],
            asks=[
                OrderLevel(price="0.56", size="150"),
                OrderLevel(price="0.57", size="250"),
            ],
            hash="abc123",
            raw_payload={"test": True},
        )
        snapshot.compute_metrics()

        await postgres_writer.write_orderbook(snapshot)
        await postgres_writer.flush()

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM orderbook_snapshots
                WHERE listener_id = $1 AND asset_id = $2
                ORDER BY timestamp DESC LIMIT 1
                """,
                test_listener_id,
                token_id,
            )

        assert row is not None
        assert float(row["best_bid"]) == 0.55
        assert float(row["best_ask"]) == 0.56
        assert float(row["spread"]) == pytest.approx(0.01, rel=1e-6)
        assert row["is_forward_filled"] is False

    @pytest.mark.asyncio
    async def test_write_forward_filled_orderbook(
        self, postgres_writer, db_pool, test_listener_id
    ):
        """Test writing a forward-filled orderbook snapshot."""
        token_id = f"token-{uuid4().hex[:8]}"
        market = Market(
            condition_id=f"condition-{uuid4().hex[:8]}",
            token_id=token_id,
            question="Forward fill test?",
            state=MarketState.TRACKING,
        )
        await postgres_writer.write_market(market)

        snapshot = OrderbookSnapshot(
            listener_id=test_listener_id,
            asset_id=token_id,
            market="test-market",
            timestamp=1700000000100,
            bids=[OrderLevel(price="0.50", size="100")],
            asks=[OrderLevel(price="0.51", size="100")],
            is_forward_filled=True,
            source_timestamp=1700000000000,
        )
        snapshot.compute_metrics()

        await postgres_writer.write_orderbook(snapshot)
        await postgres_writer.flush()

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM orderbook_snapshots
                WHERE listener_id = $1 AND asset_id = $2 AND is_forward_filled = true
                """,
                test_listener_id,
                token_id,
            )

        assert row is not None
        assert row["is_forward_filled"] is True
        assert row["source_timestamp"] == 1700000000000

    @pytest.mark.asyncio
    async def test_batch_flush(self, postgres_writer, db_pool, test_listener_id):
        """Test that multiple orderbooks are batched and flushed."""
        token_id = f"token-{uuid4().hex[:8]}"
        market = Market(
            condition_id=f"condition-{uuid4().hex[:8]}",
            token_id=token_id,
            question="Batch test?",
            state=MarketState.TRACKING,
        )
        await postgres_writer.write_market(market)

        # Write multiple snapshots
        for i in range(5):
            snapshot = OrderbookSnapshot(
                listener_id=test_listener_id,
                asset_id=token_id,
                market="batch-test",
                timestamp=1700000000000 + (i * 100),
                bids=[OrderLevel(price=f"0.{50+i}", size="100")],
                asks=[OrderLevel(price=f"0.{51+i}", size="100")],
            )
            snapshot.compute_metrics()
            await postgres_writer.write_orderbook(snapshot)

        await postgres_writer.flush()

        async with db_pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM orderbook_snapshots
                WHERE listener_id = $1 AND asset_id = $2
                """,
                test_listener_id,
                token_id,
            )

        assert count == 5


class TestPostgresWriterTrades:
    """Tests for trade write operations."""

    @pytest.mark.asyncio
    async def test_write_trade(self, postgres_writer, db_pool, test_listener_id):
        """Test writing a trade to the database."""
        token_id = f"token-{uuid4().hex[:8]}"
        market = Market(
            condition_id=f"condition-{uuid4().hex[:8]}",
            token_id=token_id,
            question="Trade test?",
            state=MarketState.TRACKING,
        )
        await postgres_writer.write_market(market)

        trade = Trade(
            listener_id=test_listener_id,
            asset_id=token_id,
            market="trade-test-market",
            timestamp=1700000000000,
            price=0.55,
            size=100.0,
            side="BUY",
            fee_rate_bps=10,
            raw_payload={"maker": "0x123", "taker": "0x456"},
        )

        await postgres_writer.write_trade(trade)
        await postgres_writer.flush()

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM trades
                WHERE listener_id = $1 AND asset_id = $2
                """,
                test_listener_id,
                token_id,
            )

        assert row is not None
        assert float(row["price"]) == 0.55
        assert float(row["size"]) == 100.0
        assert row["side"] == "BUY"
        assert row["fee_rate_bps"] == 10


class TestPostgresWriterStateTransitions:
    """Tests for market state transition tracking."""

    @pytest.mark.asyncio
    async def test_write_state_transition(
        self, postgres_writer, db_pool, test_listener_id
    ):
        """Test writing a state transition."""
        condition_id = f"condition-{uuid4().hex[:8]}"

        await postgres_writer.write_state_transition(
            market_id=condition_id,
            old_state="discovered",
            new_state="tracking",
            metadata={"reason": "liquidity threshold met"},
        )

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM market_state_history
                WHERE listener_id = $1 AND condition_id = $2
                """,
                test_listener_id,
                condition_id,
            )

        assert row is not None
        assert row["previous_state"] == "discovered"
        assert row["new_state"] == "tracking"


class TestPostgresWriterEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_write_orderbook_without_market_fk_violation(
        self, postgres_writer, db_pool, test_listener_id
    ):
        """Test that FK violation on orderbook write is handled gracefully."""
        snapshot = OrderbookSnapshot(
            listener_id=test_listener_id,
            asset_id="nonexistent-token",
            market="test-market",
            timestamp=1700000000000,
            bids=[OrderLevel(price="0.50", size="100")],
            asks=[OrderLevel(price="0.51", size="100")],
        )
        snapshot.compute_metrics()

        await postgres_writer.write_orderbook(snapshot)
        await postgres_writer.flush()

        # Should not raise, writer handles FK violations gracefully
        async with db_pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM orderbook_snapshots
                WHERE asset_id = 'nonexistent-token'
                """
            )
        assert count == 0

    @pytest.mark.asyncio
    async def test_empty_orderbook(self, postgres_writer, db_pool, test_listener_id):
        """Test writing an orderbook with empty bids/asks."""
        token_id = f"token-{uuid4().hex[:8]}"
        market = Market(
            condition_id=f"condition-{uuid4().hex[:8]}",
            token_id=token_id,
            question="Empty orderbook test?",
            state=MarketState.TRACKING,
        )
        await postgres_writer.write_market(market)

        snapshot = OrderbookSnapshot(
            listener_id=test_listener_id,
            asset_id=token_id,
            market="empty-test",
            timestamp=1700000000000,
            bids=[],
            asks=[],
        )
        snapshot.compute_metrics()

        await postgres_writer.write_orderbook(snapshot)
        await postgres_writer.flush()

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM orderbook_snapshots
                WHERE listener_id = $1 AND asset_id = $2
                """,
                test_listener_id,
                token_id,
            )

        assert row is not None
        assert row["best_bid"] is None
        assert row["best_ask"] is None
