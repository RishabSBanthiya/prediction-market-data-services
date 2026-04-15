# Plan: Parquet Data Loader for Jon-Becker Prediction Market Dataset

## Context

The existing backtesting framework (`src/backtest/`) only supports loading data from PostgreSQL via `PostgresDataLoader`. We want to also support the [Jon-Becker prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) dataset — the largest public dataset of Polymarket and Kalshi trade data, stored as chunked Parquet files (~36 GiB).

**Problem**: `BacktestEngine` hardcodes `PostgresDataLoader()` at `src/backtest/core/backtest_engine.py:114` and `IDataLoader.load()` takes `BacktestConfig` which requires `postgres_dsn`. Since we cannot modify existing files, we need a new engine + data loader combination.

**Key data gap**: The Jon-Becker dataset has **trade-level data** and **market metadata snapshots** (with best bid/ask for Kalshi), but **no L2 orderbook depth**. The existing engine walks L2 levels for execution — we handle this via synthetic orderbook generation.

## New Files (no existing files modified)

```
src/backtest/parquet/
├── __init__.py                          # Public API with lazy imports
├── config.py                            # ParquetBacktestConfig model
├── data_loader.py                       # ParquetDataLoader (DuckDB-based)
├── engine.py                            # ParquetBacktestEngine
├── orderbook_synthesizer.py             # Synthesizes OrderbookSnapshot from limited data
└── adapters/
    ├── __init__.py
    ├── base.py                          # BaseParquetAdapter ABC
    ├── kalshi_adapter.py                # Kalshi Parquet → model mapping
    └── polymarket_adapter.py            # Polymarket Parquet → model mapping

src/backtest/strategies/examples/
└── trade_follower.py                    # Example strategy for trade-only data

scripts/examples/
└── run_parquet_backtest.py              # CLI entry point

tests/backtest/parquet/
├── __init__.py
├── conftest.py                          # Fixtures: synthetic Parquet files
├── test_config.py                       # ~15 tests
├── test_kalshi_adapter.py               # ~12 tests
├── test_polymarket_adapter.py           # ~15 tests
├── test_orderbook_synthesizer.py        # ~10 tests
├── test_data_loader.py                  # ~8 tests (with synthetic Parquet)
└── test_integration.py                  # ~6 full pipeline tests
```

## Implementation Steps

### Step 1: `ParquetBacktestConfig` (`src/backtest/parquet/config.py`)

Pydantic `BaseModel` (same pattern as `BacktestConfig` in `src/backtest/models/config.py`).

Key fields:
- `data_root: Path` — root of Jon-Becker `data/` directory
- `platform: ParquetPlatform` — enum: `"kalshi"` | `"polymarket"`
- `start_time_ms: int`, `end_time_ms: int` — time range filter
- `tickers: Optional[list[str]]` — Kalshi market tickers filter
- `condition_ids: Optional[list[str]]` — Polymarket condition IDs filter
- `event_tickers: Optional[list[str]]` — Kalshi event-level filter
- `title_contains: Optional[str]` — text search in market title
- `min_volume: Optional[float]` — minimum volume filter
- `orderbook_mode: OrderbookSynthesisMode` — `"single_level"` | `"trade_implied"` | `"none"`
- `synthetic_depth_size: str = "100"` — assumed size for synthetic orderbook levels
- `initial_cash: float = 10000.0`
- `maker_fee_bps: int = 0`, `taker_fee_bps: int = 0`
- `max_events_in_memory: int = 5_000_000`

### Step 2: Adapters (`src/backtest/parquet/adapters/`)

#### `base.py` — `BaseParquetAdapter` ABC
- `adapt_market(row: dict) -> Market`
- `adapt_trade(row: dict) -> Trade`
- `get_markets_path(data_root: Path) -> Path`
- `get_trades_path(data_root: Path) -> Path`

#### `kalshi_adapter.py` — `KalshiParquetAdapter`

Field mapping:
| Parquet Field | → Model Field | Transform |
|---|---|---|
| `ticker` | `Market.token_id` | direct |
| `event_ticker` | `Market.condition_id` | direct |
| `title` | `Market.question` | direct |
| `status` | `Market.is_active` | `"open"` → True |
| `volume` | `Market.volume` | direct |
| `result` | stored for settlement | direct |
| `yes_bid` | `OrderbookSnapshot.bids[0].price` | cents/100 → `"0.50"` |
| `yes_ask` | `OrderbookSnapshot.asks[0].price` | cents/100 → `"0.50"` |
| `trade_id` | `Trade.raw_payload["trade_id"]` | direct |
| `ticker` (trade) | `Trade.asset_id` | direct |
| `count` | `Trade.size` | int → float |
| `yes_price` | `Trade.price` | cents/100 |
| `taker_side` | `Trade.side` | `"yes"` → `"buy"`, `"no"` → `"sell"` |
| `created_time` | `Trade.timestamp` | ISO 8601 → epoch ms |

#### `polymarket_adapter.py` — `PolymarketParquetAdapter`

More complex due to:
- Trades have `block_number` not timestamps → needs blocks table join
- Trades use `maker_asset_id`/`taker_asset_id` and raw amounts (6 decimals for USDC)
- Markets have `condition_id` but no explicit `token_id` → derived from trades

Field mapping:
| Parquet Field | → Model Field | Transform |
|---|---|---|
| `condition_id` | `Market.condition_id` | direct |
| `question` | `Market.question` | direct |
| `slug` | `Market.market_slug` | direct |
| `outcomes` | `Market.outcome` | parse JSON, one Market per outcome |
| `volume` | `Market.volume` | direct |
| `active` | `Market.is_active` | direct |
| `block_number` | `Trade.timestamp` | join blocks table → epoch ms |
| `maker_asset_id` | `Trade.asset_id` | this is the token_id |
| `maker_amount` / `taker_amount` | `Trade.price`, `Trade.size` | size = amount / 10^6, price = USDC / tokens |
| `fee` | `Trade.fee_rate_bps` | approximate from fee/notional |
| `taker_side` derived | `Trade.side` | infer from asset_id = 0 (USDC) check |

### Step 3: `OrderbookSynthesizer` (`src/backtest/parquet/orderbook_synthesizer.py`)

Three modes to handle the absence of real L2 depth:

**`SINGLE_LEVEL`** (default for Kalshi) — creates 1-level orderbook from market best bid/ask:
```
bids = [OrderLevel(price=yes_bid/100, size=synthetic_depth_size)]
asks = [OrderLevel(price=yes_ask/100, size=synthetic_depth_size)]
```

**`TRADE_IMPLIED`** (default for Polymarket) — after each trade at price P, creates:
```
bids = [OrderLevel(price=P - spread/2, size=synthetic_depth_size)]
asks = [OrderLevel(price=P + spread/2, size=synthetic_depth_size)]
```
Timestamp = trade timestamp + 1ms (preserves trade-before-orderbook ordering).

**`NONE`** — no orderbook events. Trade-only replay. Strategies use `on_trade()` only.

All synthetic snapshots carry `raw_payload={"synthetic": True, "mode": ...}` for strategies to check.

### Step 4: `ParquetDataLoader` (`src/backtest/parquet/data_loader.py`)

Uses **DuckDB** for efficient SQL queries across chunked Parquet files (glob patterns like `data/kalshi/trades/*.parquet`).

```python
class ParquetDataLoader:
    async def load(self, config: ParquetBacktestConfig) -> BacktestDataset:
        # Wraps sync DuckDB operations via asyncio.to_thread()
        ...
    async def close(self) -> None: ...
```

Flow:
1. Open DuckDB in-memory connection
2. Query markets Parquet, filter by config criteria → adapt via adapter
3. Query trades Parquet, filter by time range + market scope → adapt
4. For Polymarket: JOIN trades with `blocks/*.parquet` for timestamps
5. Synthesize orderbooks based on `orderbook_mode`
6. Sort, validate, return `BacktestDataset`

Key DuckDB queries:
```sql
-- Kalshi trades
SELECT * FROM read_parquet('data/kalshi/trades/*.parquet')
WHERE ticker IN (...) AND created_time >= ... AND created_time <= ...
ORDER BY created_time ASC

-- Polymarket trades with block timestamps
SELECT t.*, b.timestamp as block_ts
FROM read_parquet('data/polymarket/trades/*.parquet') t
JOIN read_parquet('data/polymarket/blocks/*.parquet') b
  ON t.block_number = b.block_number
WHERE ...
```

### Step 5: `ParquetBacktestEngine` (`src/backtest/parquet/engine.py`)

Mirrors `BacktestEngine` (lines 130-508 of `backtest_engine.py`) but uses `ParquetDataLoader`. Reuses all existing components:
- `Portfolio` for cash/position tracking
- `ExecutionEngine` for order matching
- `MetricsCollector` for performance metrics
- `MarketPairRegistry` for yes/no pair management
- `Strategy` ABC unchanged — strategies are fully compatible

```python
class ParquetBacktestEngine:
    def __init__(self, config: ParquetBacktestConfig, show_progress=True): ...
    async def run(self, strategy: Strategy) -> BacktestResult: ...
```

The `_process_orderbook_event()` and `_process_trade_event()` logic is identical to the existing engine — the only difference is data loading.

### Step 6: `__init__.py` exports (`src/backtest/parquet/__init__.py`)

Follows the lazy import pattern from `src/backtest/__init__.py`:
```python
# Eager (lightweight)
from .config import ParquetBacktestConfig, ParquetPlatform, OrderbookSynthesisMode

# Lazy via __getattr__ (requires duckdb, etc.)
# ParquetBacktestEngine, ParquetDataLoader
```

### Step 7: Example strategy + CLI script

**`trade_follower.py`** — momentum strategy that works with trade-only data:
- Tracks rolling buy/sell ratio over a configurable window
- Enters positions when ratio exceeds threshold
- Exits when ratio reverts to neutral
- Uses `on_trade()` as primary signal (works even with `orderbook_mode="none"`)

**`run_parquet_backtest.py`** — CLI with argparse:
```bash
python scripts/examples/run_parquet_backtest.py \
    --data-root ./data \
    --platform kalshi \
    --tickers PRES-2024-DJT \
    --start-date 2024-01-01 \
    --end-date 2024-12-01 \
    --initial-cash 10000
```

### Step 8: Tests

Tests in `tests/backtest/parquet/` using synthetic Parquet files created in `conftest.py` via `pyarrow.parquet.write_table()`. Follow patterns from `tests/backtest/test_integration.py`.

## Dependencies

Add to `pyproject.toml` under `[project.optional-dependencies]`:
```toml
parquet = ["duckdb>=0.9.0", "pyarrow>=14.0.0"]
```
Install: `pip install -e ".[backtest,parquet]"`

## Critical Files Referenced (read-only)

- `src/backtest/core/backtest_engine.py` — engine logic to replicate (lines 130-508)
- `src/backtest/core/interfaces.py` — `IDataLoader`, `BacktestDataset`, event types
- `src/backtest/core/strategy.py` — `Strategy` ABC, `BacktestContext`
- `src/backtest/models/config.py` — `BacktestConfig`, `FeeSchedule`, `BacktestResult`
- `src/backtest/models/portfolio.py` — `Portfolio`, `PortfolioView`
- `src/backtest/models/market_pair.py` — `MarketPairRegistry`
- `src/backtest/services/execution_engine.py` — `ExecutionEngine`
- `src/backtest/services/metrics.py` — `MetricsCollector`
- `src/models/orderbook.py` — `OrderbookSnapshot`, `OrderLevel`
- `src/models/trade.py` — `Trade`
- `src/models/market.py` — `Market`

## Verification

1. Unit tests: `python -m pytest tests/backtest/parquet/ -v -m "not integration"`
2. Integration tests: `python -m pytest tests/backtest/parquet/ -v -m integration`
3. Existing tests still pass: `python -m pytest tests/backtest/ -v` (no existing files modified)
4. End-to-end with real data (requires downloading dataset):
   ```bash
   git clone https://github.com/Jon-Becker/prediction-market-analysis /tmp/pm-data
   cd /tmp/pm-data && make setup
   python scripts/examples/run_parquet_backtest.py \
       --data-root /tmp/pm-data/data --platform kalshi --tickers PRES-2024-DJT
   ```
