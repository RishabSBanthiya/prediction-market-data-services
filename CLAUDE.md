# Prediction Market Orderbook Liquidity Service

A Python service that captures real-time orderbook data from prediction markets (Polymarket and Kalshi) and stores it in Supabase/PostgreSQL for backtesting.

## Supported Platforms

| Platform | Market Discovery | WebSocket | Auth Required |
|----------|-----------------|-----------|---------------|
| Polymarket | Gamma API | CLOB WebSocket | No |
| Kalshi | REST API | WebSocket | Yes (RSA) |

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            ListenerManager                                    │
│                   (loads configs from DB, spawns listeners)                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────┐   ┌─────────────────────────────────┐  │
│  │        Listener: NBA            │   │       Listener: Politics        │  │
│  ├─────────────────────────────────┤   ├─────────────────────────────────┤  │
│  │  Market Discovery (Gamma API)   │   │  Market Discovery (Gamma API)   │  │
│  │  WebSocket Client (CLOB)        │   │  WebSocket Client (CLOB)        │  │
│  │  State Forward Filler (100ms)   │   │  State Forward Filler (100ms)   │  │
│  └─────────────────────────────────┘   └─────────────────────────────────┘  │
│                 │                                     │                      │
│                 └──────────────┬──────────────────────┘                      │
│                                ▼                                             │
│                    ┌───────────────────────┐                                 │
│                    │    Supabase Writer    │                                 │
│                    │   (batched inserts)   │                                 │
│                    └───────────────────────┘                                 │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

1. **Market Discovery** - Polls Gamma API for markets matching listener filters
2. **WebSocket Subscription** - Subscribes to token IDs for real-time `book` and `last_trade_price` events
3. **Forward Fill** - Maintains last known state per token, emits copies at configurable interval (default 100ms)
4. **Storage** - Batches and writes snapshots to Supabase

### Forward-Fill Mechanism

WebSocket events are sparse (only sent when orderbook changes). The forward-filler creates a continuous stream:

```
Real Events:     ──●────────────────●──────────●────────────────
                   │                │          │
Forward-Fill:    ──●──●──●──●──●──●──●──●──●──●──●──●──●──●──●──
                   100ms intervals
```

Each forward-filled snapshot is marked with `is_forward_filled=true` and includes `source_timestamp`.

## Project Structure

```
src/
├── main.py                      # Entry point
├── config.py                    # Environment configuration
├── core/
│   ├── interfaces.py            # Abstract base classes (IWebSocketClient, IDataWriter, etc.)
│   ├── events.py                # Event types (OrderbookEvent, TradeEvent, MarketDiscoveredEvent)
│   ├── listener.py              # Core Listener class - orchestrates discovery, websocket, forward-fill
│   ├── listener_factory.py      # Creates Listener instances with platform routing
│   └── listener_manager.py      # Manages multiple listeners, loads configs from DB
├── services/
│   ├── polymarket_discovery.py      # PolymarketDiscoveryService - Gamma API client
│   ├── polymarket_websocket_client.py # PolymarketWebSocketClient - CLOB WebSocket handler
│   ├── kalshi_auth.py               # KalshiAuthenticator - RSA-PSS authentication
│   ├── kalshi_discovery.py          # KalshiDiscoveryService - REST API client
│   ├── kalshi_websocket_client.py   # KalshiWebSocketClient - WebSocket with orderbook state
│   ├── state_forward_filler.py      # StateForwardFiller - continuous stream generator
│   ├── supabase_writer.py           # SupabaseWriter - batched database writes
│   ├── postgres_writer.py           # PostgresWriter - batched PostgreSQL writes
│   └── config_loader.py             # SupabaseConfigLoader/PostgresConfigLoader
├── models/
│   ├── listener.py              # ListenerConfig, ListenerFilters, Platform enum
│   ├── kalshi_filters.py        # KalshiListenerFilters
│   ├── market.py                # Market model with state tracking
│   ├── orderbook.py             # OrderbookSnapshot, OrderLevel
│   └── trade.py                 # Trade model
└── utils/
    └── logger.py                # Structured JSON logging
```

## Key Classes

### Listener (src/core/listener.py)
Independent data collection unit running 3 concurrent asyncio tasks:
- `_run_discovery_loop()` - Polls Gamma API at `discovery_interval_seconds`
- `_run_websocket_listener()` - Receives and parses WebSocket events
- `_run_event_processor()` - Processes events from two queues (data high-priority, control low-priority)

### StateForwardFiller (src/services/state_forward_filler.py)
Creates continuous data stream from sparse WebSocket events:
- `add_token(token_id, market_id)` - Start tracking a token
- `update_state(snapshot)` - Update with real WebSocket event
- `remove_token(token_id)` - Stop tracking
- Emits via callback at `emit_interval_ms` (default 100ms)

### PolymarketWebSocketClient (src/services/websocket_client.py)
- URL: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- Events: `book` (orderbook), `last_trade_price` (trades)
- Supports subscribe/unsubscribe via `assets_ids` with `operation` field
- Auto-reconnect with exponential backoff (1s to 60s)
- Ping/pong every 5 seconds for connection health

### SupabaseWriter (src/services/supabase_writer.py)
- Batched inserts (BATCH_SIZE=100, FLUSH_INTERVAL=1s)
- Gracefully handles missing forward-fill columns in schema

## Database Schema

| Table | Description |
|-------|-------------|
| `listeners` | Listener configs (name, filters, intervals, is_active) |
| `markets` | Discovered markets with metadata |
| `market_state_history` | Market lifecycle transitions |
| `orderbook_snapshots` | L2 orderbook snapshots (real + forward-filled) |
| `trades` | Trade execution events |

### Key Columns in `orderbook_snapshots`

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | BIGINT | Unix timestamp in milliseconds |
| `bids` | JSONB | Array of `{price, size}` |
| `asks` | JSONB | Array of `{price, size}` |
| `best_bid`, `best_ask` | DECIMAL | Top of book prices |
| `spread`, `mid_price` | DECIMAL | Computed metrics |
| `is_forward_filled` | BOOLEAN | True if forward-filled copy |
| `source_timestamp` | BIGINT | Original event timestamp |

## Configuration

Environment variables (`.env`):
```
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_KEY=your-service-role-key
LOG_LEVEL=INFO
```

### ListenerFilters (src/models/listener.py)
```python
series_ids: list[str]      # e.g., ["10345"] for NBA
tag_ids: list[int]         # Category tags
slug_patterns: list[str]   # Match market slugs (substring)
condition_ids: list[str]   # Specific market IDs
min_liquidity: float       # Minimum liquidity threshold
min_volume: float          # Minimum volume threshold
```

## Running

```bash
# Install
pip install -e .

# Seed a listener
python scripts/seed_listener.py

# Start service
python src/main.py

# Test live data
python scripts/test_live_nba.py

# Run tests
pytest tests/ -v
```

## Filter Values

Sports series IDs:
- NBA: `10345`
- NFL: `10346`
- MLB: `10347`

## Common Issues

1. **No data captured**: Check `SELECT * FROM listeners WHERE is_active = true;`
2. **WebSocket errors**: Service auto-reconnects; check network/rate limits
3. **Missing columns**: Run latest migration or service handles gracefully

---

## Kalshi Integration

### Setting Up Kalshi Credentials

1. Log in to [Kalshi](https://kalshi.com)
2. Go to **Profile Settings** > **API Keys**
3. Click **Create New API Key**
4. Download the private key file (RSA PEM format)
5. Save the Key ID shown on screen

### Kalshi Environment Variables

Add to your `.env` file:
```
KALSHI_API_KEY=your-key-id-here
KALSHI_PRIVATE_KEY_PATH=/path/to/kalshi-private-key.pem

# Or provide key content directly (useful for Docker/cloud):
# KALSHI_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----\n..."
```

### Kalshi-Specific Files

```
src/services/
├── kalshi_auth.py              # RSA-PSS authentication
├── kalshi_discovery.py         # REST API market discovery
└── kalshi_websocket_client.py  # Real-time data (orderbook/trades)

src/models/
└── kalshi_filters.py           # Kalshi filter model
```

### KalshiListenerFilters

```python
series_tickers: list[str]      # e.g., ["KXELECTION"]
event_tickers: list[str]       # Specific event tickers
market_tickers: list[str]      # Specific market tickers
status: str = "open"           # open, closed, settled
min_volume: float              # Minimum volume threshold
min_open_interest: float       # Minimum open interest
title_contains: str            # Text search in title
```

### Kalshi Series Tickers

Common series:
- Elections: `KXELECTION`
- Economics: `KXECON`
- Finance: `KXFINANCE`
- Weather: `KXWEATHER`

### Running Kalshi Listener

```bash
# Run database migration first
psql -d your_db -f migrations/003_add_platform_column.sql

# Seed a Kalshi listener
python scripts/seed_kalshi_listener.py

# Test Kalshi connection (discovery only - no auth needed)
python scripts/test_kalshi_live.py --discovery

# Test Kalshi WebSocket (requires auth)
python scripts/test_kalshi_live.py --websocket

# Start service (handles both Polymarket and Kalshi)
python src/main.py
```

### Kalshi vs Polymarket Differences

| Aspect | Polymarket | Kalshi |
|--------|-----------|--------|
| Market ID | `token_id` (per outcome) | `ticker` (per market) |
| Orderbook | Direct bids/asks | Snapshot + delta (yes/no sides) |
| Prices | Decimals (0.0-1.0) | Cents (0-100), normalized to decimals |
| Timestamps | Milliseconds | Seconds, converted to milliseconds |
| WS Auth | None | RSA-PSS required |

### Kalshi Orderbook Normalization

Kalshi uses yes/no sides instead of bids/asks. The service normalizes:
- YES levels → bids (price = cents/100)
- NO levels at P → asks at (100-P)/100

Example:
```
Kalshi: yes: [[50, 100]], no: [[40, 150]]
Normalized: bids: [{price: "0.50", size: "100"}], asks: [{price: "0.60", size: "150"}]
```

### Platform Column

All tables now include a `platform` column (`polymarket` or `kalshi`) for filtering:

```sql
SELECT * FROM orderbook_snapshots WHERE platform = 'kalshi';
SELECT * FROM trades WHERE platform = 'polymarket';
```
