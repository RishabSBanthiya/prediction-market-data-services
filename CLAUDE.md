# Polymarket Orderbook Liquidity Service

A Python service that captures real-time orderbook data from Polymarket prediction markets and stores it in Supabase for backtesting.

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
│   ├── listener_factory.py      # Creates Listener instances with dependencies
│   └── listener_manager.py      # Manages multiple listeners, loads configs from DB
├── services/
│   ├── market_discovery.py      # PolymarketDiscoveryService - Gamma API client
│   ├── websocket_client.py      # PolymarketWebSocketClient - CLOB WebSocket handler
│   ├── state_forward_filler.py  # StateForwardFiller - continuous stream generator
│   ├── supabase_writer.py       # SupabaseWriter - batched database writes
│   └── config_loader.py         # SupabaseConfigLoader - loads listener configs
├── models/
│   ├── listener.py              # ListenerConfig, ListenerFilters
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
