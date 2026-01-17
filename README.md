# Polymarket Orderbook Liquidity Service

A Python service that captures real-time orderbook data from Polymarket prediction markets and stores it in Supabase for backtesting.

## Features

- **Real-time orderbook capture** via WebSocket connection to Polymarket CLOB
- **Continuous data stream** using forward-fill mechanism (configurable interval, default 100ms)
- **Multi-listener architecture** - run multiple independent listeners for different market categories
- **Automatic market discovery** - finds markets based on filters (series, tags, slugs)
- **Supabase storage** - all data persisted for backtesting and analysis

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
│  │                                 │   │                                 │  │
│  │  ┌───────────────────────────┐  │   │  ┌───────────────────────────┐  │  │
│  │  │    Market Discovery       │  │   │  │    Market Discovery       │  │  │
│  │  │    (Gamma API polling)    │  │   │  │    (Gamma API polling)    │  │  │
│  │  └───────────────────────────┘  │   │  └───────────────────────────┘  │  │
│  │              │                  │   │              │                  │  │
│  │              ▼                  │   │              ▼                  │  │
│  │  ┌───────────────────────────┐  │   │  ┌───────────────────────────┐  │  │
│  │  │    WebSocket Client       │  │   │  │    WebSocket Client       │  │  │
│  │  │  (real-time book events)  │  │   │  │  (real-time book events)  │  │  │
│  │  └───────────────────────────┘  │   │  └───────────────────────────┘  │  │
│  │              │                  │   │              │                  │  │
│  │              ▼                  │   │              ▼                  │  │
│  │  ┌───────────────────────────┐  │   │  ┌───────────────────────────┐  │  │
│  │  │   State Forward Filler    │  │   │  │   State Forward Filler    │  │  │
│  │  │  (continuous 100ms stream)│  │   │  │  (continuous 100ms stream)│  │  │
│  │  └───────────────────────────┘  │   │  └───────────────────────────┘  │  │
│  │              │                  │   │              │                  │  │
│  └──────────────┼──────────────────┘   └──────────────┼──────────────────┘  │
│                 │                                     │                      │
│                 └──────────────┬──────────────────────┘                      │
│                                ▼                                             │
│                    ┌───────────────────────┐                                 │
│                    │    Supabase Writer    │                                 │
│                    │   (batched inserts)   │                                 │
│                    └───────────────────────┘                                 │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

1. **Market Discovery** - Periodically polls Gamma API to find markets matching filters
2. **WebSocket Subscription** - Subscribes to discovered market tokens for real-time updates
3. **Event Processing** - Receives `book` (orderbook) and `last_trade_price` (trade) events
4. **Forward Fill** - Maintains last known state per token, emits copies at regular intervals
5. **Storage** - Batches and writes snapshots to Supabase

### Forward-Fill Mechanism

WebSocket events are sparse (only sent when orderbook changes). The forward-filler creates a continuous stream:

```
Real Events:     ──●────────────────●──────────●────────────────
                   │                │          │
Forward-Fill:    ──●──●──●──●──●──●──●──●──●──●──●──●──●──●──●──
                   100ms intervals
```

Each forward-filled snapshot is marked with `is_forward_filled=true` and includes the `source_timestamp` of the original event.

## Quick Start

### 1. Prerequisites

- Python 3.11+
- Supabase project with the schema applied
- Polymarket API access (no auth required for public data)

### 2. Installation

```bash
# Clone and install
git clone <repo-url>
cd prediction-market-data-services
pip install -e .

# Configure environment
cp .env.example .env
# Edit .env with your Supabase credentials
```

### 3. Database Setup

Run the migration in your Supabase SQL editor:

```bash
# Apply schema
psql $DATABASE_URL -f migrations/001_initial_schema.sql
```

Or copy the contents of `migrations/001_initial_schema.sql` into the Supabase SQL editor.

### 4. Create a Listener

```bash
python scripts/seed_listener.py
```

### 5. Start the Service

```bash
python src/main.py
```

---

## How-To Guides

### Setting Up a New Listener

Listeners define what markets to track. Each listener has:
- **Filters** - Which markets to discover (by series, tags, or slugs)
- **Discovery interval** - How often to check for new markets
- **Emit interval** - Forward-fill frequency (milliseconds)

#### Option 1: SQL Insert

```sql
INSERT INTO listeners (name, description, filters, discovery_interval_seconds, emit_interval_ms)
VALUES (
  'nba-listener',
  'Tracks NBA basketball prediction markets',
  '{
    "series_ids": ["10345"]
  }',
  60,
  100
);
```

#### Option 2: Python Script

Create `scripts/seed_my_listener.py`:

```python
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from supabase import create_client
from config import Config

def seed_listener():
    config = Config()
    supabase = create_client(config.supabase_url, config.supabase_key)

    listener_data = {
        "name": "politics-listener",
        "description": "Tracks US political prediction markets",
        "filters": {
            "tag_ids": [1234],           # Filter by tag
            # "series_ids": ["5678"],    # Or by series
            # "slug_patterns": ["trump"], # Or by slug pattern
            # "min_liquidity": 1000,     # Minimum liquidity threshold
            # "min_volume": 5000,        # Minimum volume threshold
        },
        "discovery_interval_seconds": 120,  # Check for new markets every 2 min
        "emit_interval_ms": 100,            # Forward-fill every 100ms
        "is_active": True,
    }

    result = supabase.table("listeners").upsert(
        listener_data, on_conflict="name"
    ).execute()

    print(f"Created listener: {result.data}")

if __name__ == "__main__":
    seed_listener()
```

Run it:
```bash
python scripts/seed_my_listener.py
```

#### Finding Filter Values

**Series IDs** (for sports leagues):
- NBA: `10345`
- NFL: `10346`
- MLB: `10347`

**Tag IDs** (for categories):
- Browse [Polymarket](https://polymarket.com) and inspect network requests
- Or query the Gamma API: `https://gamma-api.polymarket.com/tags`

### Subscribing to the Data

Once the service is running, data flows into Supabase. Query it for your backtesting needs.

#### Real-time Subscription (Supabase Realtime)

```javascript
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

// Subscribe to new orderbook snapshots
const channel = supabase
  .channel('orderbook-changes')
  .on(
    'postgres_changes',
    {
      event: 'INSERT',
      schema: 'public',
      table: 'orderbook_snapshots',
      filter: 'asset_id=eq.YOUR_TOKEN_ID'
    },
    (payload) => {
      console.log('New snapshot:', payload.new)
    }
  )
  .subscribe()
```

#### Querying Historical Data (Python)

```python
from supabase import create_client
from datetime import datetime, timedelta

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Get snapshots for a specific market in the last hour
one_hour_ago = int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)

result = supabase.table("orderbook_snapshots") \
    .select("*") \
    .eq("asset_id", "YOUR_TOKEN_ID") \
    .gte("timestamp", one_hour_ago) \
    .order("timestamp", desc=False) \
    .execute()

snapshots = result.data
print(f"Got {len(snapshots)} snapshots")

# Access orderbook data
for snap in snapshots[:5]:
    print(f"Time: {snap['timestamp']}, Bid: {snap['best_bid']}, Ask: {snap['best_ask']}")
```

#### Querying with Filtering

```python
# Get only real events (not forward-filled)
result = supabase.table("orderbook_snapshots") \
    .select("*") \
    .eq("asset_id", "YOUR_TOKEN_ID") \
    .eq("is_forward_filled", False) \
    .order("timestamp", desc=True) \
    .limit(100) \
    .execute()

# Get trades for a market
trades = supabase.table("trades") \
    .select("*") \
    .eq("asset_id", "YOUR_TOKEN_ID") \
    .order("timestamp", desc=True) \
    .limit(50) \
    .execute()
```

#### Building OHLCV Candles

```python
import pandas as pd

# Fetch snapshots
result = supabase.table("orderbook_snapshots") \
    .select("timestamp, mid_price") \
    .eq("asset_id", "YOUR_TOKEN_ID") \
    .gte("timestamp", one_hour_ago) \
    .execute()

# Convert to DataFrame
df = pd.DataFrame(result.data)
df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
df.set_index('datetime', inplace=True)

# Resample to 1-minute candles
ohlcv = df['mid_price'].resample('1T').ohlc()
print(ohlcv)
```

---

## Project Structure

```
src/
├── main.py                      # Entry point
├── config.py                    # Environment configuration
├── core/
│   ├── interfaces.py            # Abstract base classes
│   ├── events.py                # Event types for queue
│   ├── listener.py              # Core Listener class
│   ├── listener_factory.py      # Creates Listener instances
│   └── listener_manager.py      # Manages multiple listeners
├── services/
│   ├── market_discovery.py      # Gamma API client
│   ├── websocket_client.py      # WebSocket handler
│   ├── state_forward_filler.py  # Continuous stream generator
│   ├── supabase_writer.py       # Database writer
│   └── config_loader.py         # Loads listener configs
├── models/
│   ├── listener.py              # ListenerConfig, ListenerFilters
│   ├── market.py                # Market model
│   ├── orderbook.py             # OrderbookSnapshot, OrderLevel
│   └── trade.py                 # Trade model
└── utils/
    └── logger.py                # Structured logging
```

## Database Schema

| Table | Description |
|-------|-------------|
| `listeners` | Listener configurations (filters, intervals) |
| `markets` | Discovered markets with metadata |
| `market_state_history` | Market lifecycle transitions |
| `orderbook_snapshots` | L2 orderbook snapshots (real + forward-filled) |
| `trades` | Trade execution events |

### Key Columns in `orderbook_snapshots`

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | BIGINT | Unix timestamp in milliseconds |
| `bids` | JSONB | Array of `{price, size}` objects |
| `asks` | JSONB | Array of `{price, size}` objects |
| `best_bid` | DECIMAL | Top of book bid price |
| `best_ask` | DECIMAL | Top of book ask price |
| `spread` | DECIMAL | `best_ask - best_bid` |
| `mid_price` | DECIMAL | `(best_bid + best_ask) / 2` |
| `is_forward_filled` | BOOLEAN | True if this is a forward-filled copy |
| `source_timestamp` | BIGINT | Original event timestamp (if forward-filled) |

## Configuration

### Environment Variables

Create a `.env` file:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key
LOG_LEVEL=INFO
```

### Listener Filters

```python
class ListenerFilters:
    series_ids: list[str]      # e.g., ["10345"] for NBA
    tag_ids: list[int]         # Category tags
    slug_patterns: list[str]   # Match market slugs
    condition_ids: list[str]   # Specific market IDs
    min_liquidity: float       # Minimum liquidity threshold
    min_volume: float          # Minimum volume threshold
```

## Running

```bash
# Development
python src/main.py

# With Docker
docker build -t polymarket-orderbook .
docker run --env-file .env polymarket-orderbook

# Run tests
pytest tests/ -v
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Test live NBA data capture
python scripts/test_live_nba.py
```

## Troubleshooting

### No data being captured
1. Check that you have an active listener in the database: `SELECT * FROM listeners WHERE is_active = true;`
2. Verify the listener filters match active markets
3. Check logs for WebSocket connection errors

### WebSocket disconnections
The service automatically reconnects with exponential backoff. If disconnections are frequent:
- Check your network connection
- Polymarket may have rate limits for high subscription counts

### Database schema errors
If you see errors about missing columns (`is_forward_filled`, `source_timestamp`):
- Run the latest migration: `psql $DATABASE_URL -f migrations/001_initial_schema.sql`
- The service will gracefully handle missing columns but forward-fill metadata won't be stored

## API Reference

### Polymarket WebSocket
- **URL**: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- **Events**: `book` (orderbook updates), `last_trade_price` (trades)
- **Subscription**: Send `{"assets_ids": ["token_id"], "type": "market"}`

### Gamma API (Market Discovery)
- **Base URL**: `https://gamma-api.polymarket.com`
- **Endpoints**:
  - `/events` - List events with markets
  - `/markets` - List individual markets
  - `/tags` - Available category tags

## License

MIT
