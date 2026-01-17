# Polymarket Orderbook Liquidity Service

A Python service that captures real-time orderbook data from Polymarket prediction markets and stores it in Supabase for backtesting.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       ListenerManager                            │
│              (loads configs from DB, spawns listeners)           │
├─────────────────────────────────────────────────────────────────┤
│  ┌────────────────────┐  ┌────────────────────┐                 │
│  │   Listener: NBA    │  │  Listener: Other   │     ...         │
│  ├────────────────────┤  ├────────────────────┤                 │
│  │ Market Discovery   │  │ Market Discovery   │                 │
│  │ WebSocket Client   │  │ WebSocket Client   │                 │
│  │ Event Queue        │  │ Event Queue        │                 │
│  └─────────┬──────────┘  └─────────┬──────────┘                 │
│            │                       │                            │
│            └───────────┬───────────┘                            │
│                        ▼                                        │
│              ┌─────────────────────┐                            │
│              │  Supabase Writer    │                            │
│              └─────────────────────┘                            │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
src/
├── main.py                 # Entry point
├── config.py               # Environment configuration
├── core/
│   ├── interfaces.py       # Abstract base classes
│   ├── events.py           # Event types for queue
│   ├── listener.py         # Core Listener class
│   ├── listener_factory.py # Creates Listener instances
│   └── listener_manager.py # Manages multiple listeners
├── services/
│   ├── market_discovery.py # Gamma API client
│   ├── websocket_client.py # WebSocket handler
│   ├── supabase_writer.py  # Database writer
│   └── config_loader.py    # Loads listener configs
├── models/
│   ├── listener.py         # ListenerConfig, ListenerFilters
│   ├── market.py           # Market model
│   ├── orderbook.py        # OrderbookSnapshot, OrderLevel
│   └── trade.py            # Trade model
└── utils/
    └── logger.py           # Structured logging
```

## Key Classes

### Listener (src/core/listener.py)
Independent data collection unit. Each listener has its own:
- Discovery loop (polls Gamma API for markets)
- WebSocket connection (receives orderbook/trade events)
- Event queue (decoupled processing)

### ListenerManager (src/core/listener_manager.py)
Orchestrates multiple listeners:
- Loads active listener configs from Supabase
- Spawns/stops listeners dynamically
- Monitors listener health

### PolymarketDiscoveryService (src/services/market_discovery.py)
Discovers markets from Gamma API:
- Supports filtering by tag_ids, series_ids, slug_patterns
- Applies liquidity/volume thresholds

### PolymarketWebSocketClient (src/services/websocket_client.py)
Handles WebSocket connection to Polymarket CLOB:
- Exponential backoff reconnection
- Periodic ping/pong for connection health

## Database Schema

Tables:
- `listeners` - Listener configurations
- `markets` - Discovered markets
- `market_state_history` - Market lifecycle transitions
- `orderbook_snapshots` - Full L2 orderbook snapshots
- `trades` - Trade execution events

## Configuration

Environment variables (`.env`):
```
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_KEY=your-service-role-key
LOG_LEVEL=INFO
```

## Running

```bash
# Install dependencies
pip install -e .

# Run tests
pytest tests/ -v

# Seed a listener configuration
python scripts/seed_listener.py

# Start the service
python src/main.py
```

## Docker

```bash
docker build -t polymarket-orderbook .
docker run --env-file .env polymarket-orderbook
```

## Adding New Listeners

Insert a new row into the `listeners` table:
```sql
INSERT INTO listeners (name, filters, discovery_interval_seconds) VALUES (
  'politics-listener',
  '{"tag_ids": [12345]}',
  300
);
```

The service will automatically pick up new active listeners on the next config reload.
