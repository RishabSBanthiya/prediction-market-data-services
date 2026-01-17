-- Listeners table
CREATE TABLE IF NOT EXISTS listeners (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    filters JSONB NOT NULL DEFAULT '{}',
    discovery_interval_seconds INTEGER NOT NULL DEFAULT 60,
    emit_interval_ms INTEGER NOT NULL DEFAULT 100,
    enable_forward_fill BOOLEAN DEFAULT false,  -- Set to true to emit forward-filled snapshots
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Markets table
CREATE TABLE IF NOT EXISTS markets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    listener_id UUID REFERENCES listeners(id),
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    market_slug TEXT,
    event_slug TEXT,
    UNIQUE(listener_id, condition_id),
    question TEXT,
    outcome TEXT,
    outcome_index INTEGER,
    event_id TEXT,
    event_title TEXT,
    category TEXT,
    subcategory TEXT,
    series_id TEXT,
    tags JSONB,
    description TEXT,
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    game_start_time TIMESTAMPTZ,
    outcome_prices JSONB,
    volume DECIMAL,
    liquidity DECIMAL,
    is_active BOOLEAN DEFAULT true,
    is_closed BOOLEAN DEFAULT false,
    state TEXT DEFAULT 'discovered',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Market state history
CREATE TABLE IF NOT EXISTS market_state_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_id UUID REFERENCES markets(id),
    listener_id UUID REFERENCES listeners(id),
    condition_id TEXT NOT NULL,
    previous_state TEXT,
    new_state TEXT NOT NULL,
    metadata JSONB,
    transitioned_at TIMESTAMPTZ DEFAULT NOW()
);

-- Orderbook snapshots
CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    listener_id UUID REFERENCES listeners(id),
    asset_id TEXT NOT NULL,
    market TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    bids JSONB NOT NULL,
    asks JSONB NOT NULL,
    best_bid DECIMAL,
    best_ask DECIMAL,
    spread DECIMAL,
    mid_price DECIMAL,
    bid_depth DECIMAL,
    ask_depth DECIMAL,
    hash TEXT,
    raw_payload JSONB,
    is_forward_filled BOOLEAN DEFAULT false,  -- True if this is a forward-filled copy
    source_timestamp BIGINT,                   -- Original event timestamp if forward-filled
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Trades
CREATE TABLE IF NOT EXISTS trades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    listener_id UUID REFERENCES listeners(id),
    asset_id TEXT NOT NULL,
    market TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    price DECIMAL NOT NULL,
    size DECIMAL NOT NULL,
    side TEXT NOT NULL,
    fee_rate_bps INTEGER,
    raw_payload JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_listeners_active ON listeners(is_active);
CREATE INDEX IF NOT EXISTS idx_markets_listener ON markets(listener_id, is_active);
CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);
CREATE INDEX IF NOT EXISTS idx_markets_token ON markets(token_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_listener_asset_ts ON orderbook_snapshots(listener_id, asset_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_asset_timestamp ON orderbook_snapshots(asset_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_market_timestamp ON orderbook_snapshots(market, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trades_listener_asset_ts ON trades(listener_id, asset_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trades_asset_timestamp ON trades(asset_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_state_history_market ON market_state_history(market_id, transitioned_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_forward_filled ON orderbook_snapshots(is_forward_filled);
CREATE INDEX IF NOT EXISTS idx_snapshots_source_ts ON orderbook_snapshots(source_timestamp) WHERE source_timestamp IS NOT NULL;
