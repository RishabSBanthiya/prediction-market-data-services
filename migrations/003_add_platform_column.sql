-- Migration: Add platform column to support multiple prediction market platforms
-- Platform values: 'polymarket', 'kalshi'

-- Add platform column to listeners table
ALTER TABLE listeners
ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'polymarket';

-- Add platform column to markets table for data lineage
ALTER TABLE markets
ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'polymarket';

-- Add platform column to orderbook_snapshots for querying by platform
ALTER TABLE orderbook_snapshots
ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'polymarket';

-- Add platform column to trades
ALTER TABLE trades
ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'polymarket';

-- Add indexes for platform filtering (improves query performance)
CREATE INDEX IF NOT EXISTS idx_listeners_platform ON listeners(platform);
CREATE INDEX IF NOT EXISTS idx_markets_platform ON markets(platform);
CREATE INDEX IF NOT EXISTS idx_snapshots_platform ON orderbook_snapshots(platform);
CREATE INDEX IF NOT EXISTS idx_trades_platform ON trades(platform);

-- Add comments for documentation
COMMENT ON COLUMN listeners.platform IS 'Prediction market platform: polymarket, kalshi';
COMMENT ON COLUMN markets.platform IS 'Source platform for this market';
COMMENT ON COLUMN orderbook_snapshots.platform IS 'Source platform for this snapshot';
COMMENT ON COLUMN trades.platform IS 'Source platform for this trade';
