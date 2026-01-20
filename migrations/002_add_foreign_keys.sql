-- Add foreign key from orderbook_snapshots to markets
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_orderbook_snapshots_market') THEN
        ALTER TABLE orderbook_snapshots
            ADD CONSTRAINT fk_orderbook_snapshots_market
            FOREIGN KEY (listener_id, asset_id)
            REFERENCES markets(listener_id, token_id)
            ON DELETE CASCADE;
    END IF;
END $$;

-- Add foreign key from trades to markets
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_trades_market') THEN
        ALTER TABLE trades
            ADD CONSTRAINT fk_trades_market
            FOREIGN KEY (listener_id, asset_id)
            REFERENCES markets(listener_id, token_id)
            ON DELETE CASCADE;
    END IF;
END $$;

-- Add index to improve FK lookup performance
CREATE INDEX IF NOT EXISTS idx_markets_listener_token ON markets(listener_id, token_id);
