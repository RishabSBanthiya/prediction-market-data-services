-- Fix markets table to allow multiple tokens per condition_id
--
-- The original schema only intended UNIQUE(listener_id, token_id) but an additional
-- UNIQUE(listener_id, condition_id) constraint was added, preventing multiple tokens
-- (e.g., Up and Down) from being stored for the same market/condition.
--
-- This migration drops the condition_id unique constraint while keeping the token_id one.

-- Drop the condition_id unique constraint if it exists
ALTER TABLE markets DROP CONSTRAINT IF EXISTS markets_listener_id_condition_id_key;

-- Verify the correct constraint exists (should already be there from initial schema)
-- This is idempotent - won't error if constraint already exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'markets_listener_id_token_id_key'
    ) THEN
        ALTER TABLE markets ADD CONSTRAINT markets_listener_id_token_id_key
            UNIQUE (listener_id, token_id);
    END IF;
END $$;

-- Add an index on condition_id for efficient lookups (not unique, just for queries)
CREATE INDEX IF NOT EXISTS idx_markets_listener_condition ON markets(listener_id, condition_id);
