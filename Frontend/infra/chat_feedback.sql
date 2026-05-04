-- =============================================================
-- chat_feedback — user feedback on assistant responses
-- Lives in the Analytics Warehouse alongside chat_history.
-- Every assistant turn the user thumbs up/down lands here.
-- =============================================================
-- The JSONB columns (feedback_categories, sources) are intentionally
-- open-ended so we can add new feedback dimensions later without a
-- schema migration. New fields just become new keys in the JSON.

CREATE TABLE IF NOT EXISTS chat_feedback (
  id                  BIGSERIAL    PRIMARY KEY,

  -- Tenant + conversation context (matches chat_history shape)
  business_id         VARCHAR(64)  NOT NULL,    -- tenant id, e.g. '40' or '42'
  session_id          UUID         NOT NULL,    -- conversation session
  message_id          UUID         NOT NULL,    -- the assistant message being rated

  -- What was asked / what was answered (denormalised on purpose so we can
  -- analyse feedback even if chat_history is later rotated/archived)
  question            TEXT         NOT NULL,
  answer              TEXT         NOT NULL,

  -- The actual feedback
  rating              VARCHAR(10)  NOT NULL CHECK (rating IN ('up', 'down')),
  feedback_text       TEXT,                     -- optional free-text reason
  feedback_categories JSONB,                    -- e.g. { "inaccurate": true, "slow": false }

  -- Provenance / response metadata for offline analysis
  sources             JSONB,                    -- the `sources` array returned by the API
  latency_ms          INTEGER,                  -- response latency at time of generation
  llm_model           VARCHAR(64),              -- optional, fill in if backend reports it

  created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Lookup feedback for a session
CREATE INDEX IF NOT EXISTS idx_chat_feedback_session
  ON chat_feedback (business_id, session_id, created_at);

-- Aggregations: thumbs-up / thumbs-down counts over time per tenant
CREATE INDEX IF NOT EXISTS idx_chat_feedback_rating
  ON chat_feedback (business_id, rating, created_at);

-- Lookup feedback for a specific assistant message (one user, one message → up to one row;
-- we keep this as a simple index rather than a unique constraint so users can change their mind)
CREATE INDEX IF NOT EXISTS idx_chat_feedback_message
  ON chat_feedback (message_id);
