-- =============================================================================
-- pgvector extension + embeddings table
-- Run once on first deployment. Safe to re-run (IF NOT EXISTS everywhere).
-- Uses PGTarget.VECTOR pool (separate from warehouse).
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS embeddings (
    id           UUID         NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id    TEXT         NOT NULL,
    doc_id       TEXT         NOT NULL,
    doc_domain   TEXT         NOT NULL,   -- 'revenue' | 'staff' | 'services' |
                                          -- 'clients' | 'appointments' | 'expenses' |
                                          -- 'reviews' | 'payments' | 'campaigns' |
                                          -- 'attendance' | 'subscriptions'
    doc_type     TEXT         NOT NULL,   -- 'monthly_summary' | 'daily_trend' |
                                          -- 'individual' | 'ranking' |
                                          -- 'location_breakdown' | 'top_spenders' |
                                          -- 'retention_summary'
    chunk_text   TEXT         NOT NULL,
    embedding    vector(1536) NOT NULL,
    period_start DATE,                    -- promoted real column for indexed date filtering
    metadata     JSONB        NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_embeddings_tenant_doc UNIQUE (tenant_id, doc_id)
);

-- HNSW index — fast cosine similarity search
CREATE INDEX IF NOT EXISTS idx_embeddings_hnsw
    ON embeddings
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- Tenant isolation — every query filters by tenant_id
CREATE INDEX IF NOT EXISTS idx_embeddings_tenant
    ON embeddings (tenant_id);

-- Domain-filtered search — narrows candidate set before ANN
CREATE INDEX IF NOT EXISTS idx_embeddings_tenant_domain
    ON embeddings (tenant_id, doc_domain);

-- Domain + type filtered search
CREATE INDEX IF NOT EXISTS idx_embeddings_tenant_domain_type
    ON embeddings (tenant_id, doc_domain, doc_type);

-- Date range filtering — "last 6 months of revenue"
CREATE INDEX IF NOT EXISTS idx_embeddings_tenant_period
    ON embeddings (tenant_id, period_start);
