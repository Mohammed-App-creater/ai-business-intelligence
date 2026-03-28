-- pgvector embeddings (VECTOR target database)
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS embeddings (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   TEXT        NOT NULL,
    doc_id      TEXT        NOT NULL,
    doc_type    TEXT        NOT NULL,   -- 'monthly_summary','staff_summary',
                                        -- 'service_summary','appointment_summary',
                                        -- 'expense_summary','review_summary',
                                        -- 'client_summary','payment_summary',
                                        -- 'campaign_summary','attendance_summary',
                                        -- 'subscription_summary'
    chunk_text  TEXT        NOT NULL,
    embedding   vector(1536) NOT NULL,
    metadata    JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_embeddings_tenant_doc UNIQUE (tenant_id, doc_id)
);

-- HNSW index for fast cosine similarity search
CREATE INDEX IF NOT EXISTS idx_embeddings_hnsw
    ON embeddings
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- Covering index for tenant filtering
CREATE INDEX IF NOT EXISTS idx_embeddings_tenant_id
    ON embeddings (tenant_id);

-- Index for doc_type filtering
CREATE INDEX IF NOT EXISTS idx_embeddings_tenant_doctype
    ON embeddings (tenant_id, doc_type);
