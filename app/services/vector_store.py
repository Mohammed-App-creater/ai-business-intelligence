"""
vector_store.py
===============
Read/write interface to the pgvector ``embeddings`` table.

The document generator writes here; the retriever reads from here.
``EmbeddingClient`` is a separate dependency — call it before ``upsert`` /
``search`` when you need text → vector.

doc_id naming (generator / retriever contract)
----------------------------------------------
- ``{tenant_id}_monthly_{YYYY_MM}``              e.g. ``42_monthly_2026_01``
- ``{tenant_id}_daily_{YYYY_MM_DD}``             e.g. ``42_daily_2026_01_15``
- ``{tenant_id}_staff_{employee_id}_{YYYY_MM}``  e.g. ``42_staff_7_2026_01``
- ``{tenant_id}_service_{service_id}_{YYYY_MM}`` e.g. ``42_service_3_2026_01``
- ``{tenant_id}_client_retention_{YYYY_MM}``     e.g. ``42_client_retention_2026_01``
- ``{tenant_id}_appointments_{YYYY_MM}``         e.g. ``42_appointments_2026_01``
- ``{tenant_id}_expenses_{YYYY_MM}``             e.g. ``42_expenses_2026_01``
- ``{tenant_id}_reviews_{YYYY_MM}``              e.g. ``42_reviews_2026_01``
- ``{tenant_id}_payments_{YYYY_MM}``             e.g. ``42_payments_2026_01``
- ``{tenant_id}_campaigns_{YYYY_MM}``            e.g. ``42_campaigns_2026_01``
- ``{tenant_id}_attendance_{YYYY_MM}``           e.g. ``42_attendance_2026_01``
- ``{tenant_id}_subscriptions_{YYYY_MM}``        e.g. ``42_subscriptions_2026_01``
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

_UPSERT_SQL = """
INSERT INTO embeddings
    (tenant_id, doc_id, doc_type, chunk_text, embedding, metadata, updated_at)
VALUES ($1, $2, $3, $4, $5::vector, $6::jsonb, now())
ON CONFLICT (tenant_id, doc_id) DO UPDATE SET
    doc_type   = EXCLUDED.doc_type,
    chunk_text = EXCLUDED.chunk_text,
    embedding  = EXCLUDED.embedding,
    metadata   = EXCLUDED.metadata,
    updated_at = now()
RETURNING id::text
""".strip()

_SEARCH_SQL_BASE = """
SELECT
    id::text,
    doc_id,
    doc_type,
    chunk_text,
    metadata,
    created_at,
    updated_at,
    1 - (embedding <=> $2::vector) AS similarity
FROM embeddings
WHERE tenant_id = $1
ORDER BY embedding <=> $2::vector
LIMIT $3
""".strip()

_SEARCH_SQL_WITH_TYPE = """
SELECT
    id::text,
    doc_id,
    doc_type,
    chunk_text,
    metadata,
    created_at,
    updated_at,
    1 - (embedding <=> $2::vector) AS similarity
FROM embeddings
WHERE tenant_id = $1 AND doc_type = $4
ORDER BY embedding <=> $2::vector
LIMIT $3
""".strip()


class VectorStore:
    """
    Read/write interface to the pgvector embeddings table.

    Responsibilities:
    - Upsert documents (text + vector + metadata) per tenant
    - Similarity search by cosine distance, filtered by tenant_id
    - Delete documents by doc_id or bulk-delete by tenant
    - Check if a document already exists (for skip-if-unchanged logic)
    - Initialize the table schema (for first-run setup)

    Usage:
        pool  = await PGPool.from_env(PGTarget.VECTOR)
        store = VectorStore(pool)

        await store.upsert(
            tenant_id="42",
            doc_id="42_monthly_2026_01",
            doc_type="monthly_summary",
            chunk_text="...",
            embedding=[0.012, ...],
            metadata={"period_start": "2026-01-01"},
        )

        results = await store.search(
            tenant_id="42",
            query_embedding=[...],
            top_k=5,
            doc_type="monthly_summary",
        )
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool
        self._logger = logging.getLogger(__name__)

    @classmethod
    def from_pool(cls, pool: Any) -> VectorStore:
        return cls(pool)

    @staticmethod
    def _vec(embedding: list[float]) -> str:
        """Convert list[float] to pgvector string format."""
        return "[" + ",".join(f"{v:.8f}" for v in embedding) + "]"

    async def upsert(
        self,
        tenant_id: str,
        doc_id: str,
        doc_type: str,
        chunk_text: str,
        embedding: list[float],
        metadata: dict | None = None,
    ) -> str:
        """
        Insert or update a document embedding.
        Uses ON CONFLICT (tenant_id, doc_id) DO UPDATE.
        Returns the UUID id of the row as text.
        """
        meta = metadata if metadata is not None else {}
        emb = self._vec(embedding)
        async with self._pool.acquire() as conn:
            row_id = await conn.fetchval(
                _UPSERT_SQL,
                tenant_id,
                doc_id,
                doc_type,
                chunk_text,
                emb,
                json.dumps(meta),
            )
        return str(row_id)

    async def upsert_many(self, documents: list[dict]) -> int:
        """
        Bulk upsert. Each dict must have keys:
        tenant_id, doc_id, doc_type, chunk_text, embedding, metadata (optional).

        On individual document failure: log error, continue.
        """
        n = 0
        for doc in documents:
            try:
                await self.upsert(
                    tenant_id=doc["tenant_id"],
                    doc_id=doc["doc_id"],
                    doc_type=doc["doc_type"],
                    chunk_text=doc["chunk_text"],
                    embedding=doc["embedding"],
                    metadata=doc.get("metadata"),
                )
                n += 1
            except Exception:
                self._logger.exception("upsert_many: failed for doc_id=%s", doc.get("doc_id"))
        return n

    async def search(
        self,
        tenant_id: str,
        query_embedding: list[float],
        top_k: int = 5,
        doc_type: str | None = None,
    ) -> list[dict]:
        """
        Cosine similarity search; always filtered by tenant_id.
        Returns rows as dicts with similarity in [0, 1] (1 - cosine distance).
        """
        emb = self._vec(query_embedding)
        async with self._pool.acquire() as conn:
            if doc_type is None:
                rows = await conn.fetch(
                    _SEARCH_SQL_BASE,
                    tenant_id,
                    emb,
                    top_k,
                )
            else:
                rows = await conn.fetch(
                    _SEARCH_SQL_WITH_TYPE,
                    tenant_id,
                    emb,
                    top_k,
                    doc_type,
                )
        return [dict(r) for r in rows]

    async def search_multi_type(
        self,
        tenant_id: str,
        query_embedding: list[float],
        doc_types: list[str],
        top_k_per_type: int = 3,
    ) -> list[dict]:
        """One search per doc_type; merged and sorted by similarity descending."""
        combined: list[dict] = []
        for dt in doc_types:
            part = await self.search(
                tenant_id=tenant_id,
                query_embedding=query_embedding,
                top_k=top_k_per_type,
                doc_type=dt,
            )
            combined.extend(part)
        combined.sort(key=lambda r: r.get("similarity", 0.0), reverse=True)
        return combined

    async def exists(self, tenant_id: str, doc_id: str) -> bool:
        sql = """
            SELECT EXISTS(
                SELECT 1 FROM embeddings
                WHERE tenant_id = $1 AND doc_id = $2
            )
        """
        async with self._pool.acquire() as conn:
            found = await conn.fetchval(sql, tenant_id, doc_id)
        return bool(found)

    async def get_doc_ids(
        self,
        tenant_id: str,
        doc_type: str | None = None,
    ) -> list[str]:
        if doc_type is None:
            sql = """
                SELECT doc_id FROM embeddings
                WHERE tenant_id = $1
                ORDER BY doc_id
            """
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(sql, tenant_id)
        else:
            sql = """
                SELECT doc_id FROM embeddings
                WHERE tenant_id = $1 AND doc_type = $2
                ORDER BY doc_id
            """
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(sql, tenant_id, doc_type)
        return [str(r["doc_id"]) for r in rows]

    async def delete(self, tenant_id: str, doc_id: str) -> bool:
        sql = """
            DELETE FROM embeddings
            WHERE tenant_id = $1 AND doc_id = $2
            RETURNING id
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, tenant_id, doc_id)
        return row is not None

    async def delete_by_tenant(self, tenant_id: str) -> int:
        sql = "DELETE FROM embeddings WHERE tenant_id = $1 RETURNING id"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, tenant_id)
        return len(rows)

    async def delete_by_type(self, tenant_id: str, doc_type: str) -> int:
        sql = """
            DELETE FROM embeddings
            WHERE tenant_id = $1 AND doc_type = $2
            RETURNING id
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, tenant_id, doc_type)
        return len(rows)

    async def count(
        self,
        tenant_id: str,
        doc_type: str | None = None,
    ) -> int:
        if doc_type is None:
            sql = "SELECT COUNT(*) FROM embeddings WHERE tenant_id = $1"
            async with self._pool.acquire() as conn:
                n = await conn.fetchval(sql, tenant_id)
        else:
            sql = """
                SELECT COUNT(*) FROM embeddings
                WHERE tenant_id = $1 AND doc_type = $2
            """
            async with self._pool.acquire() as conn:
                n = await conn.fetchval(sql, tenant_id, doc_type)
        return int(n)

    async def initialize_schema(self) -> None:
        """
        Create embeddings DDL from infra/init_db.sql if not present.
        Safe to call repeatedly (IF NOT EXISTS).
        """
        sql_path = Path(__file__).resolve().parent.parent.parent / "infra" / "init_db.sql"
        sql_text = sql_path.read_text(encoding="utf-8")
        # asyncpg accepts one statement per execute()
        parts = [p.strip() for p in sql_text.split(";") if p.strip()]
        async with self._pool.acquire() as conn:
            for stmt in parts:
                await conn.execute(stmt)
