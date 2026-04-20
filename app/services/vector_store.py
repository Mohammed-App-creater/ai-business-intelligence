"""
vector_store.py
===============
Read/write interface to the pgvector ``embeddings`` table.

The document generator writes here; the retriever reads from here.
``EmbeddingClient`` is not imported here — only vectors are stored and searched.

``DOMAINS`` and ``DOC_TYPES`` document allowed values; the doc generator chooses
concrete strings (no runtime validation).

doc_id naming (generator / retriever contract)
----------------------------------------------
- ``{tenant_id}_revenue_monthly_{YYYY_MM}``          e.g. ``42_revenue_monthly_2026_01``
- ``{tenant_id}_revenue_daily_{YYYY_MM}``            e.g. ``42_revenue_daily_2026_01``
- ``{tenant_id}_revenue_location_{YYYY_MM}``         e.g. ``42_revenue_location_2026_01``
- ``{tenant_id}_staff_monthly_{YYYY_MM}``            e.g. ``42_staff_monthly_2026_01``
- ``{tenant_id}_staff_{emp_id}_{YYYY_MM}``           e.g. ``42_staff_7_2026_01``
- ``{tenant_id}_staff_ranking_{YYYY_MM}``            e.g. ``42_staff_ranking_2026_01``
- ``{tenant_id}_services_monthly_{YYYY_MM}``         e.g. ``42_services_monthly_2026_01``
- ``{tenant_id}_service_{svc_id}_{YYYY_MM}``         e.g. ``42_service_3_2026_01``
- ``{tenant_id}_clients_retention_{YYYY_MM}``        e.g. ``42_clients_retention_2026_01``
- ``{tenant_id}_clients_top_{YYYY_MM}``              e.g. ``42_clients_top_2026_01``
- ``{tenant_id}_appointments_monthly_{YYYY_MM}``     e.g. ``42_appointments_monthly_2026_01``
- ``{tenant_id}_expenses_monthly_{YYYY_MM}``         e.g. ``42_expenses_monthly_2026_01``
- ``{tenant_id}_reviews_monthly_{YYYY_MM}``          e.g. ``42_reviews_monthly_2026_01``
- ``{tenant_id}_payments_monthly_{YYYY_MM}``         e.g. ``42_payments_monthly_2026_01``
- ``{tenant_id}_campaigns_monthly_{YYYY_MM}``        e.g. ``42_campaigns_monthly_2026_01``
- ``{tenant_id}_attendance_monthly_{YYYY_MM}``       e.g. ``42_attendance_monthly_2026_01``
- ``{tenant_id}_subscriptions_monthly_{YYYY_MM}``    e.g. ``42_subscriptions_monthly_2026_01``
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

DOMAINS: frozenset[str] = frozenset(
    {
        "revenue",
        "staff",
        "services",
        "clients",
        "appointments",
        "expenses",
        "reviews",
        "payments",
        "campaigns",
        "attendance",
        "subscriptions",
    }
)

DOC_TYPES: frozenset[str] = frozenset(
    {
        "monthly_summary",
        "daily_trend",
        "individual",
        "ranking",
        "location_breakdown",
        "top_spenders",
        "retention_summary",
    }
)


class VectorStore:
    """Read/write interface to the pgvector ``embeddings`` table."""

    def __init__(self, pool) -> None:
        self._pool = pool
        self._logger = logging.getLogger(__name__)

    @classmethod
    def from_pool(cls, pool) -> VectorStore:
        return cls(pool)

    @staticmethod
    def _vec(embedding: list[float]) -> str:
        """Convert list[float] → pgvector string '[0.00100000,...,0.00200000]'"""
        return "[" + ",".join(f"{v:.8f}" for v in embedding) + "]"

    async def upsert(
        self,
        tenant_id: str,
        doc_id: str,
        doc_domain: str,
        doc_type: str,
        chunk_text: str,
        embedding: list[float],
        period_start: date | None = None,
        metadata: dict | None = None,
    ) -> str:
        sql = """
INSERT INTO embeddings
    (tenant_id, doc_id, doc_domain, doc_type, chunk_text,
     embedding, period_start, metadata, updated_at)
VALUES ($1, $2, $3, $4, $5, $6::vector, $7, $8::jsonb, now())
ON CONFLICT (tenant_id, doc_id) DO UPDATE SET
    doc_domain   = EXCLUDED.doc_domain,
    doc_type     = EXCLUDED.doc_type,
    chunk_text   = EXCLUDED.chunk_text,
    embedding    = EXCLUDED.embedding,
    period_start = EXCLUDED.period_start,
    metadata     = EXCLUDED.metadata,
    updated_at   = now()
RETURNING id::text
""".strip()
        meta = metadata if metadata is not None else {}
        emb = self._vec(embedding)
        async with self._pool.acquire() as conn:
            row_id = await conn.fetchval(
                sql,
                tenant_id,
                doc_id,
                doc_domain,
                doc_type,
                chunk_text,
                emb,
                period_start,
                json.dumps(meta),
            )
        return str(row_id)

    async def upsert_many(self, documents: list[dict]) -> int:
        n = 0
        for doc in documents:
            try:
                await self.upsert(
                    tenant_id=doc["tenant_id"],
                    doc_id=doc["doc_id"],
                    doc_domain=doc["doc_domain"],
                    doc_type=doc["doc_type"],
                    chunk_text=doc["chunk_text"],
                    embedding=doc["embedding"],
                    period_start=doc.get("period_start"),
                    metadata=doc.get("metadata"),
                )
                n += 1
            except Exception:
                self._logger.exception("upsert_many: failed for doc_id=%s", doc.get("doc_id"))
        return n

    def _build_search_sql_and_params(
        self,
        tenant_id: str,
        query_embedding: list[float],
        top_k: int,
        doc_domain: str | None,
        doc_type: str | None,
        since_date: date | None,
        exclude_rollup: bool = False,
    ) -> tuple[str, list]:
        params: list = [tenant_id, self._vec(query_embedding)]
        conditions = ["tenant_id = $1"]
        idx = 3
        if doc_domain is not None:
            conditions.append(f"doc_domain = ${idx}")
            params.append(doc_domain)
            idx += 1
        if doc_type is not None:
            conditions.append(f"doc_type = ${idx}")
            params.append(doc_type)
            idx += 1
        if since_date is not None:
            conditions.append(f"(period_start >= ${idx} OR period_start IS NULL)")
            params.append(since_date)
            idx += 1
        if exclude_rollup:
            conditions.append("(metadata->>'location_id')::int != 0")
        limit_idx = idx
        params.append(top_k)
        where_clause = " AND ".join(conditions)
        sql = f"""
SELECT
    id::text        AS id,
    doc_id,
    doc_domain,
    doc_type,
    chunk_text,
    period_start,
    metadata,
    created_at,
    updated_at,
    1 - (embedding <=> $2::vector) AS similarity
FROM embeddings
WHERE {where_clause}
ORDER BY embedding <=> $2::vector
LIMIT ${limit_idx}
""".strip()
        return sql, params

    async def search(
        self,
        tenant_id: str,
        query_embedding: list[float],
        top_k: int = 5,
        doc_domain: str | None = None,
        doc_type: str | None = None,
        since_date: date | None = None,
        exclude_rollup: bool = False,
    ) -> list[dict]:
        sql, params = self._build_search_sql_and_params(
            tenant_id,
            query_embedding,
            top_k,
            doc_domain,
            doc_type,
            since_date,
            exclude_rollup,
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(r) for r in rows]

    async def search_multi_domain(
        self,
        tenant_id: str,
        query_embedding: list[float],
        domains: list[str],
        top_k_per_domain: int = 3,
        since_date: date | None = None,
        exclude_rollup: bool = False,
    ) -> list[dict]:
        merged: list[dict] = []
        for domain in domains:
            part = await self.search(
                tenant_id=tenant_id,
                query_embedding=query_embedding,
                top_k=top_k_per_domain,
                doc_domain=domain,
                since_date=since_date,
                exclude_rollup=exclude_rollup,
            )
            merged.extend(part)
        return self._dedupe_sort_by_similarity(merged)

    async def search_multi_type(
        self,
        tenant_id: str,
        query_embedding: list[float],
        doc_domain: str,
        doc_types: list[str],
        top_k_per_type: int = 3,
        since_date: date | None = None,
    ) -> list[dict]:
        merged: list[dict] = []
        for dt in doc_types:
            part = await self.search(
                tenant_id=tenant_id,
                query_embedding=query_embedding,
                top_k=top_k_per_type,
                doc_domain=doc_domain,
                doc_type=dt,
                since_date=since_date,
            )
            merged.extend(part)
        return self._dedupe_sort_by_similarity(merged)

    @staticmethod
    def _dedupe_sort_by_similarity(rows: list[dict]) -> list[dict]:
        best: dict[str, dict] = {}
        for r in rows:
            did = str(r["doc_id"])
            sim = float(r.get("similarity", 0.0))
            if did not in best or sim > float(best[did].get("similarity", 0.0)):
                best[did] = r
        out = list(best.values())
        out.sort(key=lambda x: float(x.get("similarity", 0.0)), reverse=True)
        return out

    async def exists(self, tenant_id: str, doc_id: str) -> bool:
        sql = """
SELECT EXISTS(
    SELECT 1 FROM embeddings WHERE tenant_id = $1 AND doc_id = $2
)
""".strip()
        async with self._pool.acquire() as conn:
            found = await conn.fetchval(sql, tenant_id, doc_id)
        return bool(found)

    async def get_doc_metadata(self, tenant_id: str, doc_id: str) -> dict | None:
        """Return JSON metadata for a row, or None if missing."""
        sql = """
SELECT metadata FROM embeddings WHERE tenant_id = $1 AND doc_id = $2
""".strip()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, tenant_id, doc_id)
        if row is None:
            return None
        m = row["metadata"]
        if m is None:
            return {}
        return dict(m) if isinstance(m, dict) else {}

    def _build_doc_id_filters(
        self,
        tenant_id: str,
        doc_domain: str | None,
        doc_type: str | None,
    ) -> tuple[str, list]:
        params: list = [tenant_id]
        conditions = ["tenant_id = $1"]
        idx = 2
        if doc_domain is not None:
            conditions.append(f"doc_domain = ${idx}")
            params.append(doc_domain)
            idx += 1
        if doc_type is not None:
            conditions.append(f"doc_type = ${idx}")
            params.append(doc_type)
            idx += 1
        where_clause = " AND ".join(conditions)
        return where_clause, params

    async def get_doc_ids(
        self,
        tenant_id: str,
        doc_domain: str | None = None,
        doc_type: str | None = None,
    ) -> list[str]:
        where_clause, params = self._build_doc_id_filters(tenant_id, doc_domain, doc_type)
        sql = f"""
SELECT doc_id FROM embeddings
WHERE {where_clause}
ORDER BY doc_id
""".strip()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [str(r["doc_id"]) for r in rows]

    async def delete(self, tenant_id: str, doc_id: str) -> bool:
        sql = """
DELETE FROM embeddings
WHERE tenant_id = $1 AND doc_id = $2
RETURNING id
""".strip()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, tenant_id, doc_id)
        return row is not None

    async def delete_by_domain(self, tenant_id: str, doc_domain: str) -> int:
        sql = """
DELETE FROM embeddings
WHERE tenant_id = $1 AND doc_domain = $2
RETURNING id
""".strip()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, tenant_id, doc_domain)
        return len(rows)

    async def delete_by_tenant(self, tenant_id: str) -> int:
        sql = "DELETE FROM embeddings WHERE tenant_id = $1 RETURNING id"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, tenant_id)
        return len(rows)

    async def count(
        self,
        tenant_id: str,
        doc_domain: str | None = None,
        doc_type: str | None = None,
    ) -> int:
        where_clause, params = self._build_doc_id_filters(tenant_id, doc_domain, doc_type)
        sql = f"SELECT COUNT(*) FROM embeddings WHERE {where_clause}"
        async with self._pool.acquire() as conn:
            n = await conn.fetchval(sql, *params)
        return int(n or 0)

    async def initialize_schema(self) -> None:
        sql_path = Path(__file__).resolve().parent.parent.parent / "infra" / "init_db.sql"
        sql_text = sql_path.read_text(encoding="utf-8")
        parts = [p.strip() for p in sql_text.split(";") if p.strip()]
        async with self._pool.acquire() as conn:
            for stmt in parts:
                await conn.execute(stmt)
