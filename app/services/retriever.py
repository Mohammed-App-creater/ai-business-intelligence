"""
Retriever — RAG document retrieval
===================================
Orchestrates vector-store retrieval for RAG-routed questions.

Takes a user question + query analyzer results, embeds the question,
queries the vector store with targeted domains based on keyword groups,
and returns a ``RetrievalContext`` that the chat endpoint uses to build
``RagChatData``.

Usage::

    retriever = Retriever(embedding_client, vector_store)
    ctx = await retriever.retrieve(question, tenant_id, analysis_result)
    # ctx.documents → list[str]  (chunk_texts for prompt)
    # ctx.doc_ids   → list[str]  (for ChatResponse.sources)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain mapping — keyword group → vector store doc_domain(s)
# ---------------------------------------------------------------------------

KEYWORD_GROUP_TO_DOMAINS: dict[str, list[str] | None] = {
    "financial":        ["revenue"],
    "appointments":     ["appointments"],
    "clients":          ["clients"],
    "staff":            ["staff"],
    "services":         ["services"],
    "marketing":        ["marketing", "campaigns"],
    "analytics":        None,   # broad question → search all domains
    "time_comparisons": None,   # modifier, not a domain by itself
}

ALL_DOMAINS: list[str] = [
    "revenue", "staff", "services", "clients", "appointments",
    "marketing",
    "expenses", "reviews", "payments", "campaigns",
    "attendance", "subscriptions",
]

_LOCATION_COMPARE_PHRASES: list[str] = [
    "each location",
    "each branch",
    "compare to last month",
    "location's appointment",
    "location appointment volume",
    "by location",
    "per location",
    "between main",
    "between our",
    "between the",
    "vs westside",
    "vs main",
    "main st and",
    "and westside",
]


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class RetrievalContext:
    """Context returned by the retriever for building RagChatData."""

    documents: list[str] = field(default_factory=list)
    """Chunk texts from the vector store — ready for prompt injection."""

    doc_ids: list[str] = field(default_factory=list)
    """Document IDs — surfaced in ChatResponse.sources for provenance."""

    domains_searched: list[str] = field(default_factory=list)
    """Which doc_domains were queried."""

    total_results: int = 0
    """Number of documents returned."""

    latency_ms: float = 0.0
    """Total retrieval time (embed + search)."""


# ---------------------------------------------------------------------------
# Retriever
# ---------------------------------------------------------------------------

class Retriever:
    """
    RAG-only retriever. Embeds a question and queries the vector store.

    Parameters
    ----------
    embedding_client:
        ``EmbeddingClient`` instance — used to embed the user question.
    vector_store:
        ``VectorStore`` instance — used to search for similar documents.
    default_top_k:
        Default number of results for broad (unfiltered) searches.
    """

    def __init__(
        self,
        embedding_client: Any,
        vector_store: Any,
        default_top_k: int = 10,
    ) -> None:
        self._embedding_client = embedding_client
        self._vector_store = vector_store
        self._default_top_k = default_top_k

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def retrieve(
        self,
        question: str,
        tenant_id: str,
        analysis: Any,
        since_date: date | None = None,
    ) -> RetrievalContext:
        """
        Retrieve relevant documents for a RAG-routed question.

        Parameters
        ----------
        question:
            The raw user question.
        tenant_id:
            Tenant / business identifier for vector store filtering.
        analysis:
            ``AnalysisResult`` from the query analyzer — provides
            ``matched_keywords`` used to determine which domains to search.
        since_date:
            Optional date filter — only return documents with
            ``period_start >= since_date``.

        Returns
        -------
        ``RetrievalContext`` with documents and metadata.
        Always returns a valid object — never raises.
        """
        t0 = time.perf_counter()

        try:
            # 1. Resolve target domains from keyword groups
            domains = self._resolve_domains(analysis)

            # 2. Embed the question
            query_embedding = await self._embedding_client.embed(question)

            # 3. Search vector store with appropriate strategy
            results = await self._search(
                tenant_id,
                query_embedding,
                domains,
                since_date,
                question,
            )

            # 4. Build context — deduplicated, ordered by similarity
            ctx = self._build_context(results, domains)
            ctx.latency_ms = (time.perf_counter() - t0) * 1000

            logger.info(
                "retriever.retrieve tenant=%s domains=%s results=%d "
                "latency_ms=%.1f",
                tenant_id, ctx.domains_searched, ctx.total_results,
                ctx.latency_ms,
            )

            return ctx

        except Exception as exc:
            latency = (time.perf_counter() - t0) * 1000
            logger.warning(
                "retriever.retrieve failed tenant=%s error=%r latency_ms=%.1f",
                tenant_id, exc, latency,
            )
            return RetrievalContext(latency_ms=latency)

    # ------------------------------------------------------------------
    # Domain resolution
    # ------------------------------------------------------------------

    def _resolve_domains(self, analysis: Any) -> list[str]:
        """
        Map the analyzer's matched keywords to vector store doc_domains.

        Returns a list of domain strings. An empty list means "search all"
        (broad question).
        """
        from app.services.query_analyzer import RAG_KEYWORD_GROUPS

        matched_kws = set(getattr(analysis, "matched_keywords", []))

        if not matched_kws:
            return []

        # Find which keyword groups contain any of the matched keywords
        matched_groups: set[str] = set()
        for group_name, keywords in RAG_KEYWORD_GROUPS.items():
            if matched_kws & set(keywords):
                matched_groups.add(group_name)

        # Map groups → domains
        domains: list[str] = []
        has_broad = False

        for group in matched_groups:
            group_domains = KEYWORD_GROUP_TO_DOMAINS.get(group)
            if group_domains is None:
                # "analytics" or "time_comparisons" → broad
                has_broad = True
            else:
                domains.extend(group_domains)

        # Deduplicate while preserving order
        seen: set[str] = set()
        unique_domains: list[str] = []
        for d in domains:
            if d not in seen:
                seen.add(d)
                unique_domains.append(d)

        # If only broad groups matched (or nothing specific), search all
        if not unique_domains and has_broad:
            return []

        return unique_domains

    # ------------------------------------------------------------------
    # Search strategy
    # ------------------------------------------------------------------

    async def _search(
        self,
        tenant_id: str,
        query_embedding: list[float],
        domains: list[str],
        since_date: date | None,
        question: str,
    ) -> list[dict[str, Any]]:
        """
        Execute the appropriate vector store search based on domain count.

        - 0 domains (broad)  → search all, top_k = default_top_k
        - 1 domain (focused) → search single domain, top_k = 5
        - 2-3 domains        → search_multi_domain, top_k_per_domain = 3
        - 4+ domains         → search all, top_k = default_top_k
        """
        q_lower = question.lower()
        _needs_per_location = any(p in q_lower for p in _LOCATION_COMPARE_PHRASES)

        if len(domains) == 0 or len(domains) > 3:
            # Broad search — no domain filter
            return await self._vector_store.search(
                tenant_id=tenant_id,
                query_embedding=query_embedding,
                top_k=self._default_top_k,
                since_date=since_date,
                exclude_rollup=_needs_per_location,
            )

        if len(domains) == 1:
            # Deep single-domain search.
            # Staff domain needs more results — it has 61 docs across 3 types
            # and multi-staff questions need all active staff represented.
            # Services domain has 144 docs across 5 types — catalog docs
            # (lifecycle, dormant, new-this-year) get outscored by monthly
            # summaries at top_k=5; bump to 10 to capture both.
            if domains[0] == "staff":
                _top_k = 12
            elif domains[0] == "services":
                _top_k = 10
            else:
                _top_k = 5
            return await self._vector_store.search(
                tenant_id=tenant_id,
                query_embedding=query_embedding,
                top_k=_top_k,
                doc_domain=domains[0],
                since_date=since_date,
                exclude_rollup=_needs_per_location,
            )

        # 2-3 domains — balanced multi-domain search
        return await self._vector_store.search_multi_domain(
            tenant_id=tenant_id,
            query_embedding=query_embedding,
            domains=domains,
            top_k_per_domain=3,
            since_date=since_date,
            exclude_rollup=_needs_per_location,
        )

    # ------------------------------------------------------------------
    # Result building
    # ------------------------------------------------------------------

    def _build_context(
        self,
        results: list[dict[str, Any]],
        searched_domains: list[str],
    ) -> RetrievalContext:
        """
        Convert raw vector store results into a ``RetrievalContext``.

        Deduplicates by doc_id and preserves similarity ordering.
        """
        seen_ids: set[str] = set()
        documents: list[str] = []
        doc_ids: list[str] = []

        for r in results:
            doc_id = r.get("doc_id", "")
            chunk_text = r.get("chunk_text", "")

            if not chunk_text or doc_id in seen_ids:
                continue

            seen_ids.add(doc_id)
            documents.append(chunk_text)
            doc_ids.append(doc_id)

        return RetrievalContext(
            documents=documents,
            doc_ids=doc_ids,
            domains_searched=searched_domains if searched_domains else ALL_DOMAINS,
            total_results=len(documents),
        )