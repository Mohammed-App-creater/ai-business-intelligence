"""
Tests for app/services/retriever.py
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.retriever import ALL_DOMAINS, Retriever, RetrievalContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_retriever(embed_return=None, search_return=None, multi_return=None, top_k=10):
    embedding_client = MagicMock()
    embedding_client.embed = AsyncMock(return_value=embed_return or [0.1, 0.2, 0.3])

    vector_store = MagicMock()
    vector_store.search = AsyncMock(return_value=search_return or [])
    vector_store.search_multi_domain = AsyncMock(return_value=multi_return or [])

    return Retriever(embedding_client, vector_store, default_top_k=top_k)


def _analysis(keywords: list[str]):
    return SimpleNamespace(matched_keywords=keywords)


def _doc(doc_id: str, chunk_text: str) -> dict:
    return {"doc_id": doc_id, "chunk_text": chunk_text}


# ---------------------------------------------------------------------------
# RetrievalContext defaults
# ---------------------------------------------------------------------------

class TestRetrievalContext:
    def test_defaults(self):
        ctx = RetrievalContext()
        assert ctx.documents == []
        assert ctx.doc_ids == []
        assert ctx.domains_searched == []
        assert ctx.total_results == 0
        assert ctx.latency_ms == 0.0


# ---------------------------------------------------------------------------
# _resolve_domains
# ---------------------------------------------------------------------------

class TestResolveDomains:
    def setup_method(self):
        self.r = _make_retriever()

    def test_no_keywords_returns_empty(self):
        assert self.r._resolve_domains(_analysis([])) == []

    def test_financial_keyword_maps_to_revenue(self):
        assert self.r._resolve_domains(_analysis(["revenue"])) == ["revenue"]

    def test_appointments_keyword(self):
        assert self.r._resolve_domains(_analysis(["booking"])) == ["appointments"]

    def test_clients_keyword(self):
        assert self.r._resolve_domains(_analysis(["customer"])) == ["clients"]

    def test_staff_keyword(self):
        assert self.r._resolve_domains(_analysis(["stylist"])) == ["staff"]

    def test_services_keyword(self):
        assert self.r._resolve_domains(_analysis(["treatment"])) == ["services"]

    def test_marketing_keyword(self):
        assert self.r._resolve_domains(_analysis(["campaign"])) == ["campaigns"]

    def test_analytics_keyword_returns_empty_broad(self):
        # "analytics" group → None in KEYWORD_GROUP_TO_DOMAINS → broad
        result = self.r._resolve_domains(_analysis(["report"]))
        assert result == []

    def test_time_comparisons_keyword_returns_empty_broad(self):
        result = self.r._resolve_domains(_analysis(["this month"]))
        assert result == []

    def test_unknown_keyword_returns_empty(self):
        result = self.r._resolve_domains(_analysis(["not-a-real-keyword"]))
        assert result == []

    def test_multiple_specific_groups(self):
        result = self.r._resolve_domains(_analysis(["revenue", "booking"]))
        assert set(result) == {"revenue", "appointments"}

    def test_specific_plus_broad_group_returns_specific_only(self):
        # mixing specific (financial) + broad (analytics) → returns specific
        result = self.r._resolve_domains(_analysis(["revenue", "report"]))
        assert result == ["revenue"]

    def test_deduplicates_domains(self):
        # Two financial keywords → only one "revenue" domain
        result = self.r._resolve_domains(_analysis(["revenue", "profit"]))
        assert result == ["revenue"]
        assert result.count("revenue") == 1


# ---------------------------------------------------------------------------
# _build_context
# ---------------------------------------------------------------------------

class TestBuildContext:
    def setup_method(self):
        self.r = _make_retriever()

    def test_empty_results(self):
        ctx = self.r._build_context([], [])
        assert ctx.documents == []
        assert ctx.doc_ids == []
        assert ctx.total_results == 0
        assert ctx.domains_searched == ALL_DOMAINS  # fallback when no specific domains

    def test_normal_results(self):
        results = [_doc("d1", "text one"), _doc("d2", "text two")]
        ctx = self.r._build_context(results, ["revenue"])
        assert ctx.documents == ["text one", "text two"]
        assert ctx.doc_ids == ["d1", "d2"]
        assert ctx.total_results == 2
        assert ctx.domains_searched == ["revenue"]

    def test_deduplicates_by_doc_id(self):
        results = [_doc("d1", "first"), _doc("d1", "duplicate")]
        ctx = self.r._build_context(results, ["revenue"])
        assert ctx.documents == ["first"]
        assert ctx.doc_ids == ["d1"]
        assert ctx.total_results == 1

    def test_skips_empty_chunk_text(self):
        results = [_doc("d1", ""), _doc("d2", "valid")]
        ctx = self.r._build_context(results, ["revenue"])
        assert ctx.doc_ids == ["d2"]
        assert ctx.total_results == 1

    def test_preserves_order(self):
        results = [_doc("d3", "c"), _doc("d1", "a"), _doc("d2", "b")]
        ctx = self.r._build_context(results, ["staff"])
        assert ctx.documents == ["c", "a", "b"]

    def test_empty_searched_domains_falls_back_to_all_domains(self):
        ctx = self.r._build_context([_doc("d1", "text")], [])
        assert ctx.domains_searched == ALL_DOMAINS

    def test_specific_searched_domains_preserved(self):
        ctx = self.r._build_context([], ["revenue", "clients"])
        assert ctx.domains_searched == ["revenue", "clients"]


# ---------------------------------------------------------------------------
# retrieve — integration (mocked I/O)
# ---------------------------------------------------------------------------

class TestRetrieve:
    @pytest.mark.asyncio
    async def test_broad_search_no_keywords(self):
        docs = [_doc("d1", "hello")]
        r = _make_retriever(search_return=docs)

        ctx = await r.retrieve("what is my overview?", "tenant1", _analysis([]))

        r._vector_store.search.assert_called_once()
        call_kwargs = r._vector_store.search.call_args.kwargs
        assert call_kwargs["tenant_id"] == "tenant1"
        assert call_kwargs["top_k"] == 10
        assert "doc_domain" not in call_kwargs
        assert ctx.documents == ["hello"]

    @pytest.mark.asyncio
    async def test_single_domain_uses_search_with_domain(self):
        docs = [_doc("d1", "revenue data")]
        r = _make_retriever(search_return=docs)

        ctx = await r.retrieve("what is revenue?", "tenant1", _analysis(["revenue"]))

        call_kwargs = r._vector_store.search.call_args.kwargs
        assert call_kwargs["doc_domain"] == "revenue"
        assert call_kwargs["top_k"] == 5
        assert ctx.total_results == 1

    @pytest.mark.asyncio
    async def test_two_domains_uses_search_multi_domain(self):
        docs = [_doc("d1", "mixed")]
        r = _make_retriever(multi_return=docs)

        await r.retrieve("revenue and clients?", "tenant1", _analysis(["revenue", "customer"]))

        r._vector_store.search_multi_domain.assert_called_once()
        call_kwargs = r._vector_store.search_multi_domain.call_args.kwargs
        assert set(call_kwargs["domains"]) == {"revenue", "clients"}
        assert call_kwargs["top_k_per_domain"] == 3

    @pytest.mark.asyncio
    async def test_three_domains_uses_search_multi_domain(self):
        r = _make_retriever(multi_return=[])
        await r.retrieve("q", "t1", _analysis(["revenue", "customer", "stylist"]))
        r._vector_store.search_multi_domain.assert_called_once()

    @pytest.mark.asyncio
    async def test_four_domains_falls_back_to_broad_search(self):
        # 4 domains → broad search (no domain filter)
        r = _make_retriever(search_return=[])
        await r.retrieve(
            "q", "t1",
            _analysis(["revenue", "customer", "stylist", "booking", "campaign"]),
        )
        call_kwargs = r._vector_store.search.call_args.kwargs
        assert "doc_domain" not in call_kwargs
        assert call_kwargs["top_k"] == 10

    @pytest.mark.asyncio
    async def test_since_date_forwarded_to_search(self):
        r = _make_retriever(search_return=[])
        d = date(2025, 1, 1)
        await r.retrieve("overview", "t1", _analysis([]), since_date=d)
        call_kwargs = r._vector_store.search.call_args.kwargs
        assert call_kwargs["since_date"] == d

    @pytest.mark.asyncio
    async def test_since_date_forwarded_to_single_domain_search(self):
        r = _make_retriever(search_return=[])
        d = date(2025, 3, 1)
        await r.retrieve("revenue?", "t1", _analysis(["revenue"]), since_date=d)
        call_kwargs = r._vector_store.search.call_args.kwargs
        assert call_kwargs["since_date"] == d

    @pytest.mark.asyncio
    async def test_since_date_forwarded_to_multi_domain_search(self):
        r = _make_retriever(multi_return=[])
        d = date(2025, 6, 1)
        await r.retrieve("q", "t1", _analysis(["revenue", "customer"]), since_date=d)
        call_kwargs = r._vector_store.search_multi_domain.call_args.kwargs
        assert call_kwargs["since_date"] == d

    @pytest.mark.asyncio
    async def test_embed_exception_returns_empty_context(self):
        r = _make_retriever()
        r._embedding_client.embed = AsyncMock(side_effect=RuntimeError("embed failed"))

        ctx = await r.retrieve("question", "tenant1", _analysis(["revenue"]))

        assert isinstance(ctx, RetrievalContext)
        assert ctx.documents == []
        assert ctx.total_results == 0
        assert ctx.latency_ms > 0

    @pytest.mark.asyncio
    async def test_vector_store_exception_returns_empty_context(self):
        r = _make_retriever()
        r._vector_store.search = AsyncMock(side_effect=ConnectionError("db down"))

        ctx = await r.retrieve("question", "tenant1", _analysis([]))

        assert isinstance(ctx, RetrievalContext)
        assert ctx.documents == []

    @pytest.mark.asyncio
    async def test_latency_is_set(self):
        r = _make_retriever(search_return=[])
        ctx = await r.retrieve("q", "t1", _analysis([]))
        assert ctx.latency_ms >= 0

    @pytest.mark.asyncio
    async def test_custom_top_k(self):
        r = _make_retriever(search_return=[], top_k=20)
        await r.retrieve("q", "t1", _analysis([]))
        call_kwargs = r._vector_store.search.call_args.kwargs
        assert call_kwargs["top_k"] == 20

    @pytest.mark.asyncio
    async def test_embedding_passed_to_search(self):
        embedding = [0.5, 0.6, 0.7]
        r = _make_retriever(embed_return=embedding, search_return=[])
        await r.retrieve("q", "t1", _analysis([]))
        call_kwargs = r._vector_store.search.call_args.kwargs
        assert call_kwargs["query_embedding"] == embedding
