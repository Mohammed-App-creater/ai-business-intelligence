"""
tests/test_step5_connect_to_chat.py
====================================
Step 5 verification — Connect to Chat.

Tests that revenue documents:
  1. Embed and land in pgvector with the right shape
  2. Are correctly routed by revenue keywords
  3. Are retrieved for the right tenant
  4. Are NEVER retrieved for a different tenant (isolation)
  5. Produce rich, non-trivial chunk_text for each doc_type

All vector store and embedding calls use lightweight in-memory fakes —
no real pgvector or OpenAI calls needed.

Run:
    pytest tests/test_step5_connect_to_chat.py -v
"""

from __future__ import annotations

import asyncio
import math
import pytest
from unittest.mock import AsyncMock

from revenue_doc_handler import (
    generate_revenue_docs,
    REVENUE_DOC_TYPES,
    _make_doc_id,
    _chunk_monthly_summary,
    _chunk_payment_type_breakdown,
    _chunk_staff_revenue,
    _chunk_location_revenue,
    _chunk_promo_impact,
    _chunk_failed_refunds,
    CHUNK_GENERATORS,
)
from query_analyzer_revenue_keywords import RAG_KEYWORD_GROUPS
from revenue_fixtures import (
    MONTHLY_SUMMARY,
    PAYMENT_TYPES,
    STAFF_REVENUE,
    LOCATION_REVENUE,
    PROMO_IMPACT,
    FAILED_REFUNDS,
)

ORG_ID = 42
OTHER_ORG_ID = 99


# ── In-memory fakes ──────────────────────────────────────────────────────────

class FakeEmbeddingClient:
    """Returns a deterministic fake embedding vector for any text."""
    DIMENSION = 1536

    async def embed(self, text: str) -> list[float]:
        # Deterministic: hash the text into a unit vector
        seed = sum(ord(c) for c in text)
        vec = [math.sin(seed + i) for i in range(self.DIMENSION)]
        norm = math.sqrt(sum(v * v for v in vec))
        return [v / norm for v in vec]


class FakeVectorStore:
    """
    In-memory vector store. Supports upsert, exists, search, search_multi_domain.
    Enforces tenant isolation on every search.
    """

    def __init__(self):
        # doc_id → {tenant_id, doc_domain, doc_type, chunk_text, embedding, metadata}
        self._store: dict[str, dict] = {}

    async def exists(self, doc_id: str) -> bool:
        return doc_id in self._store

    async def upsert(
        self,
        doc_id: str,
        tenant_id: str,
        doc_domain: str,
        doc_type: str,
        chunk_text: str,
        embedding: list[float],
        metadata: dict,
    ) -> None:
        self._store[doc_id] = {
            "doc_id":     doc_id,
            "tenant_id":  tenant_id,
            "doc_domain": doc_domain,
            "doc_type":   doc_type,
            "chunk_text": chunk_text,
            "embedding":  embedding,
            "metadata":   metadata,
        }

    def _cosine(self, a: list[float], b: list[float]) -> float:
        dot  = sum(x * y for x, y in zip(a, b))
        na   = math.sqrt(sum(x * x for x in a))
        nb   = math.sqrt(sum(x * x for x in b))
        return dot / (na * nb) if na and nb else 0.0

    async def search(
        self,
        tenant_id: str,
        query_embedding: list[float],
        top_k: int = 5,
        doc_domain: str | None = None,
        since_date=None,
    ) -> list[dict]:
        results = []
        for rec in self._store.values():
            # STRICT tenant isolation
            if rec["tenant_id"] != tenant_id:
                continue
            if doc_domain and rec["doc_domain"] != doc_domain:
                continue
            score = self._cosine(query_embedding, rec["embedding"])
            results.append({**rec, "score": score})
        results.sort(key=lambda r: r["score"], reverse=True)
        return results[:top_k]

    async def search_multi_domain(
        self,
        tenant_id: str,
        query_embedding: list[float],
        domains: list[str],
        top_k_per_domain: int = 3,
        since_date=None,
    ) -> list[dict]:
        all_results = []
        for domain in domains:
            results = await self.search(
                tenant_id, query_embedding, top_k_per_domain, doc_domain=domain
            )
            all_results.extend(results)
        all_results.sort(key=lambda r: r["score"], reverse=True)
        return all_results

    def count_for_tenant(self, tenant_id: str, domain: str | None = None) -> int:
        return sum(
            1 for r in self._store.values()
            if r["tenant_id"] == tenant_id
            and (domain is None or r["doc_domain"] == domain)
        )


# ── Build warehouse rows from fixtures ──────────────────────────────────────

def _make_warehouse_rows() -> list[dict]:
    """Simulate what RevenueExtractor.run() writes to the warehouse."""
    rows = []
    for period_row in MONTHLY_SUMMARY["data"]:
        rows.append({
            "doc_type":        "monthly_summary",
            "tenant_id":       ORG_ID,
            "period":          period_row["period"],
            "service_revenue": period_row["service_revenue"],
            "total_tips":      period_row["total_tips"],
            "total_tax":       period_row["total_tax"],
            "total_collected": period_row["total_collected"],
            "total_discounts": period_row["total_discounts"],
            "gc_redemptions":  period_row["gc_redemptions"],
            "visit_count":     period_row["visit_count"],
            "avg_ticket":      period_row["avg_ticket"],
            "mom_growth_pct":  period_row["mom_growth_pct"],
            "trend_slope":     MONTHLY_SUMMARY["meta"]["trend_slope"],
            "trend_direction": "up",
            "refund_count":    period_row["refund_count"],
            "cancel_count":    period_row["cancel_count"],
        })

    rows.append({
        "doc_type":  "payment_type_breakdown",
        "tenant_id": ORG_ID,
        "period":    "2025-01 to 2025-06",
        "breakdown": PAYMENT_TYPES["data"],
    })

    for staff_row in STAFF_REVENUE["data"]:
        rows.append({
            "doc_type":        "staff_revenue",
            "tenant_id":       ORG_ID,
            "period":          "2025-01 to 2025-06",
            "emp_id":          staff_row["emp_id"],
            "staff_name":      staff_row["staff_name"],
            "visit_count":     staff_row["visit_count"],
            "service_revenue": staff_row["service_revenue"],
            "tips_collected":  staff_row["tips_collected"],
            "avg_ticket":      staff_row["avg_ticket"],
            "revenue_rank":    staff_row["revenue_rank"],
        })

    for loc_row in LOCATION_REVENUE["data"]:
        rows.append({
            "doc_type":             "location_revenue",
            "tenant_id":            ORG_ID,
            "period":               loc_row["period"],
            "location_id":          loc_row["location_id"],
            "location_name":        loc_row["location_name"],
            "visit_count":          loc_row["visit_count"],
            "service_revenue":      loc_row["service_revenue"],
            "total_tips":           loc_row["total_tips"],
            "avg_ticket":           loc_row["avg_ticket"],
            "total_discounts":      loc_row["total_discounts"],
            "gc_redemptions":       loc_row["gc_redemptions"],
            "pct_of_total_revenue": loc_row["pct_of_total_revenue"],
            "mom_growth_pct":       loc_row["mom_growth_pct"],
        })

    rows.append({
        "doc_type":           "promo_impact",
        "tenant_id":          ORG_ID,
        "period":             "2025-01 to 2025-06",
        "total_discount_given": PROMO_IMPACT["meta"]["total_discount_all_promos"],
        "total_promo_uses":   PROMO_IMPACT["meta"]["promo_visit_count"],
        "breakdown":          PROMO_IMPACT["data"],
    })

    rows.append({
        "doc_type":              "failed_refunds",
        "tenant_id":             ORG_ID,
        "period":                "2025-01 to 2025-06",
        "total_lost_revenue":    FAILED_REFUNDS["meta"]["total_lost_revenue"],
        "total_affected_visits": FAILED_REFUNDS["meta"]["total_affected_visits"],
        "breakdown":             FAILED_REFUNDS["data"],
    })

    return rows


# ── Session fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def emb():
    return FakeEmbeddingClient()

@pytest.fixture(scope="session")
def vs():
    return FakeVectorStore()

@pytest.fixture(scope="session")
def warehouse_rows():
    return _make_warehouse_rows()

@pytest.fixture(scope="session")
def embed_result(vs, emb, warehouse_rows):
    """Run generate_revenue_docs once and reuse across all tests."""
    return asyncio.get_event_loop().run_until_complete(
        generate_revenue_docs(
            org_id=ORG_ID,
            warehouse_rows=warehouse_rows,
            embedding_client=emb,
            vector_store=vs,
            force=False,
        )
    )


# ── 1. Embedding tests ───────────────────────────────────────────────────────

class TestEmbedding:
    def test_creates_documents(self, embed_result):
        assert embed_result["docs_created"] > 0

    def test_no_failures(self, embed_result):
        assert embed_result["docs_failed"] == 0

    def test_all_six_doc_types_embedded(self, vs, embed_result):
        types = {r["doc_type"] for r in vs._store.values() if r["tenant_id"] == str(ORG_ID)}
        assert REVENUE_DOC_TYPES == types

    def test_all_docs_have_revenue_domain(self, vs, embed_result):
        for rec in vs._store.values():
            if rec["tenant_id"] == str(ORG_ID):
                assert rec["doc_domain"] == "revenue"

    def test_embedding_has_correct_dimension(self, vs, embed_result):
        for rec in vs._store.values():
            assert len(rec["embedding"]) == FakeEmbeddingClient.DIMENSION

    def test_skip_on_rerun(self, vs, emb, warehouse_rows, embed_result):
        """Second run without --force must skip all already-embedded docs."""
        result2 = asyncio.get_event_loop().run_until_complete(
            generate_revenue_docs(
                org_id=ORG_ID,
                warehouse_rows=warehouse_rows,
                embedding_client=emb,
                vector_store=vs,
                force=False,
            )
        )
        assert result2["docs_created"] == 0
        assert result2["docs_skipped"] == embed_result["docs_created"]

    def test_force_reembeds(self, vs, emb, warehouse_rows, embed_result):
        """--force must re-embed and overwrite."""
        result3 = asyncio.get_event_loop().run_until_complete(
            generate_revenue_docs(
                org_id=ORG_ID,
                warehouse_rows=warehouse_rows,
                embedding_client=emb,
                vector_store=vs,
                force=True,
            )
        )
        assert result3["docs_created"] == embed_result["docs_created"]
        assert result3["docs_skipped"] == 0


# ── 2. Keyword routing tests ─────────────────────────────────────────────────

class TestKeywordRouting:
    """Verify that revenue questions are routed to the financial keyword group."""

    FINANCIAL_KEYWORDS = set(RAG_KEYWORD_GROUPS["financial"])

    def _matches(self, question: str) -> bool:
        q = question.lower()
        return any(kw in q for kw in self.FINANCIAL_KEYWORDS)

    # Category 1 — basic facts
    def test_q1_total_revenue_last_month(self):
        assert self._matches("What was my total revenue last month?")

    def test_q2_revenue_ytd(self):
        assert self._matches("How much revenue did I make this year so far?")

    def test_q3_average_ticket(self):
        assert self._matches("What is my average ticket value per visit?")

    # Category 2 — trends
    def test_q4_month_comparison(self):
        assert self._matches("How does my revenue this month compare to last month?")

    def test_q5_trending(self):
        assert self._matches("Is my revenue trending up or down over the last 6 months?")

    def test_q6_best_worst_month(self):
        assert self._matches("Which was my best revenue month this year?")

    def test_q7_quarter_yoy(self):
        assert self._matches("How does my revenue this quarter compare to the same quarter last year?")

    # Category 3 — rankings
    def test_q8_top_staff(self):
        assert self._matches("Which staff member generated the most revenue last month?")

    def test_q9_top_location(self):
        assert self._matches("Which location brought in the most revenue this year?")

    def test_q10_payment_types(self):
        assert self._matches("What percentage of my revenue came from cash vs card vs other payment types?")

    def test_q11_gift_cards(self):
        assert self._matches("How much of my revenue came from gift cards being redeemed?")

    def test_q12_promo_cost(self):
        assert self._matches("How much revenue did promo codes cost me last month?")

    # Category 4 — root cause
    def test_q13_revenue_drop(self):
        assert self._matches("Why did my revenue drop last month?")

    def test_q14_busier_revenue(self):
        assert self._matches("My revenue went up this month but I feel like I was less busy — why?")

    def test_q15_no_shows(self):
        assert self._matches("I had a lot of no-shows last week — how much revenue did that cost me?")

    # Category 5 — advice
    def test_q16_increase_revenue(self):
        assert self._matches("What can I do to increase my revenue next month?")

    def test_q17_growing_shrinking(self):
        assert self._matches("Should I be worried about my revenue trend — is my business growing or shrinking?")

    # Edge cases
    def test_q18_tips(self):
        assert self._matches("How much in tips did my staff collect last month?")

    def test_q19_tax(self):
        assert self._matches("How much tax did I collect this month?")

    def test_q20_refunds(self):
        assert self._matches("How many visits ended with a refund or failed payment?")


# ── 3. Retrieval tests ───────────────────────────────────────────────────────

class TestRetrieval:
    def test_revenue_question_returns_results(self, vs, emb, embed_result):
        query = "What was my total revenue last month?"
        query_emb = asyncio.get_event_loop().run_until_complete(emb.embed(query))
        results = asyncio.get_event_loop().run_until_complete(
            vs.search(tenant_id=str(ORG_ID), query_embedding=query_emb, top_k=5, doc_domain="revenue")
        )
        assert len(results) > 0

    def test_monthly_summary_is_retrievable(self, vs, emb, embed_result):
        query = "revenue last month total"
        query_emb = asyncio.get_event_loop().run_until_complete(emb.embed(query))
        results = asyncio.get_event_loop().run_until_complete(
            vs.search(tenant_id=str(ORG_ID), query_embedding=query_emb, top_k=10, doc_domain="revenue")
        )
        types = {r["doc_type"] for r in results}
        assert "monthly_summary" in types

    def test_staff_revenue_is_retrievable(self, vs, emb, embed_result):
        query = "which staff member made the most revenue"
        query_emb = asyncio.get_event_loop().run_until_complete(emb.embed(query))
        results = asyncio.get_event_loop().run_until_complete(
            vs.search(tenant_id=str(ORG_ID), query_embedding=query_emb, top_k=10, doc_domain="revenue")
        )
        types = {r["doc_type"] for r in results}
        assert "staff_revenue" in types

    def test_results_are_score_sorted(self, vs, emb, embed_result):
        query = "total revenue this year"
        query_emb = asyncio.get_event_loop().run_until_complete(emb.embed(query))
        results = asyncio.get_event_loop().run_until_complete(
            vs.search(tenant_id=str(ORG_ID), query_embedding=query_emb, top_k=10, doc_domain="revenue")
        )
        scores = [r["score"] for r in results]
        assert scores == sorted(scores, reverse=True), "Results not sorted by similarity score"


# ── 4. Tenant isolation tests ────────────────────────────────────────────────

class TestTenantIsolation:
    def test_no_results_for_other_tenant(self, vs, emb, embed_result):
        """A different tenant must get zero results from ORG_ID's data."""
        query = "revenue last month"
        query_emb = asyncio.get_event_loop().run_until_complete(emb.embed(query))
        results = asyncio.get_event_loop().run_until_complete(
            vs.search(
                tenant_id=str(OTHER_ORG_ID),
                query_embedding=query_emb,
                top_k=20,
                doc_domain="revenue",
            )
        )
        assert results == [], (
            f"Tenant isolation FAILURE: tenant {OTHER_ORG_ID} retrieved "
            f"{len(results)} docs belonging to tenant {ORG_ID}"
        )

    def test_correct_tenant_count(self, vs, embed_result):
        count = vs.count_for_tenant(str(ORG_ID), domain="revenue")
        assert count == embed_result["docs_created"]

    def test_other_tenant_count_zero(self, vs, embed_result):
        count = vs.count_for_tenant(str(OTHER_ORG_ID), domain="revenue")
        assert count == 0


# ── 5. Chunk text quality tests ──────────────────────────────────────────────

class TestChunkTextQuality:
    """Verify chunk_text is rich enough for RAG — not just field dumps."""

    def _sample_monthly(self):
        return {
            "period": "2025-03", "service_revenue": 13480, "total_tips": 1720,
            "total_tax": 1078, "total_collected": 15200, "total_discounts": 310,
            "gc_redemptions": 230, "visit_count": 201, "avg_ticket": 75.62,
            "mom_growth_pct": 30.2, "trend_slope": 812.4, "trend_direction": "up",
            "refund_count": 1, "cancel_count": 4,
        }

    def test_monthly_contains_period(self):
        text = _chunk_monthly_summary(self._sample_monthly())
        assert "2025-03" in text

    def test_monthly_contains_revenue_amount(self):
        text = _chunk_monthly_summary(self._sample_monthly())
        assert "13,480" in text

    def test_monthly_contains_trend_direction(self):
        text = _chunk_monthly_summary(self._sample_monthly())
        assert "growing" in text.lower()

    def test_monthly_contains_mom_change(self):
        text = _chunk_monthly_summary(self._sample_monthly())
        assert "30.2" in text

    def test_first_period_no_comparison_language(self):
        row = {**self._sample_monthly(), "mom_growth_pct": None, "period": "2025-01"}
        text = _chunk_monthly_summary(row)
        assert "first recorded" in text.lower()

    def test_payment_type_contains_cash_and_card(self):
        row = {
            "period": "2025-01 to 2025-06",
            "breakdown": PAYMENT_TYPES["data"],
        }
        text = _chunk_payment_type_breakdown(row)
        assert "Card" in text
        assert "Cash" in text

    def test_staff_chunk_contains_name_and_rank(self):
        row = {
            "staff_name": "Maria Lopez", "period": "2025-01 to 2025-06",
            "service_revenue": 22540, "tips_collected": 3180,
            "visit_count": 318, "avg_ticket": 80.94, "revenue_rank": 1,
        }
        text = _chunk_staff_revenue(row)
        assert "Maria Lopez" in text
        assert "#1" in text

    def test_location_chunk_contains_location_name(self):
        row = {
            "location_name": "Main St", "period": "2025-03",
            "service_revenue": 7620, "pct_of_total_revenue": 56.5,
            "visit_count": 112, "avg_ticket": 75.89, "total_tips": 980,
            "total_discounts": 170, "gc_redemptions": 130, "mom_growth_pct": 30.5,
        }
        text = _chunk_location_revenue(row)
        assert "Main St" in text
        assert "30.5" in text

    def test_failed_refunds_mentions_no_show_gap(self):
        row = {
            "period": "2025-01 to 2025-06",
            "total_lost_revenue": 2985, "total_affected_visits": 42,
            "breakdown": FAILED_REFUNDS["data"],
        }
        text = _chunk_failed_refunds(row)
        assert "no-show" in text.lower()

    def test_all_chunk_generators_produce_output(self):
        """Every CHUNK_GENERATOR must produce non-empty text for a minimal row."""
        minimal_rows = {
            "monthly_summary":        {"period": "2025-01"},
            "payment_type_breakdown": {"period": "2025-01", "breakdown": []},
            "staff_revenue":          {"staff_name": "Test", "period": "2025-01"},
            "location_revenue":       {"location_name": "Loc", "period": "2025-01"},
            "promo_impact":           {"period": "2025-01", "breakdown": []},
            "failed_refunds":         {"period": "2025-01", "breakdown": []},
        }
        for doc_type, row in minimal_rows.items():
            fn = CHUNK_GENERATORS[doc_type]
            text = fn(row)
            assert isinstance(text, str) and len(text) > 0, \
                f"chunk generator for {doc_type} returned empty text"
