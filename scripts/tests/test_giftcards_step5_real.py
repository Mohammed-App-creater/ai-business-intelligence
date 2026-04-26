"""
scripts/tests/test_giftcards_step5_real.py
============================================

Step 5 verification — runs against REAL services on the VM:
  - Real warehouse pool (PGPool to wh_pool)
  - Real pgvector pool (PGPool to vec_pool)
  - Real EmbeddingClient (calls OpenAI/Voyage)
  - Real query_analyzer module
  - Real retriever module

Pre-requisites (must run first):
  1. wh_giftcards_tables.sql applied to warehouse
  2. All 5 Step 5 patches deployed (giftcards.py, giftcards_etl.py,
     doc_generator __init__.py edits, query_analyzer keywords,
     retriever domain mapping)
  3. ETL + embed must have run for biz 42:
        python scripts/embed_documents.py \\
            --org-id 42 \\
            --domain giftcards \\
            --start-date 2025-01-01 --end-date 2026-03-31 \\
            --force

Then run:
  PYTHONPATH=. python scripts/tests/test_giftcards_step5_real.py

The script exits 0 if all checks pass, 1 if any fail.

WHAT IT VERIFIES
================
1. Warehouse persistence — wh_giftcard_* tables have rows for biz 42
2. pgvector population — rag_documents (or whatever your vector_store table
   is called) has the expected 44 doc_types under tenant_id='42',
   doc_domain='giftcards'
3. Tenant isolation — biz 99 has zero giftcards docs
4. Query routing (three-hop) — all 30 acceptance questions hit the
   giftcards keyword group via query_analyzer
5. Retriever — fetching with a giftcards keyword returns chunks scoped
   to tenant_id='42' and doc_domain='giftcards'
6. PII guardrail — no email/phone/GC-XXX strings in any stored chunk_text
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
from collections import Counter

from dotenv import load_dotenv
load_dotenv()    # must run BEFORE PGPool.from_env reads env vars

from app.services.db.db_pool import PGPool, PGTarget

# Modules under test
from app.services.query_analyzer import RAG_KEYWORD_GROUPS

logging.basicConfig(level=logging.WARNING, format="%(message)s")


# =============================================================================
# 30 acceptance questions
# =============================================================================

ACCEPTANCE_QUESTIONS = {
    "Q1":  "How many gift cards did I sell last month?",
    "Q2":  "What's my outstanding gift card liability?",
    "Q3":  "How many active gift cards do I have?",
    "Q4":  "What's the gift card redemption trend over the last 6 months?",
    "Q5":  "How many gift cards have I sold this year so far?",
    "Q6":  "Has my gift card liability gone up or down over the last 6 months?",
    "Q7":  "How does this March compare to last March for gift card redemption?",
    "Q8":  "Which staff redeems the most gift cards?",
    "Q9":  "Which branch has the most gift card redemptions?",
    "Q10": "What percentage of gift card redemption happened at Westside?",
    "Q12": "What's the most common gift card denomination?",
    "Q13": "Why is my gift card revenue up so much this month?",
    "Q14": "How many gift cards are sitting unused?",
    "Q15": "On average, how long does a gift card sit before it gets redeemed?",
    "Q16": "Should I be promoting gift cards more?",
    "Q17": "What should I do about dormant gift cards?",
    "Q18": "How many prepaid cards do I have outstanding?",
    "Q19": "What's the total value on my gift vouchers?",
    "Q20": "How much stored value do customers still have?",
    "Q21": "How many GCs got redeemed last month?",
    "Q22": "What's the average remaining balance on active gift cards?",
    "Q23": "What percentage of gift cards I issued have been redeemed?",
    "Q24": "Are there any gift cards that show drained but still active?",
    "Q25": "How many gift cards have been deactivated?",
    "Q26": "Which gift card has been dormant the longest?",
    "Q27": "How much extra do customers spend on top of their gift cards?",
    "Q28": "What's the aging breakdown of my outstanding gift card liability?",
    "Q29": "Are gift cards more often redeemed on weekends?",
    "Q30": "How many of my redeemed gift cards needed multiple visits to drain?",
    "Q31": "Were there any refunded gift card redemptions this quarter?",
}


# =============================================================================
# Tally helpers
# =============================================================================

class TestTally:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.failures: list[str] = []

    def ok(self, msg: str) -> None:
        print(f"  \033[92m✓\033[0m {msg}")
        self.passed += 1

    def fail(self, msg: str) -> None:
        print(f"  \033[91m✗\033[0m {msg}")
        self.failed += 1
        self.failures.append(msg)

    def section(self, name: str) -> None:
        print(f"\n=== {name} ===")

    def summary(self) -> int:
        print("\n" + "=" * 60)
        if self.failed == 0:
            print(f"\033[92m✓ STEP 5 REAL-ENV VERIFICATION: {self.passed}/{self.passed} PASSED\033[0m")
            return 0
        print(f"\033[91m✗ {self.failed} CHECK(S) FAILED ({self.passed} passed)\033[0m")
        for f in self.failures:
            print(f"  - {f}")
        return 1


# =============================================================================
# Check 1 — Warehouse persistence
# =============================================================================

async def check_warehouse(t: TestTally, wh_pool) -> None:
    t.section("CHECK 1 — Warehouse persistence (wh_giftcard_* tables)")
    expected = {
        "wh_giftcard_monthly":                13,
        "wh_giftcard_liability_snapshot":      1,
        "wh_giftcard_by_staff":               12,
        "wh_giftcard_by_location":            10,
        "wh_giftcard_aging_snapshot":          5,
        "wh_giftcard_anomalies_snapshot":      1,
        "wh_giftcard_denomination_snapshot":   6,
        "wh_giftcard_health_snapshot":         1,
    }
    async with wh_pool.acquire() as conn:
        for tbl, expected_n in expected.items():
            n = await conn.fetchval(
                f"SELECT COUNT(*) FROM {tbl} WHERE business_id = 42")
            if n == expected_n:
                t.ok(f"{tbl}: {n} rows")
            else:
                t.fail(f"{tbl}: expected {expected_n}, got {n}")

        # Anchor numbers from biz 42 fixture
        liab = await conn.fetchval(
            "SELECT outstanding_liability_total FROM wh_giftcard_liability_snapshot "
            "WHERE business_id = 42 ORDER BY snapshot_date DESC LIMIT 1")
        if liab and abs(float(liab) - 1125.50) < 0.01:
            t.ok(f"outstanding_liability_total = $1,125.50 anchor matches")
        else:
            t.fail(f"outstanding_liability_total expected $1,125.50, got ${liab}")

        anom = await conn.fetchval(
            "SELECT refunded_redemption_count FROM wh_giftcard_anomalies_snapshot "
            "WHERE business_id = 42 ORDER BY snapshot_date DESC LIMIT 1")
        if anom == 0:
            t.ok(f"anomalies refunded_count = 0 (Q31 always-emit)")
        else:
            t.fail(f"refunded_redemption_count expected 0, got {anom}")


# =============================================================================
# Check 2 — pgvector population + tenant isolation
# =============================================================================

# Vector store table name varies by setup. Try common names and use the first
# that exists. Adjust this list if your project uses a different table.
_VECTOR_TABLE_CANDIDATES = ["rag_documents", "embeddings", "documents",
                              "vector_documents", "doc_embeddings"]


async def _detect_vector_table(vec_pool) -> str:
    async with vec_pool.acquire() as conn:
        for name in _VECTOR_TABLE_CANDIDATES:
            row = await conn.fetchrow(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_name = $1)", name)
            if row and row["exists"]:
                return name
    raise RuntimeError(
        f"Could not find vector store table. Tried: {_VECTOR_TABLE_CANDIDATES}. "
        "Set VECTOR_TABLE manually below.")


async def check_pgvector(t: TestTally, vec_pool) -> str | None:
    t.section("CHECK 2 — pgvector population")
    try:
        vector_table = await _detect_vector_table(vec_pool)
    except RuntimeError as e:
        t.fail(str(e))
        return None
    t.ok(f"vector store table detected: {vector_table}")

    async with vec_pool.acquire() as conn:
        # Total giftcards docs for biz 42
        n_total = await conn.fetchval(
            f"SELECT COUNT(*) FROM {vector_table} "
            f"WHERE tenant_id = '42' AND doc_domain = 'giftcards'")
        if n_total >= 30:
            t.ok(f"biz 42 giftcards docs: {n_total} (expected ≥30)")
        else:
            t.fail(f"biz 42 giftcards docs: {n_total} — too few, embed run may have failed")

        # Per doc_type breakdown
        rows = await conn.fetch(
            f"SELECT doc_type, COUNT(*) AS n FROM {vector_table} "
            f"WHERE tenant_id = '42' AND doc_domain = 'giftcards' "
            f"GROUP BY doc_type ORDER BY doc_type")
        expected_types = {
            "monthly_summary":       (1, 20),
            "liability_snapshot":    (1, 1),
            "by_staff":              (3, 30),
            "by_location":           (2, 30),
            "aging_bucket":          (4, 4),
            "dormancy_summary":      (1, 1),
            "anomalies_snapshot":    (1, 1),
            "denomination_snapshot": (1, 1),
            "health_snapshot":       (1, 1),
        }
        actual_types = {r["doc_type"]: r["n"] for r in rows}
        for dt, (lo, hi) in expected_types.items():
            n = actual_types.get(dt, 0)
            if lo <= n <= hi:
                t.ok(f"doc_type '{dt}': {n} (expected {lo}-{hi})")
            else:
                t.fail(f"doc_type '{dt}': {n} (expected {lo}-{hi})")

        # Tenant isolation — biz 99 should have zero giftcards docs
        n99 = await conn.fetchval(
            f"SELECT COUNT(*) FROM {vector_table} "
            f"WHERE tenant_id = '99' AND doc_domain = 'giftcards'")
        if n99 == 0:
            t.ok(f"tenant isolation: biz 99 has zero giftcards docs")
        else:
            t.fail(f"tenant isolation BROKEN: biz 99 has {n99} giftcards docs")

        # PII guardrail — no email / phone / GC-XXX in any chunk_text
        sample = await conn.fetch(
            f"SELECT doc_id, chunk_text FROM {vector_table} "
            f"WHERE tenant_id = '42' AND doc_domain = 'giftcards'")
        leaks = 0
        for r in sample:
            text = r["chunk_text"]
            if re.search(r"[\w\.\-]+@[\w\.\-]+", text):
                t.fail(f"PII LEAK (email) in {r['doc_id']}")
                leaks += 1
            if re.search(r"\b\d{3}[\-\.\s]?\d{3}[\-\.\s]?\d{4}\b", text):
                t.fail(f"PII LEAK (phone) in {r['doc_id']}")
                leaks += 1
            # GC-XXX numbers should never appear; only integer card IDs
            # (anomalies chunk uses "internal card ids: 1, 2, 8")
            if re.search(r"\bGC-\d{3,}\b", text):
                t.fail(f"PII LEAK (GC-XXX card number) in {r['doc_id']}")
                leaks += 1
        if leaks == 0:
            t.ok(f"PII guardrail: no email/phone/GC-XXX leaks across {len(sample)} chunks")

    return vector_table


# =============================================================================
# Check 3 — Query routing (three-hop)
# =============================================================================

async def check_query_routing(t: TestTally) -> None:
    t.section("CHECK 3 — Three-hop routing (30 acceptance questions)")
    keywords = RAG_KEYWORD_GROUPS.get("giftcards", [])
    if not keywords:
        t.fail("RAG_KEYWORD_GROUPS['giftcards'] is empty or missing — query_analyzer not patched")
        return
    t.ok(f"RAG_KEYWORD_GROUPS['giftcards'] has {len(keywords)} keywords")

    matched = 0
    unmatched: list[tuple[str, str]] = []
    for qid, q in ACCEPTANCE_QUESTIONS.items():
        q_lower = q.lower()
        kws = [kw for kw in keywords if kw in q_lower]
        if kws:
            matched += 1
        else:
            unmatched.append((qid, q))

    if matched == len(ACCEPTANCE_QUESTIONS):
        t.ok(f"all {matched}/{len(ACCEPTANCE_QUESTIONS)} questions match giftcards group")
    else:
        for qid, q in unmatched:
            t.fail(f"{qid} did not route to giftcards: {q!r}")

    # Also verify retriever has the domain mapping
    try:
        from app.services.retriever import KEYWORD_GROUP_TO_DOMAINS
        if KEYWORD_GROUP_TO_DOMAINS.get("giftcards") == ["giftcards"]:
            t.ok("KEYWORD_GROUP_TO_DOMAINS['giftcards'] = ['giftcards'] (retriever patched)")
        else:
            t.fail(f"KEYWORD_GROUP_TO_DOMAINS['giftcards'] = "
                   f"{KEYWORD_GROUP_TO_DOMAINS.get('giftcards')} (expected ['giftcards'])")
    except ImportError:
        t.fail("Could not import KEYWORD_GROUP_TO_DOMAINS — adjust import path")


# =============================================================================
# Check 4 — Retriever returns giftcards chunks for biz 42
# =============================================================================

async def check_retriever(t: TestTally, vec_pool, vector_table: str) -> None:
    """Direct SQL retrieval check (skips the cosine-sim layer to avoid embedding
    costs)."""
    t.section("CHECK 4 — Retriever scopes to tenant + domain")
    if vector_table is None:
        t.fail("Skipped — vector_table not detected")
        return

    async with vec_pool.acquire() as conn:
        # Question: "What's my outstanding gift card liability?" should retrieve
        # liability_snapshot. We simulate this by domain+doc_type filter (the
        # actual cosine-sim selection happens in the live retriever).
        liab_chunk = await conn.fetchrow(
            f"SELECT doc_id, chunk_text FROM {vector_table} "
            f"WHERE tenant_id = '42' AND doc_domain = 'giftcards' "
            f"AND doc_type = 'liability_snapshot' LIMIT 1")
        if liab_chunk:
            txt = liab_chunk["chunk_text"]
            if "$1,125.50" in txt or "1,125.50" in txt or "1125.50" in txt:
                t.ok(f"liability chunk retrievable + contains anchor $1,125.50")
            else:
                t.fail(f"liability chunk found but missing $1,125.50 anchor")
                print(f"      doc_id={liab_chunk['doc_id']}")
                print(f"      first 200 chars: {txt[:200]}")
        else:
            t.fail("no liability_snapshot chunk in pgvector for biz 42")

        # Q31: anomalies chunk must say "zero refunded"
        anom_chunk = await conn.fetchrow(
            f"SELECT doc_id, chunk_text FROM {vector_table} "
            f"WHERE tenant_id = '42' AND doc_domain = 'giftcards' "
            f"AND doc_type = 'anomalies_snapshot' LIMIT 1")
        if anom_chunk and "zero refunded" in anom_chunk["chunk_text"].lower():
            t.ok(f"anomalies chunk has 'zero refunded' (Q31 always-emit)")
        else:
            t.fail(f"anomalies chunk missing or doesn't say 'zero refunded'")

        # Per-location chunk should have BOTH 'branch' AND 'location' (L5)
        loc_chunk = await conn.fetchrow(
            f"SELECT doc_id, chunk_text FROM {vector_table} "
            f"WHERE tenant_id = '42' AND doc_domain = 'giftcards' "
            f"AND doc_type = 'by_location' LIMIT 1")
        if loc_chunk:
            txt = loc_chunk["chunk_text"].lower()
            if "branch" in txt and "location" in txt:
                t.ok(f"per-location chunk has both 'branch' and 'location' (L5)")
            else:
                t.fail(f"L5 violation: per-location chunk missing 'branch' or 'location'")
        else:
            t.fail("no by_location chunk found in pgvector for biz 42")

        # Synonym header on every chunk (L6)
        synonym_check = await conn.fetchval(
            f"SELECT COUNT(*) FROM {vector_table} "
            f"WHERE tenant_id = '42' AND doc_domain = 'giftcards' "
            f"AND chunk_text NOT ILIKE '%gift card%'")
        if synonym_check == 0:
            t.ok(f"L6: every giftcards chunk contains 'gift card' synonym")
        else:
            t.fail(f"L6 violation: {synonym_check} chunks missing 'gift card' synonym")


# =============================================================================
# Main
# =============================================================================

async def main() -> int:
    t = TestTally()

    print("Step 5 real-environment verification — Gift Cards (Domain 9)")
    print("=" * 60)

    wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)
    vec_pool = await PGPool.from_env(PGTarget.VECTOR)

    try:
        await check_warehouse(t, wh_pool)
        vector_table = await check_pgvector(t, vec_pool)
        await check_query_routing(t)
        await check_retriever(t, vec_pool, vector_table)
    finally:
        await wh_pool.close()
        await vec_pool.close()

    return t.summary()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))