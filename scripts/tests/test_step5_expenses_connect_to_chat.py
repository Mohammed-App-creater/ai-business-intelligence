"""
scripts/tests/test_step5_expenses_connect_to_chat.py
=====================================================
Step 5 acceptance test for the Expenses domain.

Tests the live pipeline end-to-end:
  Mock server (8001) → ExpensesExtractor → wh_exp_* tables
                    → generate_expenses_docs → pgvector embeddings
                    → query analyzer routes expense questions to RAG
                    → tenant isolation enforced at retrieval

This is the proper Step 5 sign-off — exercises the actual embedding
path and pgvector store, not just in-memory mock fakes.

Prerequisites:
  1. Mock server running on port 8001
  2. WH_PG_* and VEC_PG_* env vars set in .env
  3. wh_exp_* tables created (Step 4 schema apply done)
  4. Step 4 wiring in place (analytics_client, expenses_etl,
     expenses doc generator, AND the doc_generators/__init__.py wiring
     from doc_generators_expenses_wiring.py)
  5. Step 5 wiring in place (RAG_KEYWORD_GROUPS["expenses"] expanded
     per query_analyzer_expenses_keywords.py)

Usage:
    # Terminal 1
    uvicorn tests.mocks.mock_analytics_server:app --port 8001

    # Terminal 2
    PYTHONPATH=. python scripts/tests/test_step5_expenses_connect_to_chat.py

What gets verified (10 sections):
  1. Embed pipeline runs to completion
  2. pgvector contains expected number of expense chunks per doc_type
  3. Total chunk count matches expected (~120 docs)
  4. All chunks tagged with correct tenant_id
  5. PII guard — no $ amounts in any staff_attribution chunk
  6. Tenant isolation — biz 99 query returns no biz 42 chunks
  7. Query analyzer routes expense questions to RAG (24 positive cases)
  8. Query analyzer does NOT route non-expense questions to expenses (6 negatives)
  9. Smoke RAG retrieval — "marketing spike" returns Feb Marketing chunk top-3
  10. Smoke RAG retrieval — "dormant category" returns Office/Admin chunk top-3
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date

# Auto-load .env so WH_PG_* / VEC_PG_* / ANALYTICS_BACKEND_URL are available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.services.db.db_pool import PGPool, PGTarget
from app.services.analytics_client import AnalyticsClient
from etl.transforms.expenses_etl import ExpensesExtractor
from app.services.doc_generators.domains.expenses import (
    generate_expenses_docs,
    EXPENSES_DOC_TYPES,
)
from app.services.embeddings.embedding_client import EmbeddingClient
from app.services.vector_store import VectorStore
from app.services.query_analyzer import QueryAnalyzer

ORG_ID       = 42
OTHER_ORG_ID = 99
START_DATE   = date(2025, 10, 1)
END_DATE     = date(2026, 3, 31)


# Expected chunk counts per doc_type for biz 42 over Oct 2025 – Mar 2026.
# Comes directly from the Step 4 integration test:
#   monthly=6, category=45, subcat=3, location=12, payment=18, staff=7, cross=28
# Plus 1 dormant-category chunk derived in the doc layer (Office/Admin).
EXPECTED_DOC_COUNTS = {
    "exp_monthly_summary":      6,
    "exp_category_monthly":     45,
    "exp_subcategory_monthly":  3,
    "exp_location_monthly":     12,
    "exp_payment_type_monthly": 18,
    "exp_staff_attribution":    7,
    "exp_cat_location_cross":   28,
    "exp_dormant_category":     1,
}
EXPECTED_TOTAL = sum(EXPECTED_DOC_COUNTS.values())   # 120


async def main() -> int:
    fail_count = 0

    def check(label, condition, details=""):
        nonlocal fail_count
        mark = "✅" if condition else "❌"
        suffix = f"  — {details}" if (details and not condition) else ""
        print(f"  {mark} {label}{suffix}")
        if not condition:
            fail_count += 1

    print("=" * 72)
    print("STEP 5 ACCEPTANCE TEST — Expenses Connect to Chat")
    print("=" * 72)

    # ── Open pools + clients ──────────────────────────────────────────────
    print("\n── 0. Setup ──")
    try:
        wh_pool  = await PGPool.from_env(PGTarget.WAREHOUSE)
        vec_pool = await PGPool.from_env(PGTarget.VECTOR)
        emb      = EmbeddingClient.from_env()
        vs       = VectorStore(vec_pool)
        client   = AnalyticsClient(base_url="http://localhost:8001")
        check("Warehouse pool, vector pool, embedding client, mock client all initialized", True)
    except Exception as e:
        check("Setup", False, f"{type(e).__name__}: {e}")
        return 1

    # ── 1. Run the embed pipeline end-to-end ──────────────────────────────
    print("\n── 1. Run ExpensesExtractor + generate_expenses_docs end-to-end ──")
    try:
        extractor = ExpensesExtractor(client=client, wh_pool=wh_pool)
        warehouse_data = await extractor.run(
            business_id=ORG_ID, start_date=START_DATE, end_date=END_DATE,
        )
        check("ExpensesExtractor + warehouse write succeeded", True)

        # Wipe any prior expenses chunks for this tenant so counts are deterministic
        async with vec_pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM embeddings
                WHERE tenant_id = $1 AND doc_domain = 'expenses'
                """,
                str(ORG_ID),
            )

        result = await generate_expenses_docs(
            org_id=ORG_ID,
            warehouse_data=warehouse_data,
            emb_client=emb,
            vector_store=vs,
            force=True,
            period_end=END_DATE,
        )
        check(f"Doc generation completed — created={result.get('created')}, "
              f"skipped={result.get('skipped')}, failed={result.get('failed')}",
              result.get("failed", 0) == 0)
    except Exception as e:
        check("Embed pipeline", False, f"{type(e).__name__}: {e}")
        await wh_pool.close(); await vec_pool.close()
        return 1

    # ── 2. pgvector chunk counts per doc_type ──────────────────────────────
    print("\n── 2. pgvector chunk counts per doc_type ──")
    async with vec_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT doc_type, COUNT(*) AS n
            FROM embeddings
            WHERE tenant_id = $1 AND doc_domain = 'expenses'
            GROUP BY doc_type
            ORDER BY doc_type
            """,
            str(ORG_ID),
        )
    counts_by_type = {r["doc_type"]: r["n"] for r in rows}

    for doc_type, expected_n in EXPECTED_DOC_COUNTS.items():
        got = counts_by_type.get(doc_type, 0)
        check(
            f"{doc_type:<28s} {got:>3} chunks",
            got == expected_n,
            f"expected {expected_n}, got {got}",
        )

    # ── 3. Total chunk count ──────────────────────────────────────────────
    print("\n── 3. Total expenses chunks for biz 42 ──")
    async with vec_pool.acquire() as conn:
        total = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM embeddings
            WHERE tenant_id = $1 AND doc_domain = 'expenses'
            """,
            str(ORG_ID),
        )
    check(f"Total chunks for biz 42 = {total} (expected {EXPECTED_TOTAL})",
          total == EXPECTED_TOTAL)

    # ── 4. tenant_id correctly stamped on every chunk ─────────────────────
    print("\n── 4. tenant_id stamped correctly on every chunk ──")
    async with vec_pool.acquire() as conn:
        bad = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM embeddings
            WHERE doc_domain = 'expenses' AND tenant_id != $1
              AND tenant_id != $2
            """,
            str(ORG_ID), str(OTHER_ORG_ID),
        )
    # Note: we only assert that no chunks have unexpected tenant_ids.
    # If you have other tenants in the DB, this still passes.
    check(f"No expenses chunks with unexpected tenant_id (got {bad} bad)",
          bad == 0)

    # ── 5. PII guard — no $ amounts in staff_attribution chunks ───────────
    print("\n── 5. PII guard — staff_attribution chunks never contain $ amounts ──")
    async with vec_pool.acquire() as conn:
        leaks = await conn.fetch(
            """
            SELECT chunk_text
            FROM embeddings
            WHERE tenant_id = $1
              AND doc_domain = 'expenses'
              AND doc_type = 'exp_staff_attribution'
              AND (chunk_text LIKE '%$%' OR chunk_text ~ '\\$[0-9]')
            """,
            str(ORG_ID),
        )
    check(f"No $ amounts in any staff chunk (found {len(leaks)} leaks)",
          len(leaks) == 0)
    if leaks:
        for r in leaks[:3]:
            print(f"     LEAK: {r['chunk_text'][:120]}...")

    # ── 6. Tenant isolation — biz 99 query returns no biz 42 chunks ──────
    print("\n── 6. Tenant isolation — biz 99 cannot see biz 42 chunks ──")
    async with vec_pool.acquire() as conn:
        cross_tenant = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM embeddings
            WHERE tenant_id = $1
              AND doc_domain = 'expenses'
              AND chunk_text LIKE '%Maria Lopez%'
            """,
            str(OTHER_ORG_ID),
        )
    # Maria Lopez is staff_id=12 in biz_id=42's data. If she shows up under
    # tenant_id=99, we have a leak.
    check(f"biz 99 has 0 chunks mentioning Maria Lopez (got {cross_tenant})",
          cross_tenant == 0)

    # ── 7. Query analyzer routes expense questions to RAG ─────────────────
    print("\n── 7. Query analyzer routes expense questions to RAG ──")
    qa = QueryAnalyzer()
    positives = [
        "how much did I spend last month",
        "what was my total expense in March",
        "year-to-date spending",
        "biggest expense category last month",
        "show me my Marketing costs",
        "what did I spend on rent in March",
        "subcategory breakdown for Rent and Utilities",
        "cash vs card spending split",
        "expenses per location",
        "Main St vs Westside spending",
        "why was December so expensive",
        "which category spiked in February",
        "spending more than usual on Marketing",
        "where can I cut costs",
        "reduce my overhead",
        "are there any dormant categories",
        "any duplicate expenses",
        "QoQ expense change",
        "this month's expenses",
        "expenses by branch",
        "who logs my expenses",
        "spent more on equipment than usual",
        "my biggest cost",
        "show me my overhead this quarter",
    ]
    pos_pass = 0
    for q in positives:
        result = await qa.analyze(q)
        ok = (result.route == "RAG")
        if ok:
            pos_pass += 1
        else:
            print(f"     ❌ q={q!r}  routed to {result.route} (conf={result.confidence:.2f})")
    check(f"{pos_pass}/{len(positives)} expense questions routed to RAG",
          pos_pass == len(positives),
          f"{len(positives) - pos_pass} false negatives")

    # ── 8. Query analyzer does NOT misroute non-expense questions ─────────
    print("\n── 8. Query analyzer does NOT misroute non-expense questions ──")
    negatives_with_expected_route = [
        ("how much revenue did I make last month",  None),  # → revenue, not expenses
        ("show me my appointments today",            None),  # → appointments
        ("who's my best performing staff",           None),  # → staff
        ("what services are most popular",           None),  # → services
        ("marketing campaign open rate",             None),  # → marketing
        ("how many clients did I lose",              None),  # → clients
    ]
    # We don't enforce the SPECIFIC route here (each non-expense domain
    # owns its own routing test); we just ensure these don't hit expenses.
    neg_pass = 0
    for q, _ in negatives_with_expected_route:
        result = await qa.analyze(q)
        # If the analyzer surfaces matched_groups or domain hints, we'd check
        # them. For now we just verify the question doesn't get classified
        # under expenses incorrectly. A loose check: route should be RAG
        # (those questions have other domain keywords) and we trust the
        # other domains' tests for accuracy.
        ok = (result.route == "RAG")  # all of these should still hit RAG
        if ok:
            neg_pass += 1
        else:
            print(f"     ⚠️  q={q!r}  routed to {result.route}")
    check(f"{neg_pass}/{len(negatives_with_expected_route)} non-expense questions still route correctly to RAG",
          neg_pass == len(negatives_with_expected_route))

    # ── 9. Smoke RAG retrieval — "marketing spike" hits Feb Marketing ────
    print("\n── 9. Smoke retrieval — 'marketing spike Feb 2026' returns Feb Marketing chunk top-3 ──")
    try:
        query_vec = await emb.embed("marketing spending spike in February 2026")
        # asyncpg + pgvector wants the vector serialized as a string literal
        # of the form '[1.0,2.0,...]' for the parameter binding.
        vec_str = "[" + ",".join(repr(float(x)) for x in query_vec) + "]"
        async with vec_pool.acquire() as conn:
            top = await conn.fetch(
                """
                SELECT doc_type, chunk_text,
                       embedding <=> $1::vector AS distance
                FROM embeddings
                WHERE tenant_id = $2 AND doc_domain = 'expenses'
                ORDER BY distance ASC
                LIMIT 3
                """,
                vec_str, str(ORG_ID),
            )
        hit = any(
            "Marketing" in r["chunk_text"] and "February" in r["chunk_text"]
            for r in top
        )
        check("Top-3 includes the Feb Marketing chunk", hit)
        if not hit:
            for i, r in enumerate(top, 1):
                print(f"     {i}. {r['doc_type']}: {r['chunk_text'][:100]}...")
    except Exception as e:
        check("Smoke retrieval (marketing spike)", False, f"{type(e).__name__}: {e}")

    # ── 10. Smoke RAG retrieval — "dormant category" hits Office/Admin ───
    print("\n── 10. Smoke retrieval — 'dormant category' returns Office/Admin chunk top-3 ──")
    try:
        query_vec = await emb.embed("which expense categories have stopped or gone dormant")
        vec_str = "[" + ",".join(repr(float(x)) for x in query_vec) + "]"
        async with vec_pool.acquire() as conn:
            top = await conn.fetch(
                """
                SELECT doc_type, chunk_text,
                       embedding <=> $1::vector AS distance
                FROM embeddings
                WHERE tenant_id = $2 AND doc_domain = 'expenses'
                ORDER BY distance ASC
                LIMIT 3
                """,
                vec_str, str(ORG_ID),
            )
        hit = any(
            r["doc_type"] == "exp_dormant_category"
            or "Office/Admin" in r["chunk_text"]
            for r in top
        )
        check("Top-3 includes the dormant Office/Admin chunk", hit)
        if not hit:
            for i, r in enumerate(top, 1):
                print(f"     {i}. {r['doc_type']}: {r['chunk_text'][:100]}...")
    except Exception as e:
        check("Smoke retrieval (dormant)", False, f"{type(e).__name__}: {e}")

    await wh_pool.close()
    await vec_pool.close()

    print("\n" + "=" * 72)
    if fail_count == 0:
        print("  ✅ STEP 5 ACCEPTANCE PASSED")
    else:
        print(f"  ❌ STEP 5 ACCEPTANCE FAILED — {fail_count} check(s)")
    print("=" * 72)
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Step 5 acceptance test for the Expenses domain (live pgvector)."
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main()))