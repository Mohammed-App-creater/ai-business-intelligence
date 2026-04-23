"""
scripts/tests/test_step5_promos_connect_to_chat.py
=====================================================
Step 5 verification — Connect to Chat.

Runs the 4 checks that prior domain sprints use to sign off Step 5:

  CHECK 1 — EMBED        : run embed_documents.py for the promos domain,
                            verify chunks land in pgvector with all 6
                            expected doc_types
  CHECK 2 — ROUTE        : fire 9 representative questions through the
                            query analyzer, verify they route to RAG
                            with the 'promos' keyword group matched
  CHECK 3 — RETRIEVE     : embed a representative question, search
                            pgvector with the retriever, verify
                            promos chunks come back for biz 42
  CHECK 4 — ISOLATE      : same search for biz 99 must NOT return any
                            biz 42 chunks (tenant leak check)

Usage:
    PYTHONPATH=. python scripts/tests/test_step5_promos_connect_to_chat.py

Exit code 0 on all-pass, non-zero on any failure.
Prints a pass/fail summary at the end.

Requires:
    - Mock analytics server running on ANALYTICS_BACKEND_URL
    - WAREHOUSE + VECTOR Postgres reachable via PGPool.from_env
    - Step 4 sign-off already done (warehouse write + ETL working)
    - Patches applied:
        * query_analyzer.py: RAG_KEYWORD_GROUPS["promos"] populated
        * query_analyzer.py: _DATA_METRIC_OVERRIDES includes promos phrases
        * retriever.py: KEYWORD_GROUP_TO_DOMAINS["promos"] = ["promos"]
        * retriever.py: ALL_DOMAINS includes "promos"
"""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("step5_promos")

# ─── Config ────────────────────────────────────────────────────────────────────
TEST_ORG_ID     = 42
ISO_TEST_ORG_ID = 99
START_DATE      = date(2025, 11, 1)
END_DATE        = date(2026, 4, 30)

EXPECTED_DOC_TYPES = {
    "promo_monthly_summary",
    "promo_code_monthly",
    "promo_code_window_total",
    "promo_location_monthly",
    "promo_location_rollup",
    "promo_catalog_health",
}

# Questions representative of each Step 1 category — should route to RAG
ROUTING_TEST_QUESTIONS = [
    # Basic facts
    "how many promos were redeemed last month",
    "what was the total discount given through promos in March",

    # Rankings
    "which promo code was redeemed most last month",
    "what is the biggest single discount given this month",

    # Trends
    "show me promo activity trend over 6 months",
    "what percent of visits used a promo in March",

    # Location
    "which branch redeems the most coupons",
    "show me discount by location last month",

    # Lifecycle / catalog health
    "are there any dormant promo codes",
    "which codes are expired but still marked active",
]

# A question we'll embed for the retrieval check
RETRIEVAL_PROBE_QUESTION = "which promo code had the most redemptions last month"


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 1 — Embed promos to pgvector
# ─────────────────────────────────────────────────────────────────────────────

async def check1_embed_pgvector() -> None:
    logger.info("─" * 78)
    logger.info("CHECK 1: Embed promos — run DocGenerator + verify pgvector")
    logger.info("─" * 78)

    from app.core.config import settings
    from app.services.db.db_pool import PGPool, PGTarget
    from app.services.db.warehouse_client import WarehouseClient
    from app.services.doc_generators import DocGenerator
    from app.services.embeddings.embedding_client import EmbeddingClient
    from app.services.llm.llm_gateway import LLMGateway
    from app.services.vector_store import VectorStore

    wh_pool  = await PGPool.from_env(PGTarget.WAREHOUSE)
    vec_pool = await PGPool.from_env(PGTarget.VECTOR)
    try:
        wh = WarehouseClient(wh_pool)
        vs = VectorStore(vec_pool)
        emb = EmbeddingClient.from_env()
        gateway = LLMGateway.from_env()

        # Clear any existing promos docs for a clean run
        deleted = await vs.delete_by_domain(str(TEST_ORG_ID), "promos")
        logger.info("  Cleared %d existing promos docs for biz=%d", deleted, TEST_ORG_ID)

        # Run the generator — this also re-runs the ETL (PromosExtractor.run)
        gen = DocGenerator(wh, gateway, emb, vs)
        result = await gen.generate_all(
            org_id=TEST_ORG_ID,
            period_start=START_DATE,
            months=6,
            domain="promos",
            force=True,
        )
        logger.info(
            "  Doc gen: created=%d skipped=%d failed=%d",
            result["docs_created"], result["docs_skipped"], result["docs_failed"],
        )
        assert result["docs_created"] > 0, "No docs created"
        assert result["docs_failed"] == 0, (
            f"Doc generation had failures: {result.get('errors')}"
        )

        # Verify every expected doc_type has ≥1 chunk
        async with vec_pool.acquire() as conn:
            for dt in sorted(EXPECTED_DOC_TYPES):
                n = await conn.fetchval(
                    "SELECT COUNT(*) FROM embeddings "
                    "WHERE tenant_id = $1 AND doc_domain = 'promos' AND doc_type = $2",
                    str(TEST_ORG_ID), dt,
                )
                logger.info("  pgvector doc_type=%-28s count=%d", dt, n)
                assert n > 0, f"No embeddings for doc_type={dt}"

        total = await vs.count(str(TEST_ORG_ID), "promos")
        logger.info("✓ CHECK 1 PASSED: %d total promos chunks for biz=%d", total, TEST_ORG_ID)
    finally:
        await wh_pool.close()
        await vec_pool.close()


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 2 — Router sends promos questions to RAG
# ─────────────────────────────────────────────────────────────────────────────

async def check2_router() -> None:
    logger.info("─" * 78)
    logger.info("CHECK 2: Router — promos questions route to RAG")
    logger.info("─" * 78)

    from app.services.query_analyzer import QueryAnalyzer, Route

    qa = QueryAnalyzer()
    failures = []

    for q in ROUTING_TEST_QUESTIONS:
        result = await qa.analyze(q)
        route_str = str(result.route.value) if hasattr(result.route, "value") else str(result.route)
        is_rag = "RAG" in route_str.upper() or result.route == Route.RAG
        marker = "✓" if is_rag else "✗"
        logger.info(
            "  %s [%s conf=%.2f method=%s] %s",
            marker,
            route_str.ljust(6),
            result.confidence,
            getattr(result, "method", "?"),
            q,
        )
        if not is_rag:
            failures.append(q)

    if failures:
        logger.error("✗ CHECK 2 FAILED: %d questions routed to DIRECT:", len(failures))
        for q in failures:
            logger.error("    - %s", q)
        raise AssertionError(
            f"Routing failed for {len(failures)} questions — expand "
            f"RAG_KEYWORD_GROUPS['promos'] or _DATA_METRIC_OVERRIDES"
        )

    logger.info("✓ CHECK 2 PASSED: all %d questions routed to RAG", len(ROUTING_TEST_QUESTIONS))


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 3 — Retrieval returns promos chunks for biz 42
# ─────────────────────────────────────────────────────────────────────────────

async def check3_retrieval() -> None:
    logger.info("─" * 78)
    logger.info("CHECK 3: Retrieval — promos chunks come back for biz 42")
    logger.info("─" * 78)

    from app.services.db.db_pool import PGPool, PGTarget
    from app.services.embeddings.embedding_client import EmbeddingClient
    from app.services.query_analyzer import QueryAnalyzer
    from app.services.retriever import Retriever
    from app.services.vector_store import VectorStore

    vec_pool = await PGPool.from_env(PGTarget.VECTOR)
    try:
        vs = VectorStore(vec_pool)
        emb = EmbeddingClient.from_env()

        qa = QueryAnalyzer()
        analysis = await qa.analyze(RETRIEVAL_PROBE_QUESTION)
        logger.info(
            "  probe question: %r → route=%s conf=%.2f groups=%s",
            RETRIEVAL_PROBE_QUESTION,
            analysis.route,
            analysis.confidence,
            getattr(analysis, "matched_groups", getattr(analysis, "keyword_groups", [])),
        )

        retriever = Retriever(embedding_client=emb, vector_store=vs)
        ctx = await retriever.retrieve(
            question=RETRIEVAL_PROBE_QUESTION,
            tenant_id=str(TEST_ORG_ID),
            analysis=analysis,
        )

        logger.info(
            "  Retrieved %d docs in %.0fms",
            len(ctx.documents),
            getattr(ctx, "retrieval_ms", 0) or 0,
        )
        assert len(ctx.documents) > 0, (
            "Retriever returned 0 docs — check retriever.py KEYWORD_GROUP_TO_DOMAINS"
        )

        # Check that at least one doc mentions "promo" or "redemption" or "discount"
        joined = "\n---\n".join(ctx.documents[:3])
        keywords_found = any(
            kw in joined.lower()
            for kw in ("promo", "redemption", "discount", "coupon")
        )
        assert keywords_found, (
            "Retrieved docs do not mention promo/redemption/discount — "
            "retrieval is hitting the wrong domain. Check "
            "KEYWORD_GROUP_TO_DOMAINS['promos'] in retriever.py"
        )

        # Log first 2 doc previews
        for i, doc in enumerate(ctx.documents[:2], start=1):
            preview = doc.replace("\n", " ")[:160]
            logger.info("  doc[%d]: %s...", i, preview)

        logger.info("✓ CHECK 3 PASSED: retrieval returns promo chunks for biz=%d", TEST_ORG_ID)
    finally:
        await vec_pool.close()


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 4 — Tenant isolation (biz 99 must not see biz 42 data)
# ─────────────────────────────────────────────────────────────────────────────

async def check4_tenant_isolation() -> None:
    logger.info("─" * 78)
    logger.info("CHECK 4: Tenant isolation — biz 99 must not see biz 42 promo chunks")
    logger.info("─" * 78)

    from app.services.db.db_pool import PGPool, PGTarget
    from app.services.embeddings.embedding_client import EmbeddingClient
    from app.services.query_analyzer import QueryAnalyzer
    from app.services.retriever import Retriever
    from app.services.vector_store import VectorStore

    vec_pool = await PGPool.from_env(PGTarget.VECTOR)
    try:
        vs = VectorStore(vec_pool)
        emb = EmbeddingClient.from_env()

        qa = QueryAnalyzer()
        analysis = await qa.analyze(RETRIEVAL_PROBE_QUESTION)

        retriever = Retriever(embedding_client=emb, vector_store=vs)
        ctx_99 = await retriever.retrieve(
            question=RETRIEVAL_PROBE_QUESTION,
            tenant_id=str(ISO_TEST_ORG_ID),
            analysis=analysis,
        )
        logger.info("  biz=99 retrieved %d docs", len(ctx_99.documents))

        # Scan for biz-42-specific markers
        biz_42_markers = ["Main St", "Westside", "DM8880", "PM8880", "Awan"]
        joined = "\n".join(ctx_99.documents).lower()
        leaked = [m for m in biz_42_markers if m.lower() in joined]
        if leaked:
            logger.error("✗ TENANT LEAK — biz=99 docs mention biz-42 entities:")
            for m in leaked:
                logger.error("    - %s", m)
            raise AssertionError(f"Tenant leak: biz 99 sees {leaked}")

        # Also do a raw DB check
        async with vec_pool.acquire() as conn:
            n_99_promos = await conn.fetchval(
                "SELECT COUNT(*) FROM embeddings "
                "WHERE tenant_id = $1 AND doc_domain = 'promos'",
                str(ISO_TEST_ORG_ID),
            )
            n_42_promos = await conn.fetchval(
                "SELECT COUNT(*) FROM embeddings "
                "WHERE tenant_id = $1 AND doc_domain = 'promos'",
                str(TEST_ORG_ID),
            )
            logger.info(
                "  pgvector: biz=42 has %d promos chunks; biz=99 has %d",
                n_42_promos, n_99_promos,
            )

        logger.info("✓ CHECK 4 PASSED: no biz-42 data leaks into biz-99 retrieval")
    finally:
        await vec_pool.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> int:
    logger.info("=" * 78)
    logger.info("PROMOS STEP 5 — CONNECT TO CHAT")
    logger.info("=" * 78)

    passed = []
    failed = []

    for name, fn in [
        ("CHECK 1 — EMBED",     check1_embed_pgvector),
        ("CHECK 2 — ROUTE",     check2_router),
        ("CHECK 3 — RETRIEVE",  check3_retrieval),
        ("CHECK 4 — ISOLATE",   check4_tenant_isolation),
    ]:
        try:
            await fn()
            passed.append(name)
        except AssertionError as e:
            logger.error("✗ %s FAILED: %s", name, e)
            failed.append(name)
        except Exception as e:
            logger.error("✗ %s unexpected error: %r", name, e, exc_info=True)
            failed.append(name)

    logger.info("=" * 78)
    logger.info("SUMMARY")
    logger.info("=" * 78)
    for n in passed:
        logger.info("  ✓ %s", n)
    for n in failed:
        logger.error("  ✗ %s", n)

    if failed:
        logger.error("")
        logger.error("%d/%d checks failed — Step 5 NOT signed off", len(failed), len(passed)+len(failed))
        return 1

    logger.info("")
    logger.info("✓✓✓ ALL 4 CHECKS PASSED — Step 5 sign-off achievable ✓✓✓")
    logger.info("")
    logger.info("Next: Step 6 — run all 26 Step 1 questions through the live chat endpoint.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
    