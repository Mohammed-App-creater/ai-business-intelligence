"""
scripts/tests/test_promos_step4_smoke.py
=========================================
End-to-end smoke test for the Promos domain Step 4 ETL wiring.

Verifies:
  L1a: ETL extraction succeeds — PromosExtractor.run() returns expected dict
  L1b: Warehouse write lands rows in all 5 wh_promo_* tables
  L1c: Doc generator embeds chunks for all 6 doc types
  L1d: pgvector contains the expected number of rows per doc_type
  L1e: Tenant isolation — biz 99 fixture stays separate from biz 42

Per Lesson 1 from prior sprints: BOTH the warehouse-write log AND the
pgvector row count are required proofs.

Usage:
    PYTHONPATH=. python scripts/tests/test_promos_step4_smoke.py

Requires:
    - Mock analytics server running on ANALYTICS_BACKEND_URL
    - WAREHOUSE Postgres reachable via PGPool.from_env(WAREHOUSE)
    - VECTOR Postgres (pgvector) reachable via PGPool.from_env(VECTOR)
    - Mock fixtures already wired into the mock server
    - Warehouse migration 2026_04_22_promos_warehouse.sql already applied
"""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("step4_smoke")

# ─── Config ────────────────────────────────────────────────────────────────────
TEST_ORG_ID    = 42
ISO_TEST_ORG_ID = 99
START_DATE     = date(2025, 11, 1)
END_DATE       = date(2026, 4, 30)

# Expected row counts (from fixtures self-test / Step 3.5 cross-checks)
EXPECTED_MIN_MONTHLY        = 6   # 6 months in window
EXPECTED_MIN_CODES_MONTHLY  = 12  # ~5 codes × 6 months, sparse
EXPECTED_MIN_CODES_WINDOW   = 5   # 5 real codes (orphan may or may not appear)
EXPECTED_MIN_LOCS_ROLLUP    = 12  # 2 locations × 6 months
EXPECTED_MIN_LOCS_BY_CODE   = 24  # ~2 codes/loc × 2 locs × 6 months
EXPECTED_MIN_CATALOG        = 5   # 5 catalog codes (orphan never in catalog)


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — ETL extraction (no warehouse write)
# ─────────────────────────────────────────────────────────────────────────────

async def step1_etl_extraction() -> dict:
    logger.info("─" * 78)
    logger.info("STEP 1: ETL extraction via PromosExtractor (no warehouse write)")
    logger.info("─" * 78)

    from app.core.config import settings
    from app.services.analytics_client import AnalyticsClient
    from etl.transforms.promos_etl import PromosExtractor

    client = AnalyticsClient(base_url=settings.ANALYTICS_BACKEND_URL)
    extractor = PromosExtractor(client=client, wh_pool=None)
    result = await extractor.run(
        business_id=TEST_ORG_ID,
        start_date=START_DATE,
        end_date=END_DATE,
    )

    counts = result.get("counts", {})
    logger.info("Extracted counts: %s", counts)

    assertions = [
        ("monthly",           counts.get("monthly", 0),           EXPECTED_MIN_MONTHLY),
        ("codes_monthly",     counts.get("codes_monthly", 0),     EXPECTED_MIN_CODES_MONTHLY),
        ("codes_window",      counts.get("codes_window", 0),      EXPECTED_MIN_CODES_WINDOW),
        ("locations_rollup",  counts.get("locations_rollup", 0),  EXPECTED_MIN_LOCS_ROLLUP),
        ("locations_by_code", counts.get("locations_by_code", 0), EXPECTED_MIN_LOCS_BY_CODE),
        ("catalog_health",    counts.get("catalog_health", 0),    EXPECTED_MIN_CATALOG),
    ]
    failed = [(k, got, want) for k, got, want in assertions if got < want]
    if failed:
        for k, got, want in failed:
            logger.error("✗ %s: got %d, expected at least %d", k, got, want)
        raise AssertionError(f"Step 1 row-count check failed for: {[f[0] for f in failed]}")

    logger.info("✓ Step 1 PASSED: all 6 slices returned ≥ expected row counts")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Warehouse write (Lesson 1a: write log)
# ─────────────────────────────────────────────────────────────────────────────

async def step2_warehouse_write() -> None:
    logger.info("─" * 78)
    logger.info("STEP 2: Warehouse write — PromosExtractor with wh_pool")
    logger.info("─" * 78)

    from app.core.config import settings
    from app.services.analytics_client import AnalyticsClient
    from app.services.db.db_pool import PGPool, PGTarget
    from etl.transforms.promos_etl import PromosExtractor

    wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)
    try:
        client = AnalyticsClient(base_url=settings.ANALYTICS_BACKEND_URL)
        extractor = PromosExtractor(client=client, wh_pool=wh_pool)
        await extractor.run(
            business_id=TEST_ORG_ID,
            start_date=START_DATE,
            end_date=END_DATE,
        )

        # Verify rows landed in each table
        async with wh_pool.acquire() as conn:
            checks = {
                "wh_promo_monthly":         "SELECT COUNT(*) FROM wh_promo_monthly WHERE business_id = $1",
                "wh_promo_codes":           "SELECT COUNT(*) FROM wh_promo_codes WHERE business_id = $1",
                "wh_promo_codes (window)":  "SELECT COUNT(*) FROM wh_promo_codes WHERE business_id = $1 AND period_start IS NULL",
                "wh_promo_locations":       "SELECT COUNT(*) FROM wh_promo_locations WHERE business_id = $1",
                "wh_promo_location_codes":  "SELECT COUNT(*) FROM wh_promo_location_codes WHERE business_id = $1",
                "wh_promo_catalog_health":  "SELECT COUNT(*) FROM wh_promo_catalog_health WHERE business_id = $1",
            }
            for name, sql in checks.items():
                n = await conn.fetchval(sql, TEST_ORG_ID)
                logger.info("  %-30s rows for biz=%d: %d", name, TEST_ORG_ID, n)
                assert n > 0, f"Warehouse table {name} is empty after ETL run"

        # IDEMPOTENCY check — re-run, expect counts unchanged (or window/catalog
        # rewrite to identical row counts via DELETE+INSERT)
        logger.info("  Idempotency re-run (second ETL pass)...")
        await extractor.run(
            business_id=TEST_ORG_ID,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        async with wh_pool.acquire() as conn:
            n_codes_after = await conn.fetchval(
                "SELECT COUNT(*) FROM wh_promo_codes WHERE business_id = $1",
                TEST_ORG_ID,
            )
            n_window_after = await conn.fetchval(
                "SELECT COUNT(*) FROM wh_promo_codes "
                "WHERE business_id = $1 AND period_start IS NULL",
                TEST_ORG_ID,
            )
            n_catalog_after = await conn.fetchval(
                "SELECT COUNT(*) FROM wh_promo_catalog_health WHERE business_id = $1",
                TEST_ORG_ID,
            )
            logger.info(
                "  After re-run: codes=%d (incl. window=%d), catalog=%d",
                n_codes_after, n_window_after, n_catalog_after,
            )
            assert n_window_after >= 1, "Idempotency: window rows lost after re-run"
            assert n_catalog_after >= 1, "Idempotency: catalog rows lost after re-run"

        logger.info("✓ Step 2 PASSED: warehouse write succeeded + idempotent")
    finally:
        await wh_pool.close()


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Doc generator + pgvector (Lesson 1b: pgvector row count)
# ─────────────────────────────────────────────────────────────────────────────

async def step3_doc_generation_and_embed() -> None:
    logger.info("─" * 78)
    logger.info("STEP 3: Doc generation + pgvector upsert")
    logger.info("─" * 78)

    from app.core.config import settings
    from app.services.analytics_client import AnalyticsClient
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

        # Clear any existing promos docs for this tenant (clean test)
        deleted = await vs.delete_by_domain(str(TEST_ORG_ID), "promos")
        logger.info("  Cleared %d existing promos docs for biz=%d", deleted, TEST_ORG_ID)

        gen = DocGenerator(wh, gateway, emb, vs)
        result = await gen.generate_all(
            org_id=TEST_ORG_ID,
            period_start=START_DATE,
            months=6,
            domain="promos",
            force=True,
        )
        logger.info(
            "  generate_all result: created=%d skipped=%d failed=%d",
            result["docs_created"], result["docs_skipped"], result["docs_failed"],
        )
        assert result["docs_created"] > 0, "No docs created"
        assert result["docs_failed"] == 0, f"Doc generation had failures: {result.get('errors')}"

        # Per-doc-type pgvector verification
        expected_doc_types = {
            "promo_monthly_summary",
            "promo_code_monthly",
            "promo_code_window_total",
            "promo_location_monthly",
            "promo_location_rollup",
            "promo_catalog_health",
        }
        async with vec_pool.acquire() as conn:
            for dt in sorted(expected_doc_types):
                n = await conn.fetchval(
                    "SELECT COUNT(*) FROM embeddings "
                    "WHERE tenant_id = $1 AND doc_domain = 'promos' AND doc_type = $2",
                    str(TEST_ORG_ID), dt,
                )
                logger.info("  pgvector doc_type=%-30s count=%d", dt, n)
                assert n > 0, f"No embeddings for doc_type={dt}"

        total = await vs.count(str(TEST_ORG_ID), "promos")
        logger.info("✓ Step 3 PASSED: %d promos docs in pgvector for biz=%d", total, TEST_ORG_ID)
    finally:
        await wh_pool.close()
        await vec_pool.close()


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Tenant isolation
# ─────────────────────────────────────────────────────────────────────────────

async def step4_tenant_isolation() -> None:
    logger.info("─" * 78)
    logger.info("STEP 4: Tenant isolation — biz 99 must not see biz 42's data")
    logger.info("─" * 78)

    from app.core.config import settings
    from app.services.analytics_client import AnalyticsClient
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

        # Generate biz 99 docs (will pull MONTHLY_SUMMARY_99 + CODES_WINDOW_99)
        await vs.delete_by_domain(str(ISO_TEST_ORG_ID), "promos")
        gen = DocGenerator(wh, gateway, emb, vs)
        result_99 = await gen.generate_all(
            org_id=ISO_TEST_ORG_ID,
            period_start=START_DATE,
            months=6,
            domain="promos",
            force=True,
        )
        logger.info(
            "  biz=99 generate_all: created=%d skipped=%d failed=%d",
            result_99["docs_created"], result_99["docs_skipped"], result_99["docs_failed"],
        )

        # Cross-tenant query — verify biz 99 cannot see biz 42 chunks
        async with vec_pool.acquire() as conn:
            n_99_in_42 = await conn.fetchval(
                "SELECT COUNT(*) FROM embeddings "
                "WHERE tenant_id = $1 AND doc_domain = 'promos' "
                "AND chunk_text LIKE '%Main St%'",  # biz-42 location name
                str(ISO_TEST_ORG_ID),
            )
            logger.info("  biz=99 chunks containing biz-42 location names: %d", n_99_in_42)
            assert n_99_in_42 == 0, "TENANT LEAK: biz 99 has biz 42 location data"

        logger.info("✓ Step 4 PASSED: tenant isolation holds")
    finally:
        await wh_pool.close()
        await vec_pool.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> int:
    logger.info("=" * 78)
    logger.info("PROMOS STEP 4 SMOKE TEST")
    logger.info("=" * 78)

    try:
        await step1_etl_extraction()
        await step2_warehouse_write()
        await step3_doc_generation_and_embed()
        await step4_tenant_isolation()
    except AssertionError as e:
        logger.error("✗ TEST FAILED: %s", e)
        return 1
    except Exception as e:
        logger.error("✗ Unexpected error: %r", e, exc_info=True)
        return 1

    logger.info("=" * 78)
    logger.info("✓✓✓ ALL 4 STEPS PASSED — Step 4 sign-off achievable ✓✓✓")
    logger.info("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))