"""
scripts/tests/test_promos_step4_smoke.py
=========================================
Step 4 end-to-end smoke test for the Promos domain.

Per Lesson 1 — Step 4 is NOT done until BOTH of these are verified:
  1. ETL logs "warehouse write complete" with non-zero counts
  2. pgvector contains the expected number of doc_domain='promos' rows

This test validates BOTH proofs in one run, against the mock analytics server.

Usage:
    # Terminal 1 — start the mock server:
    PYTHONPATH=. uvicorn tests.mocks.mock_analytics_server:app --port 8001

    # Terminal 2 — run this test:
    PYTHONPATH=. python scripts/tests/test_promos_step4_smoke.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("step4_smoke")

EXPECTED_DOC_TYPES = {
    "promo_monthly_summary",
    "promo_code_monthly",
    "promo_code_window_total",
    "promo_location_monthly",
    "promo_location_rollup",
    "promo_catalog_health",
}

BUSINESS_ID = 42
START_DATE = date(2025, 11, 1)
END_DATE = date(2026, 5, 1)   # exclusive — captures Apr partial too


async def step1_etl_extraction():
    """Run the extractor against the mock server and capture counts."""
    from app.services.analytics_client import AnalyticsClient
    from app.services.db.db_pool import warehouse_pool
    from etl.transforms.promos_etl import PromosExtractor

    base_url = os.environ.get("ANALYTICS_BACKEND_URL", "http://localhost:8001")
    log.info("step1.start url=%s business_id=%s", base_url, BUSINESS_ID)

    client = AnalyticsClient(base_url=base_url)
    wh_pool = await warehouse_pool()

    extractor = PromosExtractor(analytics_client=client, wh_pool=wh_pool)
    counts = await extractor.run(
        business_id=BUSINESS_ID,
        start_date=START_DATE,
        end_date=END_DATE,
    )

    # Lesson 1 proof #1 — non-zero monthly + codes counts (the load-bearing tables)
    assert counts["monthly"] > 0, \
        f"FAIL: wh_promo_monthly empty — counts={counts}"
    assert counts["codes_monthly"] > 0, \
        f"FAIL: wh_promo_codes monthly rows empty — counts={counts}"
    assert counts["codes_window"] > 0, \
        f"FAIL: wh_promo_codes window rows empty — counts={counts}"
    assert counts["locations_rollup"] > 0, \
        f"FAIL: wh_promo_locations empty — counts={counts}"
    assert counts["catalog_health"] > 0, \
        f"FAIL: wh_promo_catalog_health empty — counts={counts}"

    log.info("✓ step1.PASS warehouse_counts=%s", counts)
    return counts


async def step2_warehouse_readback():
    """Read every row count from each warehouse table — confirms data persisted."""
    from app.services.db.db_pool import warehouse_pool
    from app.services.db.warehouse.wh_promos import WhPromos

    wh_pool = await warehouse_pool()
    wh = WhPromos(wh_pool)

    monthly       = await wh.monthly(BUSINESS_ID)
    codes_monthly = await wh.codes_monthly(BUSINESS_ID)
    codes_window  = await wh.codes_window(BUSINESS_ID)
    loc_rollup    = await wh.locations_rollup(BUSINESS_ID)
    loc_codes     = await wh.location_codes(BUSINESS_ID)
    catalog       = await wh.catalog_health(BUSINESS_ID)

    log.info(
        "step2.warehouse readback: monthly=%d codes_monthly=%d codes_window=%d "
        "loc_rollup=%d loc_codes=%d catalog=%d",
        len(monthly), len(codes_monthly), len(codes_window),
        len(loc_rollup), len(loc_codes), len(catalog),
    )

    # Sanity — every row must have business_id=42
    for table_name, rows in [
        ("monthly", monthly), ("codes_monthly", codes_monthly),
        ("codes_window", codes_window), ("loc_rollup", loc_rollup),
        ("loc_codes", loc_codes), ("catalog", catalog),
    ]:
        for r in rows:
            assert r["business_id"] == BUSINESS_ID, \
                f"FAIL tenant leak in {table_name}: row has business_id={r['business_id']}"

    # Verify NULL period_start handling (Lesson 3)
    null_period_codes = [r for r in codes_window if r["period_start"] is None]
    assert len(null_period_codes) > 0, \
        "FAIL: codes_window should have rows with period_start IS NULL"

    log.info("✓ step2.PASS all rows scoped to business_id=42, NULL periods present")


async def step3_doc_generation_and_embedding():
    """Run doc generator and verify pgvector row count (Lesson 1 proof #2)."""
    from app.services.doc_generator import DocGenerator
    from app.services.db.db_pool import warehouse_pool, vector_pool

    wh_pool  = await warehouse_pool()
    vec_pool = await vector_pool()

    doc_gen = DocGenerator(wh_pool=wh_pool, vec_pool=vec_pool)
    created, skipped, failed = await doc_gen.generate(
        business_id=BUSINESS_ID,
        domain="promos",
    )

    log.info("step3.doc_gen created=%d skipped=%d failed=%d",
             created, skipped, failed)

    assert created > 0, f"FAIL: zero promo docs created — created={created} failed={failed}"
    assert failed == 0, f"FAIL: doc generation had {failed} failures"

    # Lesson 1 proof #2 — read back from pgvector
    async with vec_pool.acquire() as conn:
        # Total promos chunks
        total = await conn.fetchval(
            """
            SELECT COUNT(*) FROM document_chunks
             WHERE tenant_id = $1 AND doc_domain = 'promos'
            """,
            str(BUSINESS_ID),
        )
        # Per doc_type breakdown
        by_type = await conn.fetch(
            """
            SELECT doc_type, COUNT(*) AS n FROM document_chunks
             WHERE tenant_id = $1 AND doc_domain = 'promos'
          GROUP BY doc_type
          ORDER BY doc_type
            """,
            str(BUSINESS_ID),
        )

    log.info("step3.pgvector rows total=%d", total)
    for r in by_type:
        log.info("  %s: %d chunks", r["doc_type"], r["n"])

    # Confirm all 6 doc types present
    actual_types = {r["doc_type"] for r in by_type}
    missing = EXPECTED_DOC_TYPES - actual_types
    assert not missing, \
        f"FAIL: pgvector missing doc_types: {missing}"

    assert total == created, \
        f"FAIL: pgvector row count {total} != doc gen created count {created}"

    log.info("✓ step3.PASS pgvector contains %d chunks across all 6 doc_types", total)


async def step4_tenant_isolation_check():
    """Confirm business_id=99 docs don't leak when querying biz=42."""
    from app.services.db.db_pool import vector_pool

    vec_pool = await vector_pool()
    async with vec_pool.acquire() as conn:
        leaks = await conn.fetchval(
            """
            SELECT COUNT(*) FROM document_chunks
             WHERE tenant_id = $1 AND doc_domain = 'promos'
               AND chunk_text LIKE '%BIZ99CODE%'
            """,
            str(BUSINESS_ID),
        )

    assert leaks == 0, \
        f"FAIL: tenant isolation breach — biz 42 has {leaks} chunks containing biz 99 data"

    log.info("✓ step4.PASS no tenant leaks detected")


async def main():
    log.info("=" * 78)
    log.info("PROMOS STEP 4 SMOKE TEST")
    log.info("=" * 78)

    try:
        await step1_etl_extraction()
        await step2_warehouse_readback()
        await step3_doc_generation_and_embedding()
        await step4_tenant_isolation_check()

        log.info("=" * 78)
        log.info("✓ ALL CHECKS PASSED — Step 4 is complete.")
        log.info("=" * 78)
        log.info("")
        log.info("Per Lesson 1, Step 4 is verified:")
        log.info("  ✓ Warehouse 'write complete' log emitted with non-zero counts")
        log.info("  ✓ pgvector contains all 6 expected doc_types")
        log.info("  ✓ Tenant isolation confirmed (no biz 99 leaks into biz 42)")
        log.info("")
        log.info("Ready for Step 5 (router keywords + chat connection).")
        return 0

    except AssertionError as e:
        log.error("✗ %s", e)
        return 1
    except Exception as e:
        log.error("✗ Unexpected error: %r", e, exc_info=True)
        return 2


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))