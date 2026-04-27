"""
scripts/tests/test_forms_step4_real.py
=======================================

Step 4 verification — runs against REAL services on the VM:
  - Real warehouse pool (PGPool to wh_pool)
  - Real AnalyticsClient hitting the mock server :8001
  - Real FormsExtractor end-to-end

Pre-requisites (must run first):
  1. wh_forms_tables.sql applied to warehouse
  2. forms_fixtures.py at tests/fixtures/forms_fixtures.py
  3. analytics_client.py has the 4 get_forms_* methods
  4. mock_analytics_server.py has the 4 forms endpoints
  5. Mock server running on :8001

Then run:
  PYTHONPATH=. python scripts/tests/test_forms_step4_real.py

WHAT IT VERIFIES
================
1. Mock server reachable and all 4 endpoints respond
2. AnalyticsClient methods work end-to-end
3. FormsExtractor.run() succeeds without error
4. All 4 wh_form_* tables get the expected row counts
5. Locked anchors land in the warehouse (catalog totals, lifecycle stats,
   per-form ranks, monthly counts)
6. Idempotency — re-running produces same row counts (no duplicates)
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date

from dotenv import load_dotenv
load_dotenv()

from app.services.db.db_pool import PGPool, PGTarget
from app.services.analytics_client import AnalyticsClient
from etl.transforms.forms_etl import FormsExtractor

logging.basicConfig(level=logging.WARNING, format="%(message)s")

# Constants matching the locked fixture
BUSINESS_ID    = 42
SNAPSHOT_DATE  = date(2026, 3, 31)
START_DATE     = date(2025, 1, 1)
END_DATE       = date(2026, 3, 31)


# ─────────────────────────────────────────────────────────────────────────────
# Tally helpers
# ─────────────────────────────────────────────────────────────────────────────

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
            print(f"\033[92m✓ STEP 4 REAL-ENV VERIFICATION: {self.passed}/{self.passed} PASSED\033[0m")
            return 0
        print(f"\033[91m✗ {self.failed} CHECK(S) FAILED ({self.passed} passed)\033[0m")
        for f in self.failures:
            print(f"  - {f}")
        return 1


# ─────────────────────────────────────────────────────────────────────────────
# Check 1 — ETL run completes without error
# ─────────────────────────────────────────────────────────────────────────────

async def check_etl_run(t: TestTally, analytics, wh_pool) -> dict | None:
    t.section("CHECK 1 — ETL extractor run end-to-end")
    extractor = FormsExtractor(analytics=analytics, wh=wh_pool)
    try:
        rows = await extractor.run(
            business_id=BUSINESS_ID,
            start_date=START_DATE,
            end_date=END_DATE,
            snapshot_date=SNAPSHOT_DATE,
        )
        t.ok("FormsExtractor.run() completed without exception")
    except Exception as e:
        t.fail(f"FormsExtractor.run() raised: {e}")
        return None

    # Quick shape checks on the returned rows
    if rows.get("catalog"):
        t.ok(f"catalog: dict with {len(rows['catalog'])} keys")
    else:
        t.fail("catalog: empty/None")
    if isinstance(rows.get("monthly"), list) and len(rows["monthly"]) >= 7:
        t.ok(f"monthly: {len(rows['monthly'])} rows (expected ≥7)")
    else:
        t.fail(f"monthly: unexpected count {len(rows.get('monthly') or [])}")
    if isinstance(rows.get("per_form"), list) and len(rows["per_form"]) == 4:
        t.ok(f"per_form: {len(rows['per_form'])} rows")
    else:
        t.fail(f"per_form: expected 4 rows, got {len(rows.get('per_form') or [])}")
    if rows.get("lifecycle"):
        t.ok(f"lifecycle: dict with {len(rows['lifecycle'])} keys")
    else:
        t.fail("lifecycle: empty/None — ALWAYS-EMIT contract violated!")

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Check 2 — Warehouse persistence
# ─────────────────────────────────────────────────────────────────────────────

async def check_warehouse(t: TestTally, wh_pool) -> None:
    t.section("CHECK 2 — Warehouse persistence (wh_form_* tables)")
    expected = {
        "wh_form_catalog_snapshot":    1,
        "wh_form_monthly":              7,
        "wh_form_per_form_snapshot":    4,
        "wh_form_lifecycle_snapshot":   1,
    }
    async with wh_pool.acquire() as conn:
        for tbl, expected_n in expected.items():
            n = await conn.fetchval(
                f"SELECT COUNT(*) FROM {tbl} WHERE business_id = $1",
                BUSINESS_ID)
            if n == expected_n:
                t.ok(f"{tbl}: {n} rows")
            else:
                t.fail(f"{tbl}: expected {expected_n}, got {n}")


# ─────────────────────────────────────────────────────────────────────────────
# Check 3 — Anchor numbers landed correctly
# ─────────────────────────────────────────────────────────────────────────────

async def check_anchors(t: TestTally, wh_pool) -> None:
    t.section("CHECK 3 — Locked anchors verified in warehouse")
    async with wh_pool.acquire() as conn:
        # Catalog
        c = await conn.fetchrow(
            "SELECT * FROM wh_form_catalog_snapshot "
            "WHERE business_id=$1 AND snapshot_date=$2",
            BUSINESS_ID, SNAPSHOT_DATE)
        if c is None:
            t.fail("catalog row missing")
        else:
            checks = [
                ("total_template_count", 4),
                ("active_template_count", 3),
                ("inactive_template_count", 1),
                ("active_dormant_count", 1),
                ("lifetime_submission_total", 18),
                ("recent_90d_submission_total", 12),
            ]
            for col, expected_v in checks:
                actual = c[col]
                if actual == expected_v:
                    t.ok(f"catalog.{col} = {actual}")
                else:
                    t.fail(f"catalog.{col}: expected {expected_v}, got {actual}")

        # Lifecycle
        l = await conn.fetchrow(
            "SELECT * FROM wh_form_lifecycle_snapshot "
            "WHERE business_id=$1 AND snapshot_date=$2",
            BUSINESS_ID, SNAPSHOT_DATE)
        if l is None:
            t.fail("lifecycle row missing — ALWAYS-EMIT broken")
        else:
            for col, expected_v in [
                ("total_submissions", 18),
                ("ready_count", 5),
                ("complete_count", 10),
                ("approved_count", 3),
                ("stuck_ready_count", 4),
            ]:
                actual = l[col]
                if actual == expected_v:
                    t.ok(f"lifecycle.{col} = {actual}")
                else:
                    t.fail(f"lifecycle.{col}: expected {expected_v}, got {actual}")
            # completion_rate is NUMERIC — compare with float tolerance
            cr = float(l["completion_rate_pct"])
            if abs(cr - 72.22) < 0.01:
                t.ok(f"lifecycle.completion_rate_pct = {cr}")
            else:
                t.fail(f"lifecycle.completion_rate_pct: expected 72.22, got {cr}")

        # Per-form ranks
        rows = await conn.fetch(
            "SELECT form_id, form_name, lifetime_submission_count, "
            "       is_active_dormant, rank_by_submissions "
            "FROM wh_form_per_form_snapshot "
            "WHERE business_id=$1 AND snapshot_date=$2 "
            "ORDER BY rank_by_submissions",
            BUSINESS_ID, SNAPSHOT_DATE)
        if len(rows) != 4:
            t.fail(f"per_form: expected 4 rows, got {len(rows)}")
        else:
            top = rows[0]
            if (top["form_id"] == 1 and top["lifetime_submission_count"] == 8
                and top["rank_by_submissions"] == 1):
                t.ok(f"per_form rank 1: form_id=1 'Intake Questionnaire' (8 subs)")
            else:
                t.fail(f"per_form rank 1: unexpected — {dict(top)}")

            dormant = [r for r in rows if r["is_active_dormant"]]
            if len(dormant) == 1 and dormant[0]["form_id"] == 4:
                t.ok(f"per_form active_dormant: form 4 'New Customer Welcome' (F11 target)")
            else:
                t.fail(f"per_form active_dormant: expected [form 4], got "
                       f"{[d['form_id'] for d in dormant]}")

        # Monthly — March 2026 anchor
        mar = await conn.fetchrow(
            "SELECT * FROM wh_form_monthly "
            "WHERE business_id=$1 AND period_start=$2",
            BUSINESS_ID, date(2026, 3, 1))
        if mar is None:
            t.fail("monthly: March 2026 row missing")
        else:
            if mar["submission_count"] == 5:
                t.ok(f"monthly Mar 2026 submission_count = 5")
            else:
                t.fail(f"monthly Mar 2026 submission_count: expected 5, got {mar['submission_count']}")
            mom = float(mar["mom_submission_pct"])
            if abs(mom - 25.0) < 0.01:
                t.ok(f"monthly Mar 2026 mom_submission_pct = {mom}%")
            else:
                t.fail(f"monthly Mar 2026 mom_submission_pct: expected 25.0, got {mom}")


# ─────────────────────────────────────────────────────────────────────────────
# Check 4 — Idempotency (re-run produces same row counts, no duplicates)
# ─────────────────────────────────────────────────────────────────────────────

async def check_idempotency(t: TestTally, analytics, wh_pool) -> None:
    t.section("CHECK 4 — Idempotency (ETL re-run)")
    extractor = FormsExtractor(analytics=analytics, wh=wh_pool)
    await extractor.run(
        business_id=BUSINESS_ID,
        start_date=START_DATE,
        end_date=END_DATE,
        snapshot_date=SNAPSHOT_DATE,
    )
    expected = {
        "wh_form_catalog_snapshot":    1,
        "wh_form_monthly":              7,
        "wh_form_per_form_snapshot":    4,
        "wh_form_lifecycle_snapshot":   1,
    }
    async with wh_pool.acquire() as conn:
        for tbl, expected_n in expected.items():
            n = await conn.fetchval(
                f"SELECT COUNT(*) FROM {tbl} WHERE business_id = $1",
                BUSINESS_ID)
            if n == expected_n:
                t.ok(f"{tbl}: still {n} rows after re-run (idempotent ✓)")
            else:
                t.fail(f"{tbl}: row count drifted — got {n}, expected {expected_n}")


# ─────────────────────────────────────────────────────────────────────────────
# Check 5 — Tenant isolation (biz 99 has zero forms rows)
# ─────────────────────────────────────────────────────────────────────────────

async def check_tenant_isolation(t: TestTally, wh_pool) -> None:
    t.section("CHECK 5 — Tenant isolation")
    async with wh_pool.acquire() as conn:
        for tbl in ["wh_form_catalog_snapshot", "wh_form_monthly",
                     "wh_form_per_form_snapshot", "wh_form_lifecycle_snapshot"]:
            n = await conn.fetchval(
                f"SELECT COUNT(*) FROM {tbl} WHERE business_id = 99")
            if n == 0:
                t.ok(f"{tbl}: biz 99 has 0 rows")
            else:
                t.fail(f"{tbl}: tenant leak — biz 99 has {n} rows!")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> int:
    t = TestTally()
    print("Step 4 real-environment verification — Forms (Domain 10)")
    print("=" * 60)

    import os
    base_url = os.environ.get("ANALYTICS_BACKEND_URL", "http://localhost:8001")
    print(f"  Analytics URL: {base_url}")

    wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)
    analytics = AnalyticsClient(base_url=base_url)

    try:
        rows = await check_etl_run(t, analytics, wh_pool)
        if rows:
            await check_warehouse(t, wh_pool)
            await check_anchors(t, wh_pool)
            await check_idempotency(t, analytics, wh_pool)
            await check_tenant_isolation(t, wh_pool)
    finally:
        await wh_pool.close()
        if hasattr(analytics, "close"):
            await analytics.close()

    return t.summary()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))