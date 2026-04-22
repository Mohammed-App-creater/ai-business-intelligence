"""
scripts/tests/test_expenses_etl.py
===================================
Step 4 integration check for the Expenses domain.

Runs the ExpensesExtractor against the mock server (port 8001) and verifies:
  1. All 6 endpoints return data
  2. Transforms produce the expected row counts and shapes
  3. Required fields are present on every row
  4. Business_id is stamped on every row
  5. Dormant-category doc-layer logic fires correctly (1 dormant: Office/Admin)
  6. Key stories surface end-to-end (Feb Marketing spike, Dec Equipment spike)
  7. PII guard: staff attribution doc NEVER mentions total_amount_logged $

WITHOUT --warehouse:
  Uses wh_pool=None — runs the in-memory transforms only. Good for fast
  pre-warehouse checks.

WITH --warehouse:
  Opens a real PGPool via WH_PG_* env vars, hands it to the extractor,
  and adds three additional sections (10/11/12) that verify the seven
  upserts landed correctly and row counts in the warehouse match the
  in-memory transform output.

Usage:
    # Terminal 1 — mock server
    uvicorn tests.mocks.mock_analytics_server:app --port 8001

    # Terminal 2 — without warehouse (fast)
    PYTHONPATH=. python scripts/tests/test_expenses_etl.py

    # Terminal 2 — with warehouse (full Step 4 sign-off)
    PYTHONPATH=. python scripts/tests/test_expenses_etl.py --warehouse
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date

# These imports assume the project root is on sys.path.
from app.services.analytics_client import AnalyticsClient
from etl.transforms.expenses_etl import ExpensesExtractor
from app.services.doc_generators.domains.expenses import (
    CHUNK_GENERATORS,
    _detect_dormant_categories,
    _chunk_staff_attribution,
    _chunk_category_monthly,
    _chunk_monthly_summary,
)

BASE_URL = "http://localhost:8001"
BUSINESS_ID = 42
START_DATE = date(2025, 10, 1)
END_DATE   = date(2026, 3, 31)


async def main(use_warehouse: bool = False) -> int:
    fail_count = 0

    def check(label, condition, details=""):
        nonlocal fail_count
        mark = "✅" if condition else "❌"
        print(f"  {mark} {label}" + (f"  — {details}" if details else ""))
        if not condition:
            fail_count += 1

    print("=" * 68)
    print(f"STEP 4 INTEGRATION TEST — Expenses Domain"
          f"{' (with warehouse)' if use_warehouse else ' (in-memory only)'}")
    print("=" * 68)

    # ── Open warehouse pool if requested ───────────────────────────────
    wh_pool = None
    if use_warehouse:
        try:
            from app.services.db.db_pool import PGPool, PGTarget
        except ImportError as e:
            print(f"  ❌ Cannot import PGPool: {e}")
            print("     Run without --warehouse, or fix the import path.")
            return 1
        try:
            wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)
            print(f"  ✅ Warehouse pool opened (WH_PG_* env vars)")
        except Exception as e:
            print(f"  ❌ Failed to open warehouse pool: {e}")
            print("     Check WH_PG_HOST / WH_PG_USER / WH_PG_PASSWORD / WH_PG_NAME")
            return 1

    client = AnalyticsClient(base_url=BASE_URL)
    extractor = ExpensesExtractor(client=client, wh_pool=wh_pool)

    print("\n── 1. Run ExpensesExtractor end-to-end ──")
    try:
        result = await extractor.run(
            business_id=BUSINESS_ID,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        check("Extractor ran without exceptions", True)
    except Exception as e:
        check("Extractor ran without exceptions", False, str(e))
        return 1

    print("\n── 2. Row counts per slice ──")
    expected = {
        "monthly_summary":          6,
        "category_breakdown":       45,
        "subcategory_breakdown":    3,   # 3 subcats on Mar Rent only
        "location_breakdown":       12,
        "payment_type_breakdown":   18,
        "staff_attribution":        7,
        "category_location_cross":  28,
    }
    for key, exp in expected.items():
        got = len(result.get(key, []))
        check(f"{key:<28s} {got} rows", got == exp, f"expected {exp}")

    print("\n── 3. Required fields on monthly_summary ──")
    req_monthly = {
        "business_id", "period", "total_expenses", "mom_change_pct",
        "ytd_total", "current_quarter_total", "qoq_change_pct",
        "months_in_window", "expense_rank_in_window",
    }
    if result["monthly_summary"]:
        row0 = result["monthly_summary"][0]
        missing = req_monthly - set(row0.keys())
        check("All required monthly_summary fields present",
              not missing, f"missing: {sorted(missing)}")

    print("\n── 4. business_id stamped on every row ──")
    for key in expected:
        rows = result.get(key, [])
        bad = [r for r in rows if r.get("business_id") != BUSINESS_ID]
        check(f"{key}: business_id = 42 on every row",
              not bad, f"{len(bad)} bad rows")

    print("\n── 5. Dormant-category detection ──")
    dormant = _detect_dormant_categories(
        category_rows=result["category_breakdown"],
        period_end=date(2026, 3, 31),
        silence_months=3,
    )
    check("1 dormant category detected", len(dormant) == 1,
          f"got {len(dormant)}: {[d['category_name'] for d in dormant]}")
    if dormant:
        check("Dormant category is Office/Admin",
              dormant[0]["category_name"] == "Office/Admin",
              f"got {dormant[0]['category_name']}")
        check("Last active in December 2025",
              str(dormant[0]["last_active_period"])[:7] == "2025-12",
              f"got {dormant[0]['last_active_period']}")

    print("\n── 6. Story verification — Feb 2026 Marketing spike ──")
    feb_mkt = next(
        (r for r in result["category_breakdown"]
         if r["period"] == "2026-02-01" and r["category_id"] == 15),
        None,
    )
    check("Feb Marketing row exists", feb_mkt is not None)
    if feb_mkt:
        check("anomaly_flag == 'spike'", feb_mkt["anomaly_flag"] == "spike",
              f"got {feb_mkt['anomaly_flag']}")
        chunk = _chunk_category_monthly(feb_mkt)
        check("Chunk text mentions 'SPIKE' (RAG vocab)",
              "SPIKE" in chunk.upper(), f"chunk: {chunk[:200]}")

    print("\n── 7. Story verification — Mar 2026 monthly summary ──")
    mar = next(
        (r for r in result["monthly_summary"] if r["period"] == "2026-03-01"),
        None,
    )
    check("Mar 2026 row exists", mar is not None)
    if mar:
        chunk = _chunk_monthly_summary(mar)
        check("Monthly summary chunk mentions 'year-to-date'",
              "year-to-date" in chunk.lower())
        check("Monthly summary chunk mentions 'quarter-over-quarter'",
              "quarter-over-quarter" in chunk.lower())
        check("Chunk mentions 'expenses' vocabulary",
              "expenses" in chunk.lower())
        check("Chunk mentions 'costs' vocabulary",
              "costs" in chunk.lower() or "cost" in chunk.lower())

    print("\n── 8. PII guard — staff attribution chunks never leak $ amounts ──")
    for row in result["staff_attribution"]:
        chunk = _chunk_staff_attribution(row)
        has_dollar = "$" in chunk
        amt = row.get("total_amount_logged", 0)
        amt_str = f"{amt:,.0f}".replace(",", "")
        has_amount = amt_str in chunk.replace(",", "") and amt > 0
        check(
            f"Staff chunk for {row.get('employee_name')} in {row.get('period')} has no $ amount",
            not has_dollar and not has_amount,
            "LEAK DETECTED" if (has_dollar or has_amount) else "",
        )

    print("\n── 9. All 7 chunk generators produce non-empty text for sample rows ──")
    samples = {
        "exp_monthly_summary":      result["monthly_summary"][0] if result["monthly_summary"] else None,
        "exp_category_monthly":     result["category_breakdown"][0] if result["category_breakdown"] else None,
        "exp_subcategory_monthly":  result["subcategory_breakdown"][0] if result["subcategory_breakdown"] else None,
        "exp_location_monthly":     result["location_breakdown"][0] if result["location_breakdown"] else None,
        "exp_payment_type_monthly": result["payment_type_breakdown"][0] if result["payment_type_breakdown"] else None,
        "exp_staff_attribution":    result["staff_attribution"][0] if result["staff_attribution"] else None,
        "exp_cat_location_cross":   result["category_location_cross"][0] if result["category_location_cross"] else None,
    }
    for doc_type, row in samples.items():
        if row is None:
            check(f"{doc_type} sample row available", False, "no sample row")
            continue
        fn = CHUNK_GENERATORS[doc_type]
        text = fn(row)
        check(f"{doc_type} chunk non-empty", bool(text) and len(text) > 50,
              f"got {len(text) if text else 0} chars")

    # ──────────────────────────────────────────────────────────────────────
    # WAREHOUSE-ONLY SECTIONS (10-12) — only run when --warehouse is set
    # ──────────────────────────────────────────────────────────────────────
    if use_warehouse and wh_pool is not None:
        print("\n── 10. Warehouse — all 7 wh_exp_* tables exist ──")
        expected_tables = [
            "wh_exp_monthly_summary",
            "wh_exp_category_breakdown",
            "wh_exp_subcategory_breakdown",
            "wh_exp_location_breakdown",
            "wh_exp_payment_type_breakdown",
            "wh_exp_staff_attribution",
            "wh_exp_category_location_cross",
        ]
        async with wh_pool.acquire() as conn:
            existing = {
                r["table_name"]
                for r in await conn.fetch(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name LIKE 'wh_exp_%'"
                )
            }
        for t in expected_tables:
            check(f"{t} exists", t in existing,
                  "missing — run wh_exp_tables.sql against warehouse")

        if not all(t in existing for t in expected_tables):
            print("\n   Skipping sections 11-12 — apply schema first, then re-run.")
            await wh_pool.close()
            return 1 if fail_count else 0

        print("\n── 11. Warehouse row counts match in-memory transform output ──")
        wh_counts: dict[str, int] = {}
        async with wh_pool.acquire() as conn:
            for table in expected_tables:
                wh_counts[table] = await conn.fetchval(
                    f"SELECT count(*) FROM {table} WHERE business_id = $1",
                    BUSINESS_ID,
                )

        slice_to_table = {
            "monthly_summary":         "wh_exp_monthly_summary",
            "category_breakdown":      "wh_exp_category_breakdown",
            "subcategory_breakdown":   "wh_exp_subcategory_breakdown",
            "location_breakdown":      "wh_exp_location_breakdown",
            "payment_type_breakdown":  "wh_exp_payment_type_breakdown",
            "staff_attribution":       "wh_exp_staff_attribution",
            "category_location_cross": "wh_exp_category_location_cross",
        }
        for slice_name, table in slice_to_table.items():
            wh_n = wh_counts[table]
            mem_n = len(result[slice_name])
            check(
                f"{table:<35s} wh={wh_n:>3} mem={mem_n:>3}",
                wh_n == mem_n,
                f"mismatch ({wh_n} vs {mem_n})",
            )

        print("\n── 12. Warehouse spot-checks — key story values land correctly ──")
        async with wh_pool.acquire() as conn:
            # Monthly Mar 2026 — total + ytd + qoq
            mar = await conn.fetchrow(
                """
                SELECT total_expenses, ytd_total, qoq_change_pct
                FROM wh_exp_monthly_summary
                WHERE business_id = $1 AND period = '2026-03-01'
                """,
                BUSINESS_ID,
            )
            check("Mar 2026 monthly row in warehouse", mar is not None)
            if mar:
                check(f"Mar total_expenses == 4320  (got {mar['total_expenses']})",
                      float(mar["total_expenses"]) == 4320.0)
                check(f"Mar ytd_total == 12810  (got {mar['ytd_total']})",
                      float(mar["ytd_total"]) == 12810.0)
                check(f"Mar qoq_change_pct ≈ -7.17  (got {mar['qoq_change_pct']})",
                      abs(float(mar["qoq_change_pct"]) - (-7.17)) < 0.05)

            # Feb Marketing spike anomaly_flag
            feb_mkt = await conn.fetchrow(
                """
                SELECT category_total, anomaly_flag, pct_vs_baseline
                FROM wh_exp_category_breakdown
                WHERE business_id = $1
                  AND period = '2026-02-01'
                  AND category_id = 15
                """,
                BUSINESS_ID,
            )
            check("Feb Marketing row in warehouse", feb_mkt is not None)
            if feb_mkt:
                check(f"Feb Marketing anomaly_flag == 'spike'  (got {feb_mkt['anomaly_flag']!r})",
                      feb_mkt["anomaly_flag"] == "spike")
                check(f"Feb Marketing pct_vs_baseline ≈ 82.86  (got {feb_mkt['pct_vs_baseline']})",
                      abs(float(feb_mkt["pct_vs_baseline"]) - 82.86) < 0.5)

            # Office/Admin must be ABSENT from 2026 rows (drives dormant detection)
            admin_2026_count = await conn.fetchval(
                """
                SELECT count(*) FROM wh_exp_category_breakdown
                WHERE business_id = $1 AND category_id = 18 AND period >= '2026-01-01'
                """,
                BUSINESS_ID,
            )
            check(f"Office/Admin (cat 18) absent in 2026 rows  (got {admin_2026_count} rows)",
                  admin_2026_count == 0)

            # Subcategory drill-down — 3 rows for Mar Rent
            sub_count = await conn.fetchval(
                """
                SELECT count(*) FROM wh_exp_subcategory_breakdown
                WHERE business_id = $1 AND period = '2026-03-01' AND category_id = 14
                """,
                BUSINESS_ID,
            )
            check(f"Mar Rent has 3 subcategory rows  (got {sub_count})",
                  sub_count == 3)

            # Idempotency check — run the extractor again, row counts must
            # not change (DELETE-then-INSERT semantics inside transaction)
            print("\n── 12b. Idempotency — re-running extractor produces same row counts ──")
            await extractor.run(
                business_id=BUSINESS_ID,
                start_date=START_DATE,
                end_date=END_DATE,
            )
            for slice_name, table in slice_to_table.items():
                n_after = await conn.fetchval(
                    f"SELECT count(*) FROM {table} WHERE business_id = $1",
                    BUSINESS_ID,
                )
                check(
                    f"{table:<35s} stayed at {wh_counts[table]} after re-run  (got {n_after})",
                    n_after == wh_counts[table],
                )

        await wh_pool.close()

    print("\n" + "=" * 68)
    if fail_count == 0:
        print("  ✅ STEP 4 INTEGRATION PASSED")
        print("=" * 68)
        return 0
    else:
        print(f"  ❌ STEP 4 INTEGRATION FAILED — {fail_count} check(s)")
        print("=" * 68)
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Step 4 integration test for the Expenses domain ETL."
    )
    parser.add_argument(
        "--warehouse",
        action="store_true",
        help="Open a real warehouse pool via WH_PG_* env vars and run the "
             "additional warehouse-write sections (10/11/12).",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main(use_warehouse=args.warehouse)))