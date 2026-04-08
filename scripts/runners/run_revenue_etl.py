"""
LEO Revenue ETL — Run & Verify Script
Step 4: Confirm data lands in the warehouse correctly.

Run this after the backend team deploys the 6 revenue endpoints.
Checks: data shape, tenant isolation, required fields, no nulls in
critical columns, and trend_slope computation.

Usage:
    python run_revenue_etl.py --business_id 42 --months 12
"""

import sys
import os
import asyncio
import argparse
from datetime import date
from dateutil.relativedelta import relativedelta

# Resolve project root so app/ and etl/ are importable regardless of CWD
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from app.services.analytics_client import AnalyticsClient
from etl.transforms.revenue_etl import RevenueExtractor


ANALYTICS_BASE_URL = "https://your-analytics-backend.example.com"  # update before running

REQUIRED_FIELDS_BY_DOC_TYPE = {
    "monthly_summary":       ["period", "service_revenue", "visit_count", "avg_ticket", "mom_growth_pct", "text"],
    "payment_type_breakdown":["breakdown", "text"],
    "staff_revenue":         ["staff_name", "service_revenue", "revenue_rank", "text"],
    "location_revenue":      ["location_name", "period", "service_revenue", "pct_of_total_revenue", "text"],
    "promo_impact":          ["total_discount_given", "breakdown", "text"],
    "failed_refunds":        ["total_lost_revenue", "total_affected_visits", "breakdown", "text"],
}


async def run(business_id: int, months: int):
    end_date   = date.today().replace(day=1) - relativedelta(days=1)
    start_date = (end_date + relativedelta(days=1)) - relativedelta(months=months)

    print(f"\n{'='*60}")
    print(f"  LEO Revenue ETL — business_id={business_id}")
    print(f"  Range: {start_date} → {end_date}")
    print(f"{'='*60}\n")

    client    = AnalyticsClient(base_url=ANALYTICS_BASE_URL)
    extractor = RevenueExtractor(client=client)

    docs = await extractor.run(business_id, start_date, end_date)

    # ── Verification checks ──────────────────────────────────────────────────
    passed = 0
    failed = 0

    def check(label: str, condition: bool, detail: str = ""):
        nonlocal passed, failed
        if condition:
            print(f"  ✅ {label}")
            passed += 1
        else:
            print(f"  ❌ {label}" + (f" — {detail}" if detail else ""))
            failed += 1

    print("── Doc count checks ──")
    check("Produced at least 1 document", len(docs) >= 1, f"got {len(docs)}")

    doc_types = [d["doc_type"] for d in docs]
    for dt in REQUIRED_FIELDS_BY_DOC_TYPE:
        check(f"Doc type '{dt}' present", dt in doc_types)

    print("\n── Tenant isolation checks ──")
    wrong_tenant = [d for d in docs if d.get("tenant_id") != business_id]
    check("All docs scoped to correct business_id", len(wrong_tenant) == 0,
          f"{len(wrong_tenant)} docs with wrong tenant_id")

    print("\n── Required field checks ──")
    for dt, fields in REQUIRED_FIELDS_BY_DOC_TYPE.items():
        matching = [d for d in docs if d["doc_type"] == dt]
        for doc in matching:
            for field in fields:
                check(
                    f"{dt}.{field} present",
                    field in doc and doc[field] is not None,
                    f"missing or null in period={doc.get('period', '?')}"
                )

    print("\n── Revenue sanity checks ──")
    monthly_docs = [d for d in docs if d["doc_type"] == "monthly_summary"]
    for doc in monthly_docs:
        check(
            f"service_revenue >= 0 for {doc['period']}",
            doc.get("service_revenue", -1) >= 0,
        )
        check(
            f"avg_ticket >= 0 for {doc['period']}",
            doc.get("avg_ticket", -1) >= 0,
        )
        check(
            f"total_collected >= service_revenue for {doc['period']}",
            doc.get("total_collected", 0) >= doc.get("service_revenue", 0),
        )

    print("\n── Trend slope check ──")
    if monthly_docs:
        slopes = set(d.get("trend_slope") for d in monthly_docs)
        check("trend_slope is consistent across monthly docs", len(slopes) == 1,
              f"got {slopes}")
        slope_val = list(slopes)[0]
        check("trend_slope is a number", isinstance(slope_val, (int, float)),
              f"got {type(slope_val)}")

    print("\n── Embedding text check ──")
    for doc in docs:
        check(
            f"text field non-empty for {doc['doc_type']}",
            bool(doc.get("text", "").strip()),
        )

    print(f"\n{'='*60}")
    print(f"  Results: {passed} passed, {failed} failed")
    if failed == 0:
        print("  ✅ Revenue ETL verification PASSED — ready for Step 5")
    else:
        print("  ❌ Fix the failures above before moving to Step 5")
    print(f"{'='*60}\n")

    return docs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--business_id", type=int, required=True)
    parser.add_argument("--months", type=int, default=12)
    args = parser.parse_args()

    asyncio.run(run(args.business_id, args.months))