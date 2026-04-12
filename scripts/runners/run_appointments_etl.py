"""
scripts/runners/run_appointments_etl.py
========================================
Verification runner for the Appointments domain ETL.

Pulls all 4 appointment slices from the analytics backend
(or mock server), runs them through AppointmentsExtractor,
and verifies the output before it hits pgvector.

Usage (against mock server):
    # Start mock server first:
    uvicorn tests.mocks.mock_analytics_server:app --port 8001 --reload

    # Then run:
    python scripts/runners/run_appointments_etl.py --business_id 42 --months 6

Usage (against real backend):
    ANALYTICS_BACKEND_URL=https://api.yourbackend.com \
    python scripts/runners/run_appointments_etl.py --business_id 42 --months 6

Follows the same pattern as: scripts/runners/run_revenue_etl.py
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date
from dateutil.relativedelta import relativedelta

# ── Path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.services.analytics_client import AnalyticsClient
from etl.transforms.appointments_etl import AppointmentsExtractor

ANALYTICS_BASE_URL = os.getenv("ANALYTICS_BACKEND_URL", "http://localhost:8001")

# ─────────────────────────────────────────────────────────────────────────────
# Verification helpers
# ─────────────────────────────────────────────────────────────────────────────

passed = 0
failed = 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {label}")
    else:
        failed += 1
        suffix = f" — {detail}" if detail else ""
        print(f"  ❌ {label}{suffix}")


# ─────────────────────────────────────────────────────────────────────────────
# Main runner
# ─────────────────────────────────────────────────────────────────────────────

async def run(business_id: int, months: int) -> list[dict]:
    global passed, failed
    passed = failed = 0

    end_date   = date.today().replace(day=1) - relativedelta(days=1)
    start_date = (end_date.replace(day=1) - relativedelta(months=months - 1))

    print(f"\n{'='*62}")
    print(f"  LEO AI BI — Appointments ETL Verification")
    print(f"  business_id : {business_id}")
    print(f"  period      : {start_date} → {end_date}  ({months} months)")
    print(f"  backend     : {ANALYTICS_BASE_URL}")
    print(f"{'='*62}\n")

    client    = AnalyticsClient(base_url=ANALYTICS_BASE_URL)
    extractor = AppointmentsExtractor(client=client)

    try:
        docs = await extractor.run(business_id, start_date, end_date)
    except Exception as e:
        print(f"  ❌ ETL run failed: {e}")
        return []

    # ── Doc count checks ──────────────────────────────────────────────────────
    print("── Document counts ──")
    doc_types = [
        "appt_monthly_summary",
        "appt_staff_breakdown",
        "appt_service_breakdown",
        "appt_staff_service_cross",
    ]
    by_type: dict[str, list[dict]] = {t: [] for t in doc_types}
    for doc in docs:
        dt = doc.get("doc_type", "unknown")
        if dt in by_type:
            by_type[dt].append(doc)

    check("Got at least 1 document total", len(docs) >= 1,
          f"got {len(docs)}")
    for dt in doc_types:
        count = len(by_type[dt])
        check(f"  {dt}: {count} docs", count >= 1,
              "expected at least 1")

    # ── Tenant isolation ──────────────────────────────────────────────────────
    print("\n── Tenant isolation ──")
    wrong_tenant = [d for d in docs if d.get("tenant_id") != business_id]
    check("All docs have correct tenant_id",
          len(wrong_tenant) == 0,
          f"{len(wrong_tenant)} docs have wrong tenant_id")

    # ── Required fields per doc type ─────────────────────────────────────────
    print("\n── Required fields ──")

    monthly_required = [
        "period", "location_id", "location_name",
        "total_booked", "completed_count", "cancelled_count",
        "cancellation_rate_pct", "no_show_rate_pct",
        "morning_count", "afternoon_count", "evening_count",
        "weekend_count", "weekday_count", "peak_slot",
    ]
    staff_required = [
        "period", "staff_id", "staff_name", "location_id",
        "total_booked", "completed_count", "completion_rate_pct",
        "no_show_rate_pct", "distinct_services_handled",
    ]
    service_required = [
        "period", "service_id", "service_name",
        "total_booked", "completed_count", "cancelled_count",
        "cancellation_rate_pct", "distinct_clients",
        "repeat_visit_count", "peak_slot",
    ]
    cross_required = [
        "period", "staff_id", "staff_name",
        "service_id", "service_name",
        "total_booked", "completed_count", "completion_rate_pct",
    ]

    field_map = {
        "appt_monthly_summary":    monthly_required,
        "appt_staff_breakdown":    staff_required,
        "appt_service_breakdown":  service_required,
        "appt_staff_service_cross": cross_required,
    }

    for dt, required_fields in field_map.items():
        sample = by_type[dt][0] if by_type[dt] else {}
        for field in required_fields:
            check(f"  [{dt}] {field} present",
                  field in sample,
                  f"missing in first doc")

    # ── Business logic checks ─────────────────────────────────────────────────
    print("\n── Business logic checks ──")

    # Monthly: completed <= total_booked
    for doc in by_type["appt_monthly_summary"]:
        total     = doc.get("total_booked", 0) or 0
        completed = doc.get("completed_count", 0) or 0
        period    = doc.get("period", "?")
        loc       = doc.get("location_name", "?")
        check(
            f"  monthly [{period}|{loc}] completed_count <= total_booked",
            completed <= total,
            f"completed={completed} > total={total}",
        )

    # Monthly: cancelled + no_shows <= total_booked
    for doc in by_type["appt_monthly_summary"]:
        total     = doc.get("total_booked", 0) or 0
        cancelled = doc.get("cancelled_count", 0) or 0
        no_shows  = doc.get("no_show_count", 0) or 0
        period    = doc.get("period", "?")
        loc       = doc.get("location_name", "?")
        check(
            f"  monthly [{period}|{loc}] cancelled + no_shows <= total_booked",
            (cancelled + no_shows) <= total,
            f"cancelled={cancelled} + no_shows={no_shows} > total={total}",
        )

    # Monthly: cancellation_rate_pct is 0–100
    for doc in by_type["appt_monthly_summary"]:
        rate   = doc.get("cancellation_rate_pct", -1)
        period = doc.get("period", "?")
        loc    = doc.get("location_name", "?")
        check(
            f"  monthly [{period}|{loc}] cancellation_rate_pct in [0,100]",
            0 <= rate <= 100,
            f"got {rate}",
        )

    # Rollup rows exist for every period
    rollup_periods = {
        d["period"] for d in by_type["appt_monthly_summary"]
        if d.get("location_id") == 0
    }
    all_periods = {
        d["period"] for d in by_type["appt_monthly_summary"]
    }
    check(
        "  Rollup rows (location_id=0) exist for all periods",
        rollup_periods == all_periods,
        f"rollup periods={rollup_periods}, all periods={all_periods}",
    )

    # Staff: completion_rate_pct is 0–100
    for doc in by_type["appt_staff_breakdown"]:
        rate   = doc.get("completion_rate_pct", -1)
        name   = doc.get("staff_name", "?")
        period = doc.get("period", "?")
        check(
            f"  staff [{period}|{name}] completion_rate_pct in [0,100]",
            0 <= rate <= 100,
            f"got {rate}",
        )

    # Cross: total_booked >= completed_count
    for doc in by_type["appt_staff_service_cross"]:
        total     = doc.get("total_booked", 0) or 0
        completed = doc.get("completed_count", 0) or 0
        staff     = doc.get("staff_name", "?")
        svc       = doc.get("service_name", "?")
        check(
            f"  cross [{staff}|{svc}] completed <= total_booked",
            completed <= total,
            f"completed={completed} > total={total}",
        )

    # ── Embedding text check ──────────────────────────────────────────────────
    print("\n── Embedding text ──")
    for dt in doc_types:
        for doc in by_type[dt]:
            text = doc.get("text", "")
            check(
                f"  [{dt}] text field non-empty",
                bool(text.strip()),
                f"empty text for period={doc.get('period','?')}",
            )
            # Only report first failure per doc type to keep output clean
            if not text.strip():
                break

    # ── Cross-domain consistency: completed totals ────────────────────────────
    print("\n── Cross-domain consistency ──")
    # For each period, sum of completed across all staff
    # should equal the rollup completed_count for that period
    staff_sums: dict[str, int] = {}
    for doc in by_type["appt_staff_breakdown"]:
        p = doc.get("period", "")
        staff_sums[p] = staff_sums.get(p, 0) + (doc.get("completed_count", 0) or 0)

    rollup_completed: dict[str, int] = {
        d["period"]: d.get("completed_count", 0) or 0
        for d in by_type["appt_monthly_summary"]
        if d.get("location_id") == 0
    }

    for period, rollup_val in rollup_completed.items():
        staff_val = staff_sums.get(period, None)
        if staff_val is not None:
            check(
                f"  [{period}] staff completed sum ({staff_val}) == rollup completed ({rollup_val})",
                staff_val == rollup_val,
                f"diff = {staff_val - rollup_val}",
            )

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*62}")
    print(f"  Results: {passed} passed, {failed} failed")
    if failed == 0:
        print("  ✅ Appointments ETL verification PASSED — ready for Step 5")
    else:
        print("  ❌ Fix the failures above before moving to Step 5")
    print(f"{'='*62}\n")

    return docs


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run and verify the Appointments domain ETL"
    )
    parser.add_argument(
        "--business_id", type=int, required=True,
        help="OrganizationId to run the ETL for"
    )
    parser.add_argument(
        "--months", type=int, default=6,
        help="Number of months of history to pull (default: 6)"
    )
    args = parser.parse_args()

    asyncio.run(run(args.business_id, args.months))