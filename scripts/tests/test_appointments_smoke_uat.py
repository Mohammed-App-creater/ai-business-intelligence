"""
Phase 1 sign-off smoke test: run AppointmentsExtractor.run() end-to-end against
the real UAT analytics backend, validate field shapes, no warehouse writes.

Skipped automatically if ANALYTICS_BACKEND_URL is not set (e.g. for devs without
UAT access). Pass APPOINTMENTS_SMOKE_BIZ_ID and APPOINTMENTS_SMOKE_WINDOW env vars
to override the defaults.

Run locally:
    python -m pytest scripts/tests/test_appointments_smoke_uat.py -v
"""
from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from datetime import date

import pytest

# Default test parameters — same window we used throughout Phase 0.
DEFAULT_BIZ_ID = int(os.getenv("APPOINTMENTS_SMOKE_BIZ_ID", "40"))
DEFAULT_START = os.getenv("APPOINTMENTS_SMOKE_START", "2025-10-01")
DEFAULT_END = os.getenv("APPOINTMENTS_SMOKE_END", "2026-03-31")

# Skip the entire module if no analytics URL is configured. This keeps the
# test runnable in any environment without breaking CI for devs without UAT.
ANALYTICS_URL = os.getenv("ANALYTICS_BACKEND_URL")
pytestmark = pytest.mark.skipif(
    not ANALYTICS_URL,
    reason="ANALYTICS_BACKEND_URL not set; skipping UAT smoke test",
)


@pytest.fixture(scope="module")
def extracted_docs():
    """Run the extractor once against UAT, return the doc list. wh_pool=None
    to skip warehouse writes."""
    from app.services.analytics_client import AnalyticsClient
    from etl.transforms.appointments_etl import AppointmentsExtractor

    async def _run():
        client = AnalyticsClient(base_url=ANALYTICS_URL)
        extractor = AppointmentsExtractor(client=client, wh_pool=None)
        return await extractor.run(
            business_id=DEFAULT_BIZ_ID,
            start_date=date.fromisoformat(DEFAULT_START),
            end_date=date.fromisoformat(DEFAULT_END),
        )

    docs = asyncio.run(_run())
    assert docs, "Extractor returned no docs at all — UAT may be down"
    return docs


def _docs_of_type(docs, doc_type):
    return [d for d in docs if d.get("doc_type") == doc_type]


# ---------------------------------------------------------------------------
# Per-doc-type tests
# ---------------------------------------------------------------------------


def test_monthly_summary_docs_present(extracted_docs):
    rows = _docs_of_type(extracted_docs, "appt_monthly_summary")
    assert rows, "No appt_monthly_summary docs returned"


def test_staff_breakdown_docs_present(extracted_docs):
    rows = _docs_of_type(extracted_docs, "appt_staff_breakdown")
    assert rows, "No appt_staff_breakdown docs — possible regression to limit=10 default"
    # Phase 0 found this endpoint had ~61 rows for tenant 40 in the test window.
    # If we suddenly get ≤10, the limit=10000 plumbing has regressed.
    assert len(rows) > 10, (
        f"Got only {len(rows)} staff-breakdown rows; expected >10. "
        "Likely cause: APPOINTMENTS_PAGE_SIZE not being passed."
    )


def test_service_breakdown_docs_present(extracted_docs):
    rows = _docs_of_type(extracted_docs, "appt_service_breakdown")
    assert rows, "No appt_service_breakdown docs returned"


def test_staff_service_cross_docs_present(extracted_docs):
    rows = _docs_of_type(extracted_docs, "appt_staff_service_cross")
    assert rows, "No appt_staff_service_cross docs returned"


# ---------------------------------------------------------------------------
# Required-field tests (post-derivation)
# ---------------------------------------------------------------------------


def test_monthly_summary_has_period_end(extracted_docs):
    """Derivation §3.11: period_end must be present after derive_period_end()."""
    for r in _docs_of_type(extracted_docs, "appt_monthly_summary"):
        assert r.get("period_end"), f"period_end missing on monthly row: {r}"
        assert len(r["period_end"]) == 10, f"period_end not YYYY-MM-DD: {r['period_end']}"


def test_monthly_summary_has_peak_slot(extracted_docs):
    """Derivation §3.11: peak_slot must be present (or null when all slots zero)."""
    for r in _docs_of_type(extracted_docs, "appt_monthly_summary"):
        assert "peak_slot" in r, f"peak_slot key missing on monthly row: {r}"
        if r["peak_slot"] is not None:
            assert r["peak_slot"] in ("morning", "afternoon", "evening"), (
                f"Bad peak_slot value: {r['peak_slot']}"
            )


def test_monthly_summary_has_distinct_staff_count(extracted_docs):
    """Phase 1.2a: distinct_staff_count populated from staff-breakdown."""
    rows = _docs_of_type(extracted_docs, "appt_monthly_summary")
    assert any(r.get("distinct_staff_count", 0) > 0 for r in rows), (
        "No monthly row has distinct_staff_count>0 — staff_counts_map likely broken"
    )


def test_staff_breakdown_has_completion_rate(extracted_docs):
    """Derivation §3.11: completion_rate_pct must be present after derive_completion_rate()."""
    for r in _docs_of_type(extracted_docs, "appt_staff_breakdown"):
        assert "completion_rate_pct" in r, f"completion_rate_pct missing on staff row: {r}"
        v = r["completion_rate_pct"]
        assert v is None or 0 <= v <= 100, f"completion_rate_pct out of range: {v}"


def test_staff_and_service_names_populated(extracted_docs):
    """Bug §3.7 was fixed 2026-04-30; if names are empty, regression."""
    staff_rows = _docs_of_type(extracted_docs, "appt_staff_breakdown")
    empty_names = [r for r in staff_rows if not r.get("staff_name")]
    assert not empty_names, (
        f"{len(empty_names)} staff rows have empty staff_name — bug §3.7 regressed"
    )

    service_rows = _docs_of_type(extracted_docs, "appt_service_breakdown")
    empty_svc = [r for r in service_rows if not r.get("service_name")]
    assert not empty_svc, (
        f"{len(empty_svc)} service rows have empty service_name — bug §3.7 regressed"
    )


# ---------------------------------------------------------------------------
# Numeric sanity tests
# ---------------------------------------------------------------------------


def test_monthly_rollup_row_present(extracted_docs):
    """Spec §2.2: every period must have a location_id=0 rollup row."""
    rows = _docs_of_type(extracted_docs, "appt_monthly_summary")
    periods_with_rollup = {r["period"] for r in rows if r.get("location_id") == 0}
    all_periods = {r["period"] for r in rows}
    missing = all_periods - periods_with_rollup
    assert not missing, f"Periods missing rollup row: {missing}"


def test_no_negative_counts(extracted_docs):
    """Sanity: count fields must never be negative across any doc type."""
    count_fields = (
        "total_booked",
        "completed_count",
        "cancelled_count",
        "no_show_count",
        "morning_count",
        "afternoon_count",
        "evening_count",
        "weekend_count",
        "weekday_count",
        "walkin_count",
        "app_booking_count",
        "distinct_clients",
        "repeat_visit_count",
        "distinct_services_handled",
        "distinct_staff_count",
    )
    for doc in extracted_docs:
        for f in count_fields:
            v = doc.get(f)
            if v is not None:
                assert v >= 0, f"Negative {f}={v} in doc: {doc}"


def test_per_location_mom_growth_correct(extracted_docs):
    """Bug §3.8 was fixed 2026-04-30 — per-location mom_growth_pct must equal
    recomputed value, not the rollup. Regression check."""
    rows = _docs_of_type(extracted_docs, "appt_monthly_summary")
    by_loc = defaultdict(list)
    for r in rows:
        by_loc[r["location_id"]].append(r)
    for loc_id, loc_rows in by_loc.items():
        if loc_id == 0:
            continue
        loc_rows.sort(key=lambda r: r["period"])
        prev = None
        for r in loc_rows:
            tb = r.get("total_booked") or 0
            mom = r.get("mom_growth_pct")
            if prev is None:
                # First period — should be null
                assert mom is None, (
                    f"loc {loc_id} first period {r['period']} has mom={mom}, "
                    "expected null — bug §3.8 may have regressed"
                )
            else:
                expected = round((tb - prev) / prev * 100, 2) if prev else None
                if expected is not None:
                    assert abs(mom - expected) <= 0.01, (
                        f"loc {loc_id} period {r['period']} mom={mom}, "
                        f"recomputed={expected} — bug §3.8 may have regressed"
                    )
            prev = tb
