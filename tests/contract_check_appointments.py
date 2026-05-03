#!/usr/bin/env python3
"""
Phase 0 contract verification for the LEO appointments domain.

Hits each of the four analytics-backend endpoints defined in
appointments_domain_step3_api_spec_v1_1.docx, validates the response shape
and content, and writes persistent artefacts to disk so the run is replayable
and can serve as a source-of-truth baseline for future regression checks.

Per the leo-domain-integration playbook, this script is the *operational*
form of Phase 0 step 3 — every drift it catches goes into
APPOINTMENTS_KNOWN_ISSUES.md §2 with severity before Phase 1 begins.

Usage
-----
    export ANALYTICS_BASE=https://analytics-uat.leocrm.example
    export API_KEY=<service api key>
    export BIZ_ID=<UAT tenant with appointments>
    export FOREIGN_BIZ=<a different UAT tenant>

    python contract_check_appointments.py \\
        --from 2025-10-01 --to 2026-03-31 \\
        --out ./phase0_uat_results

Outputs (under --out / <UTC timestamp>/)
----------------------------------------
    raw/01_monthly_summary.json            # request URL, status, headers, body
    raw/02_staff_breakdown.json
    raw/03_service_breakdown.json
    raw/04_staff_service_cross.json
    raw/tenant_isolation.json
    raw/auth_failure.json
    raw/pagination_p1.json, raw/pagination_p2.json
    raw/empty_window.json
    report.md                              # human-readable, paste-ready
    report.json                            # machine-readable for diffing later

Exit codes
----------
    0 — no blockers (workarounds and cosmetics may exist; review report.md)
    1 — at least one blocker; do NOT proceed to Phase 1
    2 — script error (bad args, network failure on every call, etc.)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable

try:
    import httpx
except ImportError:
    sys.stderr.write("This script requires httpx. Install with: pip install httpx\n")
    sys.exit(2)


# ============================================================================
# Spec constants — derived from appointments_domain_step3_api_spec_v1_1.docx
# ============================================================================

PERIOD_START_RE = re.compile(r"^\d{4}-\d{2}-01$")
PERIOD_END_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
VALID_PEAK_SLOTS = {"morning", "afternoon", "evening", None}

# Required fields per response row, by endpoint shortname.
REQUIRED_FIELDS: dict[str, set[str]] = {
    "monthly_summary": {
        "business_id", "location_id", "location_name", "location_city",
        "period_start", "period_end",
        "total_booked", "confirmed_count", "completed_count",
        "cancelled_count", "no_show_count",
        "morning_count", "afternoon_count", "evening_count",
        "weekend_count", "weekday_count",
        "avg_actual_duration_min",
        "cancellation_rate_pct", "no_show_rate_pct",
        "mom_growth_pct", "peak_slot",  # v1.1 additions
        "walkin_count", "app_booking_count",
    },
    "staff_breakdown": {
        "business_id", "staff_id", "staff_name",
        "location_id", "location_name", "period_start",
        "total_booked", "completed_count",
        "completion_rate_pct",  # v1.1 addition
        "cancelled_count", "no_show_count", "no_show_rate_pct",
        "distinct_services_handled",
        "mom_growth_pct",  # v1.1 addition
    },
    "service_breakdown": {
        "business_id", "service_id", "service_name", "period_start",
        "total_booked", "completed_count", "cancelled_count",
        "distinct_clients", "repeat_visit_count",
        "avg_scheduled_duration_min", "avg_actual_duration_min",
        "cancellation_rate_pct",
        "morning_count", "afternoon_count", "evening_count",
        "peak_slot",  # v1.1 addition
    },
    "staff_service_cross": {
        "business_id", "staff_id", "staff_name",
        "service_id", "service_name", "period_start",
        "total_booked", "completed_count",
    },
}

# Fields added in v1.1 (post Step-7 refinement) — highest drift risk if the
# backend was deployed against an earlier draft.
V1_1_FIELDS: dict[str, set[str]] = {
    "monthly_summary": {"mom_growth_pct", "peak_slot"},
    "staff_breakdown": {"mom_growth_pct", "completion_rate_pct"},
    "service_breakdown": {"peak_slot"},
    "staff_service_cross": set(),
}

# Numeric fields that MUST be JSON numbers (not strings). String drift
# is a workaround (decode client-side) but worth flagging.
NUMERIC_FIELDS: dict[str, set[str]] = {
    "monthly_summary": {
        "business_id", "location_id",
        "total_booked", "confirmed_count", "completed_count",
        "cancelled_count", "no_show_count",
        "morning_count", "afternoon_count", "evening_count",
        "weekend_count", "weekday_count",
        "cancellation_rate_pct", "no_show_rate_pct",
        "walkin_count", "app_booking_count",
    },
    "staff_breakdown": {
        "business_id", "staff_id", "location_id",
        "total_booked", "completed_count", "completion_rate_pct",
        "cancelled_count", "no_show_count", "no_show_rate_pct",
        "distinct_services_handled",
    },
    "service_breakdown": {
        "business_id", "service_id",
        "total_booked", "completed_count", "cancelled_count",
        "distinct_clients", "repeat_visit_count",
        "cancellation_rate_pct",
        "morning_count", "afternoon_count", "evening_count",
    },
    "staff_service_cross": {
        "business_id", "staff_id", "service_id",
        "total_booked", "completed_count",
    },
}


class Sev(Enum):
    PASS = "pass"
    COSMETIC = "cosmetic"
    WORKAROUND = "workaround"
    BLOCKER = "blocker"


@dataclass
class Check:
    name: str
    severity: Sev
    detail: str = ""
    endpoint: str = ""

    def to_dict(self) -> dict:
        return {
            "endpoint": self.endpoint,
            "check": self.name,
            "severity": self.severity.value,
            "detail": self.detail,
        }


# ============================================================================
# HTTP wrapper
# ============================================================================

@dataclass
class Response:
    request_url: str
    request_method: str
    request_params: dict
    status: int
    headers: dict
    body: Any        # parsed JSON if possible, else None
    body_raw: str    # raw text always
    elapsed_ms: int = 0
    error: str | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        # Truncate raw body in metadata snapshot if huge — keep first 1MB.
        if d["body_raw"] and len(d["body_raw"]) > 1_000_000:
            d["body_raw_truncated"] = True
            d["body_raw"] = d["body_raw"][:1_000_000]
        return d


def call(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    params: dict | None = None,
) -> Response:
    headers = headers or {}
    params = params or {}
    t0 = dt.datetime.now()
    try:
        r = client.request(method, url, headers=headers, params=params, timeout=30)
        elapsed_ms = int((dt.datetime.now() - t0).total_seconds() * 1000)
        try:
            body: Any = r.json()
        except Exception:
            body = None
        return Response(
            request_url=str(r.request.url),
            request_method=method,
            request_params=params,
            status=r.status_code,
            headers=dict(r.headers),
            body=body,
            body_raw=r.text,
            elapsed_ms=elapsed_ms,
        )
    except Exception as e:
        elapsed_ms = int((dt.datetime.now() - t0).total_seconds() * 1000)
        return Response(
            request_url=url,
            request_method=method,
            request_params=params,
            status=-1,
            headers={},
            body=None,
            body_raw="",
            elapsed_ms=elapsed_ms,
            error=str(e),
        )


# ============================================================================
# Per-row validators (run when status=200 and body is a list)
# ============================================================================

def _check_required_fields(rows: list[dict], domain: str) -> list[Check]:
    """Required-field presence — sample first row, extras are cosmetic."""
    out: list[Check] = []
    if not rows:
        out.append(Check(
            name="non_empty_response",
            severity=Sev.WORKAROUND,
            detail="Endpoint returned 200 but empty list. Validation can only "
                   "check shape, not values. Re-run with a tenant/window that has data.",
        ))
        return out

    expected = REQUIRED_FIELDS[domain]
    v1_1 = V1_1_FIELDS[domain]
    sample = rows[0]
    actual = set(sample.keys())
    missing = expected - actual

    if not missing:
        out.append(Check(
            name="all_required_fields_present",
            severity=Sev.PASS,
            detail=f"All {len(expected)} required fields present on first row.",
        ))
    else:
        missing_v1_1 = missing & v1_1
        missing_other = missing - v1_1
        if missing_other:
            out.append(Check(
                name="required_fields_missing",
                severity=Sev.BLOCKER,
                detail=f"Missing required fields: {sorted(missing_other)}. "
                       "ETL deserialization will fail.",
            ))
        if missing_v1_1:
            out.append(Check(
                name="v1_1_fields_missing",
                severity=Sev.BLOCKER,
                detail=f"Missing v1.1-added fields: {sorted(missing_v1_1)}. "
                       "Backend may be deployed against earlier draft. "
                       "Will break Q5/Q10/Q17/Q18/Q29 in test harness.",
            ))

    extra = actual - expected
    if extra:
        out.append(Check(
            name="extra_fields_returned",
            severity=Sev.COSMETIC,
            detail=f"Extra fields not in spec: {sorted(extra)}.",
        ))
    return out


def _check_period_start_format(rows: list[dict]) -> Check:
    bad = [
        r.get("period_start")
        for r in rows
        if not (isinstance(r.get("period_start"), str) and PERIOD_START_RE.match(r["period_start"]))
    ]
    if not bad:
        return Check(
            name="period_start_format",
            severity=Sev.PASS,
            detail=f"All {len(rows)} rows have period_start matching YYYY-MM-01.",
        )
    return Check(
        name="period_start_format",
        severity=Sev.BLOCKER,
        detail=f"{len(bad)}/{len(rows)} rows have malformed period_start. "
               f"First bad value: {bad[0]!r}. "
               "Spec §1.2 requires YYYY-MM-01 (date), not 'YYYY-MM' string.",
    )


def _check_numeric_types(rows: list[dict], domain: str) -> Check:
    """Catch fields the backend serialized as strings ('42' instead of 42)."""
    expected_numeric = NUMERIC_FIELDS[domain]
    sample = rows[0]
    string_numerics: list[str] = []
    for f in expected_numeric:
        if f not in sample:
            continue
        v = sample[f]
        if v is None:
            continue
        if isinstance(v, str):
            string_numerics.append(f)
    if not string_numerics:
        return Check(
            name="numeric_fields_typed",
            severity=Sev.PASS,
            detail="All numeric fields are JSON numbers (not strings).",
        )
    return Check(
        name="numeric_fields_typed",
        severity=Sev.WORKAROUND,
        detail=f"{len(string_numerics)} numeric field(s) returned as strings: "
               f"{sorted(string_numerics)}. Decode client-side in extractor.",
    )


def _check_peak_slot(rows: list[dict]) -> Check | None:
    if "peak_slot" not in rows[0]:
        return None
    bad = [r["peak_slot"] for r in rows if r["peak_slot"] not in VALID_PEAK_SLOTS]
    if not bad:
        return Check(
            name="peak_slot_values",
            severity=Sev.PASS,
            detail=f"All peak_slot values are valid ({len(rows)} rows).",
        )
    return Check(
        name="peak_slot_values",
        severity=Sev.WORKAROUND,
        detail=f"{len(bad)} rows have invalid peak_slot. First: {bad[0]!r}. "
               "Spec §2.3 / §4.3 allows {'morning','afternoon','evening'} or null. "
               "Workaround: derive client-side from time-slot counts.",
    )


def _check_non_negative_counts(rows: list[dict], fields: list[str]) -> list[Check]:
    out: list[Check] = []
    for f in fields:
        if f not in rows[0]:
            continue
        bad = [r for r in rows if isinstance(r.get(f), (int, float)) and r[f] < 0]
        if bad:
            out.append(Check(
                name=f"{f}_non_negative",
                severity=Sev.BLOCKER,
                detail=f"{len(bad)} rows have negative {f}. First: {bad[0].get(f)}.",
            ))
    return out


def check_monthly_summary(rows: list[dict]) -> list[Check]:
    domain = "monthly_summary"
    out = _check_required_fields(rows, domain)
    if not rows:
        return out

    out.append(_check_period_start_format(rows))
    out.append(_check_numeric_types(rows, domain))
    peak = _check_peak_slot(rows)
    if peak:
        out.append(peak)
    out.extend(_check_non_negative_counts(rows, [
        "total_booked", "completed_count", "cancelled_count", "no_show_count",
        "morning_count", "afternoon_count", "evening_count",
        "weekend_count", "weekday_count",
        "walkin_count", "app_booking_count",
    ]))

    # Rollup row check (spec §2.2)
    rollup = [r for r in rows if r.get("location_id") == 0]
    if rollup:
        names = {r.get("location_name") for r in rollup}
        if names == {"__ALL__"}:
            out.append(Check(
                name="rollup_row_present",
                severity=Sev.PASS,
                detail=f"Found {len(rollup)} rollup rows (location_id=0, name='__ALL__').",
            ))
        else:
            out.append(Check(
                name="rollup_row_label",
                severity=Sev.WORKAROUND,
                detail=f"location_id=0 rows have name(s) {sorted(filter(None, names))} — spec expects '__ALL__'.",
            ))
    else:
        out.append(Check(
            name="rollup_row_present",
            severity=Sev.WORKAROUND,
            detail="No rollup row (location_id=0) found. Spec §2.2 mandates "
                   "one rollup row per month. Org-level questions (Q1-Q11) will degrade.",
        ))

    # Time-slot sanity: morning + afternoon + evening ≈ total_booked
    bad_sum = []
    for r in rows:
        try:
            slot_sum = int(r.get("morning_count", 0)) + int(r.get("afternoon_count", 0)) + int(r.get("evening_count", 0))
            total = int(r.get("total_booked", 0))
            if total > 0:
                drift_abs = abs(slot_sum - total)
                drift_pct = drift_abs / total
                if drift_pct > 0.05 and drift_abs > 5:
                    bad_sum.append((r.get("location_id"), r.get("period_start"), slot_sum, total))
        except (TypeError, ValueError):
            pass
    if bad_sum:
        out.append(Check(
            name="time_slot_sum_sanity",
            severity=Sev.COSMETIC,
            detail=f"{len(bad_sum)} rows have morning+afternoon+evening drifting "
                   f">5% from total_booked. First: location_id={bad_sum[0][0]}, "
                   f"period={bad_sum[0][1]}, slot_sum={bad_sum[0][2]}, total={bad_sum[0][3]}. "
                   "Possible: appointments outside 06:00-21:59. Confirm with backend.",
        ))

    # mom_growth_pct nullness on earliest period per location
    if "mom_growth_pct" in rows[0]:
        by_loc: dict[Any, list[dict]] = {}
        for r in rows:
            by_loc.setdefault(r.get("location_id"), []).append(r)
        bad_locs = []
        for loc, lrows in by_loc.items():
            try:
                lrows_sorted = sorted(lrows, key=lambda x: x.get("period_start") or "")
            except Exception:
                continue
            if not lrows_sorted:
                continue
            earliest = lrows_sorted[0]
            if earliest.get("mom_growth_pct") is not None:
                bad_locs.append((loc, earliest.get("period_start"), earliest.get("mom_growth_pct")))
        if bad_locs:
            out.append(Check(
                name="mom_growth_first_period_null",
                severity=Sev.COSMETIC,
                detail=f"{len(bad_locs)} location(s) have non-null mom_growth_pct on earliest period. "
                       f"First: location_id={bad_locs[0][0]}, period={bad_locs[0][1]}, value={bad_locs[0][2]}. "
                       "Spec says null for first period.",
            ))
        else:
            out.append(Check(
                name="mom_growth_first_period_null",
                severity=Sev.PASS,
                detail="mom_growth_pct correctly null on earliest period for every location.",
            ))

    return out


def check_staff_breakdown(rows: list[dict]) -> list[Check]:
    domain = "staff_breakdown"
    out = _check_required_fields(rows, domain)
    if not rows:
        return out

    out.append(_check_period_start_format(rows))
    out.append(_check_numeric_types(rows, domain))
    out.extend(_check_non_negative_counts(rows, [
        "total_booked", "completed_count", "cancelled_count",
        "no_show_count", "distinct_services_handled",
    ]))

    # staff_id > 0 filter (spec §3.2)
    bad_zero = [r for r in rows if r.get("staff_id") == 0]
    if bad_zero:
        out.append(Check(
            name="staff_id_filter",
            severity=Sev.BLOCKER,
            detail=f"{len(bad_zero)} rows have staff_id=0. "
                   "Spec §3.2 mandates EmployeeId>0 filter.",
        ))
    else:
        out.append(Check(
            name="staff_id_filter",
            severity=Sev.PASS,
            detail="No rows with staff_id=0.",
        ))

    # completion_rate_pct ∈ [0, 100]
    if "completion_rate_pct" in rows[0]:
        bad_rate = []
        for r in rows:
            v = r.get("completion_rate_pct")
            if v is None:
                continue
            try:
                vf = float(v)
                if not (0 <= vf <= 100):
                    bad_rate.append(r)
            except (TypeError, ValueError):
                bad_rate.append(r)
        if bad_rate:
            out.append(Check(
                name="completion_rate_range",
                severity=Sev.WORKAROUND,
                detail=f"{len(bad_rate)} rows have completion_rate_pct outside [0,100] or non-numeric. "
                       f"First: staff_id={bad_rate[0].get('staff_id')}, "
                       f"period={bad_rate[0].get('period_start')}, "
                       f"value={bad_rate[0].get('completion_rate_pct')!r}",
            ))
        else:
            out.append(Check(
                name="completion_rate_range",
                severity=Sev.PASS,
                detail="All completion_rate_pct values within [0,100].",
            ))

    # completed_count <= total_booked
    bad_completed = [r for r in rows if (r.get("completed_count") or 0) > (r.get("total_booked") or 0)]
    if bad_completed:
        out.append(Check(
            name="completed_le_total",
            severity=Sev.BLOCKER,
            detail=f"{len(bad_completed)} rows have completed_count > total_booked. "
                   f"First: staff_id={bad_completed[0].get('staff_id')}, "
                   f"period={bad_completed[0].get('period_start')}.",
        ))
    else:
        out.append(Check(
            name="completed_le_total",
            severity=Sev.PASS,
            detail="completed_count <= total_booked on every row.",
        ))

    return out


def check_service_breakdown(rows: list[dict]) -> list[Check]:
    domain = "service_breakdown"
    out = _check_required_fields(rows, domain)
    if not rows:
        return out

    out.append(_check_period_start_format(rows))
    out.append(_check_numeric_types(rows, domain))
    peak = _check_peak_slot(rows)
    if peak:
        out.append(peak)
    out.extend(_check_non_negative_counts(rows, [
        "total_booked", "completed_count", "cancelled_count",
        "distinct_clients", "repeat_visit_count",
        "morning_count", "afternoon_count", "evening_count",
    ]))

    # service_id > 0 filter (spec §4.2)
    bad_zero = [r for r in rows if r.get("service_id") == 0]
    if bad_zero:
        out.append(Check(
            name="service_id_filter",
            severity=Sev.BLOCKER,
            detail=f"{len(bad_zero)} rows have service_id=0. "
                   "Spec §4.2 mandates ServiceId>0 filter.",
        ))
    else:
        out.append(Check(
            name="service_id_filter",
            severity=Sev.PASS,
            detail="No rows with service_id=0.",
        ))

    # repeat_visit_count == total_booked - distinct_clients (exact, spec §4.2)
    bad_repeat = []
    for r in rows:
        try:
            tb = int(r.get("total_booked", 0))
            dc = int(r.get("distinct_clients", 0))
            rv = int(r.get("repeat_visit_count", 0))
            if rv != tb - dc:
                bad_repeat.append(r)
        except (TypeError, ValueError):
            pass
    if bad_repeat:
        out.append(Check(
            name="repeat_visit_formula",
            severity=Sev.WORKAROUND,
            detail=f"{len(bad_repeat)} rows fail repeat_visit_count == total_booked - distinct_clients. "
                   f"First: service_id={bad_repeat[0].get('service_id')}, "
                   f"tb={bad_repeat[0].get('total_booked')}, "
                   f"dc={bad_repeat[0].get('distinct_clients')}, "
                   f"rv={bad_repeat[0].get('repeat_visit_count')}. "
                   "Spec §4.2 mandates this exact formula.",
        ))
    else:
        out.append(Check(
            name="repeat_visit_formula",
            severity=Sev.PASS,
            detail="repeat_visit_count == total_booked - distinct_clients holds for all rows.",
        ))

    return out


def check_staff_service_cross(rows: list[dict]) -> list[Check]:
    domain = "staff_service_cross"
    out = _check_required_fields(rows, domain)
    if not rows:
        return out

    out.append(_check_period_start_format(rows))
    out.append(_check_numeric_types(rows, domain))
    out.extend(_check_non_negative_counts(rows, ["total_booked", "completed_count"]))

    # staff_id > 0 AND service_id > 0 (spec §5.2)
    bad_filter = [r for r in rows if r.get("staff_id") == 0 or r.get("service_id") == 0]
    if bad_filter:
        out.append(Check(
            name="staff_and_service_id_filter",
            severity=Sev.BLOCKER,
            detail=f"{len(bad_filter)} rows violate (staff_id>0 AND service_id>0). Spec §5.2.",
        ))
    else:
        out.append(Check(
            name="staff_and_service_id_filter",
            severity=Sev.PASS,
            detail="All rows have staff_id>0 AND service_id>0.",
        ))

    # completed_count <= total_booked
    bad_completed = [r for r in rows if (r.get("completed_count") or 0) > (r.get("total_booked") or 0)]
    if bad_completed:
        out.append(Check(
            name="completed_le_total",
            severity=Sev.BLOCKER,
            detail=f"{len(bad_completed)} rows have completed_count > total_booked. "
                   f"First: staff_id={bad_completed[0].get('staff_id')}, "
                   f"service_id={bad_completed[0].get('service_id')}, "
                   f"period={bad_completed[0].get('period_start')}.",
        ))
    else:
        out.append(Check(
            name="completed_le_total",
            severity=Sev.PASS,
            detail="completed_count <= total_booked on every row.",
        ))

    return out


# ============================================================================
# Cross-cutting checks
# ============================================================================

def check_tenant_isolation(resp: Response) -> list[Check]:
    if resp.status == 403:
        return [Check(
            name="tenant_isolation_403",
            severity=Sev.PASS,
            detail="Foreign business_id correctly rejected with 403.",
        )]
    if resp.status == 200:
        return [Check(
            name="tenant_isolation_403",
            severity=Sev.BLOCKER,
            detail="Foreign business_id returned 200 — tenant isolation NOT enforced. "
                   "STOP, escalate to backend immediately.",
        )]
    if resp.status in (401, 404):
        return [Check(
            name="tenant_isolation_403",
            severity=Sev.WORKAROUND,
            detail=f"Foreign business_id returned {resp.status} (expected 403). "
                   "Acceptable if backend conflates 'no auth' with 'wrong tenant', "
                   "but flag for backend review.",
        )]
    return [Check(
        name="tenant_isolation_403",
        severity=Sev.BLOCKER,
        detail=f"Foreign business_id returned unexpected status {resp.status}.",
    )]


def check_auth_failure(resp: Response) -> list[Check]:
    if resp.status in (401, 403):
        return [Check(
            name="auth_required",
            severity=Sev.PASS,
            detail=f"Missing API key correctly rejected with {resp.status}.",
        )]
    if resp.status == 200:
        return [Check(
            name="auth_required",
            severity=Sev.BLOCKER,
            detail="Endpoint returned 200 without API key. Severe security issue.",
        )]
    return [Check(
        name="auth_required",
        severity=Sev.WORKAROUND,
        detail=f"Missing API key returned {resp.status} (expected 401/403). "
               "Confirm with backend.",
    )]


def check_pagination(p1: Response, p2: Response, page_size: int) -> list[Check]:
    out: list[Check] = []
    if p1.status != 200 or p2.status != 200:
        out.append(Check(
            name="pagination_basic",
            severity=Sev.BLOCKER,
            detail=f"Pagination request failed: p1={p1.status} p2={p2.status}.",
        ))
        return out
    if not isinstance(p1.body, list) or not isinstance(p2.body, list):
        out.append(Check(
            name="pagination_response_shape",
            severity=Sev.BLOCKER,
            detail="Pagination response is not a JSON array.",
        ))
        return out
    if len(p1.body) > page_size:
        out.append(Check(
            name="pagination_size_honored",
            severity=Sev.BLOCKER,
            detail=f"page_size={page_size} ignored: page 1 returned {len(p1.body)} rows. "
                   "ETL won't survive at scale.",
        ))
    else:
        out.append(Check(
            name="pagination_size_honored",
            severity=Sev.PASS,
            detail=f"page 1 honors page_size: returned {len(p1.body)} ≤ {page_size}.",
        ))
    if p2.body and p1.body == p2.body:
        out.append(Check(
            name="pagination_distinct_pages",
            severity=Sev.BLOCKER,
            detail="page 2 returned identical rows to page 1 — pagination not implemented.",
        ))
    elif p2.body == []:
        out.append(Check(
            name="pagination_distinct_pages",
            severity=Sev.PASS,
            detail="page 2 returned empty (small dataset). Pagination shape OK.",
        ))
    else:
        out.append(Check(
            name="pagination_distinct_pages",
            severity=Sev.PASS,
            detail=f"page 2 differs from page 1 ({len(p2.body)} rows on page 2).",
        ))
    return out


def check_empty_window(resp: Response) -> list[Check]:
    """Confirm empty window returns [] not null. Uses far-future dates."""
    if resp.status != 200:
        return [Check(
            name="empty_response_shape",
            severity=Sev.WORKAROUND,
            detail=f"Cannot verify empty shape: status={resp.status} on far-future request. "
                   "Backend may reject 2099 dates. Verify manually with a known empty window.",
        )]
    if resp.body == []:
        return [Check(
            name="empty_response_shape",
            severity=Sev.PASS,
            detail="Empty window returned as [] (correct per spec §1.2).",
        )]
    if resp.body is None:
        return [Check(
            name="empty_response_shape",
            severity=Sev.BLOCKER,
            detail="Empty window returned null. Spec §1.2 requires []. "
                   "ETL deserialization will fail.",
        )]
    if isinstance(resp.body, list) and len(resp.body) > 0:
        return [Check(
            name="empty_response_shape",
            severity=Sev.COSMETIC,
            detail=f"Far-future request returned {len(resp.body)} rows — "
                   "couldn't test empty shape this way. Backend may be ignoring date params. "
                   "Verify manually.",
        )]
    return [Check(
        name="empty_response_shape",
        severity=Sev.WORKAROUND,
        detail=f"Far-future request returned unexpected body type: {type(resp.body).__name__}.",
    )]


def check_cross_endpoint_sanity(
    monthly: list[dict] | None,
    cross: list[dict] | None,
) -> list[Check]:
    """For each period, sum(staff_service_cross.total_booked) should be <=
    monthly_summary.total_booked at the rollup level (location_id=0).
    The cross has stricter filters (staff_id>0 AND service_id>0) so it should
    always be <=. If it's >, either filters are wrong somewhere or numbers
    don't agree across endpoints — both are problems for trust."""
    if not monthly or not cross:
        return []
    rollups: dict[str, int] = {}
    for r in monthly:
        if r.get("location_id") == 0:
            try:
                rollups[r["period_start"]] = int(r.get("total_booked", 0))
            except (TypeError, ValueError, KeyError):
                continue
    if not rollups:
        return [Check(
            name="cross_endpoint_sanity",
            severity=Sev.COSMETIC,
            detail="No rollup rows in monthly_summary; can't cross-check against staff_service_cross.",
        )]
    cross_sums: dict[str, int] = {}
    for r in cross:
        try:
            cross_sums[r["period_start"]] = cross_sums.get(r["period_start"], 0) + int(r.get("total_booked", 0))
        except (TypeError, ValueError, KeyError):
            continue
    bad = []
    for period, csum in cross_sums.items():
        rsum = rollups.get(period)
        if rsum is None:
            continue
        if csum > rsum:
            bad.append((period, csum, rsum))
    if bad:
        return [Check(
            name="cross_endpoint_sanity",
            severity=Sev.WORKAROUND,
            detail=f"{len(bad)} period(s) have sum(staff_service_cross.total_booked) > "
                   f"monthly_summary rollup total_booked. "
                   f"First: period={bad[0][0]}, cross_sum={bad[0][1]}, rollup={bad[0][2]}. "
                   "Filters or aggregations may disagree across endpoints.",
        )]
    return [Check(
        name="cross_endpoint_sanity",
        severity=Sev.PASS,
        detail=f"sum(staff_service_cross) <= monthly_summary rollup for all "
               f"{len(cross_sums)} overlapping period(s).",
    )]


# ============================================================================
# Orchestrator
# ============================================================================

ENDPOINTS: list[tuple[str, str, Callable[[list[dict]], list[Check]]]] = [
    ("monthly_summary",     "monthly-summary",     check_monthly_summary),
    ("staff_breakdown",     "staff-breakdown",     check_staff_breakdown),
    ("service_breakdown",   "service-breakdown",   check_service_breakdown),
    ("staff_service_cross", "staff-service-cross", check_staff_service_cross),
]


def main() -> int:
    # On Windows, stdout defaults to cp1252 which mangles emoji/unicode.
    # Reconfigure to UTF-8 if available (Python 3.7+); harmless if it fails.
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    parser = argparse.ArgumentParser(
        description="Phase 0 contract check for the LEO appointments domain.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--base", default=os.environ.get("ANALYTICS_BASE"),
                        help="Analytics backend base URL (env: ANALYTICS_BASE)")
    parser.add_argument("--api-key", default=os.environ.get("API_KEY"),
                        help="X-API-Key value (env: API_KEY)")
    parser.add_argument("--biz", default=os.environ.get("BIZ_ID"),
                        help="Test business_id (env: BIZ_ID)")
    parser.add_argument("--foreign-biz", default=os.environ.get("FOREIGN_BIZ"),
                        help="Foreign business_id for tenant isolation (env: FOREIGN_BIZ)")
    parser.add_argument("--from", dest="from_date", default=os.environ.get("FROM", "2025-10-01"),
                        help="Period start, YYYY-MM-DD (env: FROM)")
    parser.add_argument("--to", dest="to_date", default=os.environ.get("TO", "2026-03-31"),
                        help="Period end, YYYY-MM-DD (env: TO)")
    parser.add_argument("--out", default="phase0_uat_results",
                        help="Output root directory; a UTC-timestamp subdir is created. "
                             "Default: ./phase0_uat_results")
    parser.add_argument("--page-size-test", type=int, default=2,
                        help="page_size for pagination check (default 2)")
    args = parser.parse_args()

    missing = [k for k, v in {"--base": args.base, "--api-key": args.api_key,
                              "--biz": args.biz, "--foreign-biz": args.foreign_biz}.items() if not v]
    if missing:
        sys.stderr.write(f"Missing required arg(s): {', '.join(missing)}\n")
        sys.stderr.write("Set via flag or environment variable. See --help for details.\n")
        return 2

    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.out) / timestamp
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    headers = {"X-API-Key": args.api_key}
    common_params = {"from": args.from_date, "to": args.to_date}
    all_checks: list[Check] = []
    raw_responses: dict[str, dict] = {}
    parsed_bodies: dict[str, list[dict] | None] = {}  # for cross-endpoint check

    print()
    print(f"=== Phase 0 contract check — appointments ===")
    print(f"Base:    {args.base}")
    print(f"Biz:     {args.biz}    Foreign biz: {args.foreign_biz}")
    print(f"Window:  {args.from_date} → {args.to_date}")
    print(f"Output:  {out_dir}")
    print()

    with httpx.Client(follow_redirects=True) as client:
        # ---- 4 endpoints ----
        for idx, (key, path, validator) in enumerate(ENDPOINTS, 1):
            url = f"{args.base.rstrip('/')}/analytics/{args.biz}/appointments/{path}"
            print(f"  [{idx}/4] {path:24s} ", end="", flush=True)
            resp = call(client, "GET", url, headers=headers, params=common_params)
            raw_responses[key] = resp.to_dict()
            (raw_dir / f"0{idx}_{key}.json").write_text(
                json.dumps(resp.to_dict(), indent=2, default=str)
            , encoding="utf-8")

            ep_checks: list[Check] = []
            if resp.status != 200:
                ep_checks.append(Check(
                    name="http_status",
                    severity=Sev.BLOCKER,
                    detail=f"Expected 200, got {resp.status}. "
                           f"Body: {(resp.body_raw or '')[:300]!r}"
                           + (f" (error: {resp.error})" if resp.error else ""),
                ))
                parsed_bodies[key] = None
            elif not isinstance(resp.body, list):
                ep_checks.append(Check(
                    name="response_is_list",
                    severity=Sev.BLOCKER,
                    detail=f"Response is not a JSON array. Got: {type(resp.body).__name__}.",
                ))
                parsed_bodies[key] = None
            else:
                ep_checks.append(Check(
                    name="http_status",
                    severity=Sev.PASS,
                    detail=f"200 OK, {len(resp.body)} rows, {resp.elapsed_ms}ms.",
                ))
                ep_checks.extend(validator(resp.body))
                parsed_bodies[key] = resp.body

            for c in ep_checks:
                c.endpoint = path
            all_checks.extend(ep_checks)

            blockers = sum(1 for c in ep_checks if c.severity is Sev.BLOCKER)
            workarounds = sum(1 for c in ep_checks if c.severity is Sev.WORKAROUND)
            tag = "🚫" if blockers else ("⚠️ " if workarounds else "✅")
            print(f"{tag} {len(ep_checks)} check(s) ({blockers} blocker, {workarounds} workaround)")

        # ---- Cross-cutting ----
        print()
        print("  [+] tenant isolation         ", end="", flush=True)
        ti_url = f"{args.base.rstrip('/')}/analytics/{args.foreign_biz}/appointments/monthly-summary"
        ti_resp = call(client, "GET", ti_url, headers=headers, params=common_params)
        raw_responses["tenant_isolation"] = ti_resp.to_dict()
        (raw_dir / "tenant_isolation.json").write_text(
            json.dumps(ti_resp.to_dict(), indent=2, default=str)
        , encoding="utf-8")
        ti_checks = check_tenant_isolation(ti_resp)
        for c in ti_checks:
            c.endpoint = "tenant_isolation"
        all_checks.extend(ti_checks)
        tag = "🚫" if any(c.severity is Sev.BLOCKER for c in ti_checks) else "✅"
        print(f"{tag} {len(ti_checks)} check(s)")

        print("  [+] auth failure             ", end="", flush=True)
        af_url = f"{args.base.rstrip('/')}/analytics/{args.biz}/appointments/monthly-summary"
        af_resp = call(client, "GET", af_url, headers={}, params=common_params)
        raw_responses["auth_failure"] = af_resp.to_dict()
        (raw_dir / "auth_failure.json").write_text(
            json.dumps(af_resp.to_dict(), indent=2, default=str)
        , encoding="utf-8")
        af_checks = check_auth_failure(af_resp)
        for c in af_checks:
            c.endpoint = "auth_failure"
        all_checks.extend(af_checks)
        tag = "🚫" if any(c.severity is Sev.BLOCKER for c in af_checks) else "✅"
        print(f"{tag} {len(af_checks)} check(s)")

        print("  [+] pagination               ", end="", flush=True)
        pg_url = f"{args.base.rstrip('/')}/analytics/{args.biz}/appointments/staff-breakdown"
        p1 = call(client, "GET", pg_url, headers=headers,
                  params={**common_params, "page": 1, "page_size": args.page_size_test})
        p2 = call(client, "GET", pg_url, headers=headers,
                  params={**common_params, "page": 2, "page_size": args.page_size_test})
        raw_responses["pagination_p1"] = p1.to_dict()
        raw_responses["pagination_p2"] = p2.to_dict()
        (raw_dir / "pagination_p1.json").write_text(
            json.dumps(p1.to_dict(), indent=2, default=str)
        , encoding="utf-8")
        (raw_dir / "pagination_p2.json").write_text(
            json.dumps(p2.to_dict(), indent=2, default=str)
        , encoding="utf-8")
        pg_checks = check_pagination(p1, p2, args.page_size_test)
        for c in pg_checks:
            c.endpoint = "pagination"
        all_checks.extend(pg_checks)
        tag = "🚫" if any(c.severity is Sev.BLOCKER for c in pg_checks) else "✅"
        print(f"{tag} {len(pg_checks)} check(s)")

        print("  [+] empty-window shape       ", end="", flush=True)
        ew_url = f"{args.base.rstrip('/')}/analytics/{args.biz}/appointments/monthly-summary"
        ew_resp = call(client, "GET", ew_url, headers=headers,
                       params={"from": "2099-01-01", "to": "2099-01-31"})
        raw_responses["empty_window"] = ew_resp.to_dict()
        (raw_dir / "empty_window.json").write_text(
            json.dumps(ew_resp.to_dict(), indent=2, default=str)
        , encoding="utf-8")
        ew_checks = check_empty_window(ew_resp)
        for c in ew_checks:
            c.endpoint = "empty_window"
        all_checks.extend(ew_checks)
        tag = "🚫" if any(c.severity is Sev.BLOCKER for c in ew_checks) else "✅"
        print(f"{tag} {len(ew_checks)} check(s)")

        print("  [+] cross-endpoint sanity    ", end="", flush=True)
        ce_checks = check_cross_endpoint_sanity(
            parsed_bodies.get("monthly_summary"),
            parsed_bodies.get("staff_service_cross"),
        )
        for c in ce_checks:
            c.endpoint = "cross_endpoint_sanity"
        all_checks.extend(ce_checks)
        tag = "🚫" if any(c.severity is Sev.BLOCKER for c in ce_checks) else "✅"
        print(f"{tag} {len(ce_checks)} check(s)")

    # ---- Reports ----
    write_markdown_report(out_dir / "report.md", args, all_checks, raw_responses)
    write_json_report(out_dir / "report.json", args, all_checks)

    n_pass = sum(1 for c in all_checks if c.severity is Sev.PASS)
    n_block = sum(1 for c in all_checks if c.severity is Sev.BLOCKER)
    n_work = sum(1 for c in all_checks if c.severity is Sev.WORKAROUND)
    n_cosm = sum(1 for c in all_checks if c.severity is Sev.COSMETIC)

    print()
    print(f"=== Summary: {len(all_checks)} checks total ===")
    print(f"  PASS:       {n_pass}")
    print(f"  BLOCKER:    {n_block}")
    print(f"  WORKAROUND: {n_work}")
    print(f"  COSMETIC:   {n_cosm}")
    print()
    print(f"Wrote report.md    → {out_dir / 'report.md'}")
    print(f"Wrote report.json  → {out_dir / 'report.json'}")
    print(f"Raw responses      → {raw_dir}/")
    print()
    if n_block:
        print(f"❌ {n_block} blocker(s) — DO NOT proceed to Phase 1.")
        print(f"   Resolve each with backend, then re-run.")
        return 1
    elif n_work:
        print(f"⚠️  {n_work} workaround(s) — proceed to Phase 1.")
        print(f"   Document each in extractor with KNOWN_ISSUES line ref.")
        return 0
    else:
        print("✅ No blockers or workarounds. Green light for Phase 1.")
        return 0


# ============================================================================
# Reports
# ============================================================================

SEV_EMOJI = {
    Sev.PASS: "✅",
    Sev.BLOCKER: "🚫",
    Sev.WORKAROUND: "⚠️",
    Sev.COSMETIC: "ℹ️",
}


def write_markdown_report(path: Path, args, checks: list[Check], raw: dict) -> None:
    by_endpoint: dict[str, list[Check]] = {}
    for c in checks:
        by_endpoint.setdefault(c.endpoint or "(none)", []).append(c)

    lines: list[str] = []
    lines.append("# Appointments — Phase 0 contract check report")
    lines.append("")
    lines.append(f"- **Run UTC:** {dt.datetime.now(dt.timezone.utc).isoformat()}")
    lines.append(f"- **Base:** `{args.base}`")
    lines.append(f"- **Test biz:** `{args.biz}` | **Foreign biz:** `{args.foreign_biz}`")
    lines.append(f"- **Window:** `{args.from_date}` → `{args.to_date}`")
    lines.append("")

    n_block = sum(1 for c in checks if c.severity is Sev.BLOCKER)
    n_work = sum(1 for c in checks if c.severity is Sev.WORKAROUND)
    n_cosm = sum(1 for c in checks if c.severity is Sev.COSMETIC)
    n_pass = sum(1 for c in checks if c.severity is Sev.PASS)
    lines.append(f"**Total: {len(checks)} checks** — "
                 f"{n_pass} pass, {n_block} blocker, {n_work} workaround, {n_cosm} cosmetic")
    lines.append("")
    if n_block:
        lines.append(f"> 🚫 **{n_block} blocker(s) — DO NOT proceed to Phase 1.**")
    elif n_work:
        lines.append(f"> ⚠️ **{n_work} workaround(s) — proceed but document each.**")
    else:
        lines.append("> ✅ **No blockers or workarounds.** Green light for Phase 1.")
    lines.append("")

    # Endpoint sections
    endpoint_order = [path for _, path, _ in ENDPOINTS] + [
        "tenant_isolation", "auth_failure", "pagination",
        "empty_window", "cross_endpoint_sanity",
    ]
    seen: set[str] = set()
    for ep in endpoint_order:
        if ep not in by_endpoint:
            continue
        seen.add(ep)
        ep_checks = by_endpoint[ep]
        lines.append(f"## {ep}")
        lines.append("")
        lines.append("| | Check | Severity | Detail |")
        lines.append("|---|---|---|---|")
        for c in ep_checks:
            sym = SEV_EMOJI[c.severity]
            detail = c.detail.replace("|", "\\|").replace("\n", " ")
            lines.append(f"| {sym} | `{c.name}` | {c.severity.value} | {detail} |")
        lines.append("")

    # Any unexpected sections
    for ep, ep_checks in by_endpoint.items():
        if ep in seen:
            continue
        lines.append(f"## {ep}")
        lines.append("")
        lines.append("| | Check | Severity | Detail |")
        lines.append("|---|---|---|---|")
        for c in ep_checks:
            sym = SEV_EMOJI[c.severity]
            detail = c.detail.replace("|", "\\|").replace("\n", " ")
            lines.append(f"| {sym} | `{c.name}` | {c.severity.value} | {detail} |")
        lines.append("")

    # Raw responses inventory
    lines.append("## Raw responses (source of truth)")
    lines.append("")
    lines.append("Every request was saved verbatim under `raw/` for replay, diff against future runs,")
    lines.append("and as a baseline for source-of-truth comparisons. Each file contains:")
    lines.append("`request_url`, `request_method`, `request_params`, `status`, `headers`, `body`, `body_raw`, `elapsed_ms`.")
    lines.append("")
    for name, r in raw.items():
        body_len = len(r.get("body_raw") or "")
        body_summary = ""
        if isinstance(r.get("body"), list):
            body_summary = f" ({len(r['body'])} rows)"
        lines.append(f"- `raw/{name}.json` — status `{r.get('status')}`, "
                     f"{r.get('elapsed_ms', 0)}ms, {body_len}B{body_summary}")
    lines.append("")
    lines.append("## How to use this run as a baseline")
    lines.append("")
    lines.append("1. Commit `raw/` and `report.json` to a private location alongside the dated folder name.")
    lines.append("2. On a future run, diff the new `raw/0*_*.json` against the baseline:")
    lines.append("   `jq '.body' raw/01_monthly_summary.json > /tmp/new && jq '.body' BASELINE/raw/01_monthly_summary.json > /tmp/old && diff /tmp/old /tmp/new`")
    lines.append("3. Or compare summaries: `jq '.summary' report.json` against the baseline's.")
    lines.append("4. Any new blockers → stop; any new workarounds → log in KNOWN_ISSUES §2.")

    path.write_text("\n".join(lines), encoding="utf-8")


def write_json_report(path: Path, args, checks: list[Check]) -> None:
    blob = {
        "run_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "base": args.base,
        "biz": args.biz,
        "foreign_biz": args.foreign_biz,
        "from": args.from_date,
        "to": args.to_date,
        "summary": {
            "total":      len(checks),
            "pass":       sum(1 for c in checks if c.severity is Sev.PASS),
            "blocker":    sum(1 for c in checks if c.severity is Sev.BLOCKER),
            "workaround": sum(1 for c in checks if c.severity is Sev.WORKAROUND),
            "cosmetic":   sum(1 for c in checks if c.severity is Sev.COSMETIC),
        },
        "checks": [c.to_dict() for c in checks],
    }
    path.write_text(json.dumps(blob, indent=2, default=str), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())