#!/usr/bin/env python3
"""
scripts/tests/test_revenue_spec_conformance.py

Revenue Domain — Step 4 Pre-flight Spec Conformance Check
==========================================================

Validates that the real Analytics Backend returns responses that match the
Step 3 API spec BEFORE we wire up analytics_client.py and the ETL extractor.

Checks for each of the 6 revenue endpoints:
  - HTTP 200 OK
  - Top-level shape: { business_id, data: [...], meta: {...}? }
  - business_id in response matches request
  - Every required field in every data row is present, correct type, and
    non-null (unless explicitly nullable per spec)
  - Every required meta field is present with the correct type
  - Reports extra fields not in the spec (informational, not a failure)

This is purely read-only. It does NOT write to the warehouse.

Run:
    # Token via env var (recommended)
    export LEO_API_TOKEN='eyJ...'
    export ANALYTICS_BACKEND_URL='https://uat-ext-api-....azurewebsites.net'
    python scripts/tests/test_revenue_spec_conformance.py

    # Explicit flags
    python scripts/tests/test_revenue_spec_conformance.py \\
        --base-url https://uat-ext-api-....azurewebsites.net \\
        --token   'eyJ...' \\
        --business-id 40 \\
        --start-date 2026-01-01 \\
        --end-date   2026-03-31 \\
        -v

Exit codes:
    0 — all 6 endpoints conform to spec
    1 — any endpoint failed conformance (do NOT proceed to Step 4)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx


# ── ANSI colors ───────────────────────────────────────────────────────────────
class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    RED    = "\033[31m"
    GREEN  = "\033[32m"
    YELLOW = "\033[33m"
    CYAN   = "\033[36m"


def c(text: str, color: str) -> str:
    return f"{color}{text}{C.RESET}"


# ── Spec primitives ───────────────────────────────────────────────────────────

NUMBER = (int, float)   # JSON numeric — decimals come back as float


@dataclass
class SpecField:
    name: str
    types: tuple          # e.g. (int,), (str,), NUMBER
    required: bool = True
    nullable: bool = False  # True if spec says the field may be null

    def check(self, row: dict, path: str) -> list[str]:
        if self.name not in row:
            if self.required:
                return [f"{path}: missing required field '{self.name}'"]
            return []

        val = row[self.name]
        if val is None:
            if not self.nullable:
                return [f"{path}: field '{self.name}' is null but spec does not allow null"]
            return []

        if not isinstance(val, self.types):
            got  = type(val).__name__
            want = " | ".join(t.__name__ for t in self.types)
            return [f"{path}: field '{self.name}' has type {got}, expected {want}"]

        # bool is a subclass of int in Python — disallow bools where int expected
        if self.types == (int,) and isinstance(val, bool):
            return [f"{path}: field '{self.name}' is bool, expected int"]

        return []


@dataclass
class EndpointSpec:
    label: str
    path: str
    request_payload: dict
    data_fields: list[SpecField]
    meta_fields: list[SpecField] = field(default_factory=list)
    allow_empty_data: bool = False
    # Some endpoints might legitimately return 0 rows for the test range
    # (e.g. a business with no promo usage in that window).


# ── Build the 6 Revenue specs per the Step 3 contract ─────────────────────────

def build_specs(business_id: int, start_date: str, end_date: str) -> list[EndpointSpec]:
    base = {
        "business_id": business_id,
        "start_date":  start_date,
        "end_date":    end_date,
    }

    return [
        # ─── E1 ───────────────────────────────────────────────────────────────
        EndpointSpec(
            label="E1 — Monthly Summary",
            path="/api/v1/leo/revenue/monthly-summary",
            request_payload={**base, "group_by": "month"},
            data_fields=[
                SpecField("period",          (str,)),
                SpecField("visit_count",     (int,)),
                SpecField("service_revenue", NUMBER),
                SpecField("total_tips",      NUMBER),
                SpecField("total_tax",       NUMBER),
                SpecField("total_collected", NUMBER),
                SpecField("total_discounts", NUMBER),
                SpecField("gc_redemptions",  NUMBER),
                SpecField("avg_ticket",      NUMBER),
                SpecField("mom_growth_pct",  NUMBER, nullable=True),  # null for first period
                SpecField("refund_count",    (int,)),
                SpecField("cancel_count",    (int,)),
            ],
            meta_fields=[
                SpecField("total_service_revenue", NUMBER),
                SpecField("total_visits",          (int,)),
                # best/worst_period are null when data is empty — confirmed against UAT
                SpecField("best_period",           (str,), nullable=True),
                SpecField("worst_period",          (str,), nullable=True),
                SpecField("trend_slope",           NUMBER),
            ],
        ),

        # ─── E2 ───────────────────────────────────────────────────────────────
        EndpointSpec(
            label="E2 — Payment Types",
            path="/api/v1/leo/revenue/payment-types",
            request_payload=dict(base),
            data_fields=[
                SpecField("payment_type", (str,)),
                SpecField("visit_count",  (int,)),
                SpecField("revenue",      NUMBER),
                SpecField("pct_of_total", NUMBER),
            ],
        ),

        # ─── E3 ───────────────────────────────────────────────────────────────
        EndpointSpec(
            label="E3 — By Staff",
            path="/api/v1/leo/revenue/by-staff",
            request_payload={**base, "limit": 10},
            data_fields=[
                SpecField("emp_id",          (int,)),
                SpecField("staff_name",      (str,)),
                SpecField("visit_count",     (int,)),
                SpecField("service_revenue", NUMBER),
                SpecField("tips_collected",  NUMBER),
                SpecField("avg_ticket",      NUMBER),
                SpecField("revenue_rank",    (int,)),
            ],
        ),

        # ─── E4 ───────────────────────────────────────────────────────────────
        EndpointSpec(
            label="E4 — By Location",
            path="/api/v1/leo/revenue/by-location",
            request_payload={**base, "group_by": "month"},
            data_fields=[
                SpecField("location_id",          (int,)),
                SpecField("location_name",        (str,)),
                SpecField("period",               (str,)),
                SpecField("visit_count",          (int,)),
                SpecField("service_revenue",      NUMBER),
                SpecField("total_tips",           NUMBER),
                SpecField("avg_ticket",           NUMBER),
                SpecField("total_discounts",      NUMBER),
                SpecField("gc_redemptions",       NUMBER),
                SpecField("pct_of_total_revenue", NUMBER),
                SpecField("mom_growth_pct",       NUMBER, nullable=True),
            ],
        ),

        # ─── E5 ───────────────────────────────────────────────────────────────
        EndpointSpec(
            label="E5 — Promo Impact",
            path="/api/v1/leo/revenue/promo-impact",
            request_payload=dict(base),
            data_fields=[
                SpecField("promo_code",             (str,)),
                SpecField("promo_description",      (str,), required=False, nullable=True),
                SpecField("location_id",            (int,)),
                SpecField("location_name",          (str,)),
                SpecField("times_used",             (int,)),
                SpecField("total_discount_given",   NUMBER),
                SpecField("revenue_after_discount", NUMBER),
            ],
            meta_fields=[
                SpecField("total_discount_all_promos", NUMBER),
                SpecField("promo_visit_count",         (int,)),
            ],
            allow_empty_data=True,
        ),

        # ─── E6 ───────────────────────────────────────────────────────────────
        EndpointSpec(
            label="E6 — Failed / Refunds",
            path="/api/v1/leo/revenue/failed-refunds",
            request_payload=dict(base),
            data_fields=[
                SpecField("status_code",        (int,)),
                SpecField("status_label",       (str,)),
                SpecField("visit_count",        (int,)),
                SpecField("lost_revenue",       NUMBER),
                SpecField("avg_lost_per_visit", NUMBER),
            ],
            meta_fields=[
                SpecField("total_lost_revenue",    NUMBER),
                SpecField("total_affected_visits", (int,)),
            ],
            allow_empty_data=True,
        ),
    ]


# ── Validation ────────────────────────────────────────────────────────────────

def validate_response(spec: EndpointSpec, resp: Any, business_id: int) -> list[str]:
    issues: list[str] = []

    if not isinstance(resp, dict):
        return [f"response root is {type(resp).__name__}, expected JSON object"]

    # Top-level: business_id
    if "business_id" not in resp:
        issues.append("missing 'business_id' in response root")
    elif str(resp["business_id"]) != str(business_id):
        issues.append(
            f"business_id mismatch: got {resp['business_id']!r}, expected {business_id!r} "
            f"(possible tenant-isolation drift — investigate)"
        )

    # Top-level: data array
    data = resp.get("data")
    if data is None:
        issues.append("missing 'data' array in response root")
    elif not isinstance(data, list):
        issues.append(f"'data' is {type(data).__name__}, expected list")
    elif not data and not spec.allow_empty_data:
        issues.append(
            "'data' is empty — cannot validate field types. "
            "Try a business_id + date range known to have activity "
            "(UAT tip: business_id=40 with 2026-01-01..2026-03-31 returns data)."
        )
    elif data:
        for i, row in enumerate(data):
            if not isinstance(row, dict):
                issues.append(f"data[{i}] is {type(row).__name__}, expected object")
                continue
            for f in spec.data_fields:
                issues.extend(f.check(row, f"data[{i}]"))

    # meta
    if spec.meta_fields:
        if "meta" not in resp:
            issues.append("missing 'meta' object (spec defines required meta fields)")
        elif not isinstance(resp["meta"], dict):
            issues.append(f"'meta' is {type(resp['meta']).__name__}, expected object")
        else:
            for f in spec.meta_fields:
                issues.extend(f.check(resp["meta"], "meta"))

    return issues


def find_extra_fields(spec: EndpointSpec, resp: dict) -> list[str]:
    """Return list of fields the backend returned that the spec doesn't mention."""
    extras: list[str] = []

    data = resp.get("data")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        known = {f.name for f in spec.data_fields}
        extras.extend(f"data[].{k}" for k in data[0].keys() if k not in known)

    meta = resp.get("meta")
    if isinstance(meta, dict) and spec.meta_fields:
        known = {f.name for f in spec.meta_fields}
        extras.extend(f"meta.{k}" for k in meta.keys() if k not in known)

    return extras


# ── Runner ────────────────────────────────────────────────────────────────────

def run_checks(base_url: str,
               business_id: int,
               start_date: str,
               end_date: str,
               token: Optional[str],
               verbose: bool) -> int:
    specs = build_specs(business_id, start_date, end_date)

    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        auth_line = f"Bearer token (len={len(token)})"
    else:
        auth_line = c("NONE — backend will likely 401", C.YELLOW)

    print(c(f"\n{'═' * 72}", C.CYAN))
    print(c("  Revenue Domain — Spec Conformance Check (Step 4 pre-flight)", C.BOLD))
    print(c(f"  Target   : {base_url}", C.DIM))
    print(c(f"  Business : {business_id}", C.DIM))
    print(c(f"  Range    : {start_date}  →  {end_date}", C.DIM))
    print(f"  {c('Auth', C.DIM)}     : {auth_line}")
    print(c(f"{'═' * 72}\n", C.CYAN))

    results: list[tuple[EndpointSpec, Optional[int], Optional[dict], list[str]]] = []

    with httpx.Client(base_url=base_url, headers=headers, timeout=30.0) as client:
        for spec in specs:
            try:
                resp = client.post(spec.path, json=spec.request_payload)
            except httpx.RequestError as e:
                results.append((spec, None, None, [f"connection error: {e!s}"]))
                print(c(f"  ❌ {spec.label}", C.RED))
                print(c(f"     POST {spec.path}", C.DIM))
                print(c(f"     → connection error: {e}", C.RED))
                print()
                continue

            # Non-200 → surface status + body preview
            if resp.status_code != 200:
                body_preview = resp.text[:300].replace("\n", " ")
                issue = f"HTTP {resp.status_code}: {body_preview}"
                results.append((spec, resp.status_code, None, [issue]))
                print(c(f"  ❌ {spec.label}", C.RED))
                print(c(f"     POST {spec.path}", C.DIM))
                print(c(f"     → HTTP {resp.status_code}", C.RED))
                print(c(f"     → body: {body_preview}", C.DIM))
                print()
                continue

            # Parse JSON
            try:
                body = resp.json()
            except Exception as e:
                results.append((spec, 200, None, [f"response not JSON: {e!s}"]))
                print(c(f"  ❌ {spec.label}", C.RED))
                print(c(f"     POST {spec.path}", C.DIM))
                print(c(f"     → response not JSON: {e}", C.RED))
                print()
                continue

            # Validate against spec
            issues = validate_response(spec, body, business_id)
            extras = find_extra_fields(spec, body)
            results.append((spec, 200, body, issues))

            status = c("✅ PASS", C.GREEN) if not issues else c("❌ FAIL", C.RED)
            print(f"  {status}  {c(spec.label, C.BOLD)}")
            print(c(f"     POST {spec.path}", C.DIM))

            rows = body.get("data") or []
            print(c(f"     → {len(rows)} data rows", C.DIM))

            if issues:
                for msg in issues:
                    print(c(f"       • {msg}", C.RED))

            if extras:
                label = "extra fields (not a failure — flag for spec update?)"
                print(c(f"     → {label}:", C.YELLOW))
                for e in extras:
                    print(c(f"       + {e}", C.YELLOW))

            if verbose and rows:
                sample = json.dumps(rows[0], default=str)
                if len(sample) > 220:
                    sample = sample[:220] + " …"
                print(c("     → sample row:", C.DIM))
                print(c(f"       {sample}", C.DIM))

            print()

    # ── Summary ───────────────────────────────────────────────────────────────
    passed = sum(1 for _, _, _, issues in results if not issues)
    total  = len(results)

    print(c("═" * 72, C.CYAN))
    if passed == total:
        print(c(f"  ✅ ALL {total} ENDPOINTS CONFORM TO SPEC", C.GREEN + C.BOLD))
        print(c("     → Safe to proceed to Step 4 (ETL wiring)", C.GREEN))
        exit_code = 0
    else:
        print(c(f"  ❌ {total - passed} of {total} endpoints failed conformance",
                C.RED + C.BOLD))
        print(c("     → Fix spec drift with the backend team before wiring the ETL",
                C.RED))
        exit_code = 1
    print(c("═" * 72, C.CYAN))
    print()

    return exit_code


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Revenue Domain spec conformance check (Step 4 pre-flight)"
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("ANALYTICS_BACKEND_URL"),
        help="Analytics Backend base URL (defaults to $ANALYTICS_BACKEND_URL)",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("LEO_API_TOKEN"),
        help="Bearer token for Authorization header (defaults to $LEO_API_TOKEN)",
    )
    parser.add_argument(
        "--business-id",
        type=int,
        default=int(os.getenv("TEST_BUSINESS_ID", "40")),
        help="business_id to query (default: 40 — has UAT data in Q1 2026)",
    )
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date",   default="2026-03-31")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print sample rows and extra fields")
    args = parser.parse_args()

    if not args.base_url:
        print(c("ERROR: No backend URL provided.", C.RED + C.BOLD))
        print(c("  Set $ANALYTICS_BACKEND_URL or pass --base-url https://…", C.DIM))
        sys.exit(2)

    if not args.token:
        print(c("WARNING: No token provided — backend will likely return 401.", C.YELLOW))
        print(c("  Set $LEO_API_TOKEN or pass --token <bearer_token> to authenticate.", C.DIM))
        print()

    sys.exit(
        run_checks(
            args.base_url.rstrip("/"),
            args.business_id,
            args.start_date,
            args.end_date,
            args.token,
            args.verbose,
        )
    )


if __name__ == "__main__":
    main()