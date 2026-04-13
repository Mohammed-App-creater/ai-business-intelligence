"""
scripts/tests/test_appointments_step8.py
=========================================
Step 8 — Appointments Domain Sign-off

Two tests:
  1. Improvised questions  — 10 new phrasings the AI hasn't seen
  2. Tenant isolation      — business_id=99 must get NO business_id=42 data

Usage:
    python scripts/tests/test_appointments_step8.py
    python scripts/tests/test_appointments_step8.py --output results/step8_appointments.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx

CHAT_ENDPOINT  = "http://localhost:8000/api/v1/chat"
BUSINESS_ID    = "42"
OTHER_TENANT   = "99"     # must get zero data from business 42
REQUEST_TIMEOUT = 30.0

# ─────────────────────────────────────────────────────────────────────────────
# 10 improvised questions — different phrasing, edge cases, new angles
# ─────────────────────────────────────────────────────────────────────────────

IMPROVISED: dict[str, dict] = {
    "I1": {
        "text":     "Are we getting more no-shows lately?",
        "note":     "Colloquial trend phrasing — same as Q25 but different words",
        "expect_numbers": True,
        "must_contain_one_of": ["no-show", "no show", "%"],
        "must_not_contain": ["don't have", "no data"],
    },
    "I2": {
        "text":     "Which of our services runs the longest?",
        "note":     "Natural language for service duration ranking",
        "expect_numbers": True,
        "must_contain_one_of": ["massage", "hair color", "minute", "min"],
        "must_not_contain": ["don't have", "no data"],
    },
    "I3": {
        "text":     "How busy were we in February this year?",
        "note":     "Colloquial month reference — 2026 data in fixtures",
        "expect_numbers": True,
        "must_contain_one_of": ["february", "2026", "appointment", "booked"],
        "must_not_contain": ["don't have", "no data"],
    },
    "I4": {
        "text":     "Is Aisha Nwosu booking more or fewer clients than last month?",
        "note":     "Named staff member trend — tests staff retrieval by name",
        "expect_numbers": True,
        "must_contain_one_of": ["aisha", "nwosu"],
        "must_not_contain": ["don't have", "no data", "no staff"],
    },
    "I5": {
        "text":     "Which service do clients keep coming back for?",
        "note":     "Repeat client question rephrased naturally",
        "expect_numbers": True,
        "must_contain_one_of": ["facial", "massage", "repeat", "client"],
        "must_not_contain": ["don't have", "no data"],
    },
    "I6": {
        "text":     "Are morning or evening slots more popular at Westside?",
        "note":     "Location + time slot combination — tests per-location retrieval",
        "expect_numbers": True,
        "must_contain_one_of": ["westside", "morning", "evening"],
        "must_not_contain": ["don't have", "no data"],
    },
    "I7": {
        "text":     "What percentage of our bookings get cancelled?",
        "note":     "Cancellation rate in percentage phrasing",
        "expect_numbers": True,
        "must_contain_one_of": ["%", "percent", "cancellation", "cancel"],
        "must_not_contain": ["don't have", "no data"],
    },
    "I8": {
        "text":     "How does Main St perform compared to Westside?",
        "note":     "Location comparison — tests both locations retrieved",
        "expect_numbers": True,
        "must_contain_one_of": ["main st", "westside"],
        "must_not_contain": ["don't have", "no data"],
    },
    "I9": {
        "text":     "Give me a summary of last month's appointment activity.",
        "note":     "Open-ended summary request — tests completeness",
        "expect_numbers": True,
        "must_contain_one_of": ["completed", "cancelled", "booked", "appointment"],
        "must_not_contain": ["don't have", "no data"],
    },
    "I10": {
        "text":     "Which staff member handles the most Hair Color appointments?",
        "note":     "Staff × service cross question — tests cross doc retrieval",
        "expect_numbers": True,
        "must_contain_one_of": ["hair color", "james", "carter"],
        "must_not_contain": ["don't have", "no data"],
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Tenant isolation checks
# ─────────────────────────────────────────────────────────────────────────────

ISOLATION_CHECKS: list[dict] = [
    {
        "question":     "How many appointments did we have last month?",
        "tenant":       OTHER_TENANT,
        "must_not_contain": ["main st", "westside", "maria lopez", "james carter",
                             "aisha nwosu", "tom rivera", "247", "234"],
        "note": "business_id=99 must not see business_id=42 staff or location names or exact counts",
    },
    {
        "question":     "Which branch had the most appointments?",
        "tenant":       OTHER_TENANT,
        "must_not_contain": ["main st", "westside", "miami"],
        "note": "business_id=99 must not see business_id=42 location names",
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

NUMBER_RE = re.compile(r"\b\d[\d,\.]*%?\b")

async def ask(client: httpx.AsyncClient, question: str, business_id: str) -> tuple[str, int, float]:
    payload = {"business_id": business_id, "org_id": business_id, "question": question}
    t0 = time.perf_counter()
    try:
        resp = await client.post(CHAT_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)
        latency_ms = (time.perf_counter() - t0) * 1000
        if resp.status_code != 200:
            return f"HTTP {resp.status_code}: {resp.text}", resp.status_code, latency_ms
        body = resp.json()
        answer = body.get("answer") or body.get("response") or json.dumps(body)
        return answer, resp.status_code, latency_ms
    except Exception as e:
        latency_ms = (time.perf_counter() - t0) * 1000
        return f"ERROR: {e}", 500, latency_ms


def score_improvised(q_id: str, spec: dict, answer: str, http_status: int) -> tuple[bool, list[str]]:
    issues = []
    answer_lower = answer.lower()

    if not answer.strip() or http_status != 200:
        issues.append("empty_or_error")

    if spec.get("expect_numbers") and not NUMBER_RE.search(answer):
        issues.append("no_number")

    must_have = spec.get("must_contain_one_of", [])
    if must_have and not any(w.lower() in answer_lower for w in must_have):
        issues.append(f"missing_one_of({must_have})")

    for phrase in spec.get("must_not_contain", []):
        if phrase.lower() in answer_lower:
            issues.append(f"contains_forbidden({phrase!r})")

    return len(issues) == 0, issues


def score_isolation(check: dict, answer: str, http_status: int) -> tuple[bool, list[str]]:
    issues = []
    answer_lower = answer.lower()

    if http_status != 200:
        issues.append("http_error")

    for phrase in check["must_not_contain"]:
        if phrase.lower() in answer_lower:
            issues.append(f"data_leak({phrase!r})")

    return len(issues) == 0, issues


# ─────────────────────────────────────────────────────────────────────────────
# Main runner
# ─────────────────────────────────────────────────────────────────────────────

async def run(output_path: Optional[str]) -> None:
    print(f"\n{'═'*62}")
    print(f"  LEO AI BI — Step 8: Appointments Domain Sign-off")
    print(f"  Endpoint : {CHAT_ENDPOINT}")
    print(f"{'═'*62}\n")

    imp_results = []
    iso_results = []

    async with httpx.AsyncClient() as client:

        # ── Part 1: Improvised questions ──────────────────────────────────────
        print("  PART 1 — Improvised questions (10 new phrasings)\n")

        for q_id, spec in IMPROVISED.items():
            q_text = spec["text"]
            note   = spec.get("note", "")
            print(f"  → [{q_id}] {q_text[:65]}{'...' if len(q_text)>65 else ''}")
            if note:
                print(f"       ({note})")

            answer, http_status, latency_ms = await ask(client, q_text, BUSINESS_ID)
            passed, issues = score_improvised(q_id, spec, answer, http_status)

            imp_results.append({
                "q_id": q_id, "question": q_text, "note": note,
                "passed": passed, "issues": issues,
                "latency_ms": round(latency_ms, 1), "answer": answer,
            })

            icon = "✅" if passed else "❌"
            issues_str = ", ".join(issues) if issues else "—"
            print(f"  {icon} [{q_id}] latency={latency_ms:>6.0f}ms  issues: {issues_str}")
            if not passed:
                print(f"       Answer: {answer[:200]}")
            print()

        # ── Part 2: Tenant isolation ──────────────────────────────────────────
        print(f"\n  PART 2 — Tenant isolation (business_id={OTHER_TENANT} must not see business_id={BUSINESS_ID} data)\n")

        for i, check in enumerate(ISOLATION_CHECKS, 1):
            q_text = check["question"]
            tenant = check["tenant"]
            note   = check["note"]
            print(f"  → [ISO{i}] {q_text} (tenant={tenant})")
            print(f"       ({note})")

            answer, http_status, latency_ms = await ask(client, q_text, tenant)
            passed, issues = score_isolation(check, answer, http_status)

            iso_results.append({
                "check": f"ISO{i}", "question": q_text, "tenant": tenant,
                "passed": passed, "issues": issues,
                "latency_ms": round(latency_ms, 1), "answer": answer,
            })

            icon = "✅" if passed else "❌"
            issues_str = ", ".join(issues) if issues else "—"
            print(f"  {icon} [ISO{i}] latency={latency_ms:>6.0f}ms  issues: {issues_str}")
            if not passed:
                print(f"       ⚠️  DATA LEAK DETECTED: {issues}")
                print(f"       Answer: {answer[:300]}")
            else:
                print(f"       Answer (first 120 chars): {answer[:120]}...")
            print()

    # ── Summary ───────────────────────────────────────────────────────────────
    imp_passed  = sum(1 for r in imp_results if r["passed"])
    imp_total   = len(imp_results)
    iso_passed  = sum(1 for r in iso_results if r["passed"])
    iso_total   = len(iso_results)
    all_passed  = imp_passed == imp_total and iso_passed == iso_total

    print(f"{'═'*62}")
    print(f"  Step 8 Sign-off Results — Appointments Domain")
    print(f"{'═'*62}")
    print(f"  Improvised questions : {imp_passed}/{imp_total} passed")
    print(f"  Tenant isolation     : {iso_passed}/{iso_total} passed")

    if imp_passed < imp_total:
        print(f"\n  ── Improvised failures ───────────────────────")
        for r in imp_results:
            if not r["passed"]:
                print(f"    ❌ [{r['q_id']}] {r['question']}")
                print(f"         Issues: {', '.join(r['issues'])}")

    if iso_passed < iso_total:
        print(f"\n  ── ⚠️  ISOLATION FAILURES ────────────────────")
        for r in iso_results:
            if not r["passed"]:
                print(f"    ❌ [{r['check']}] tenant={r['tenant']}: {', '.join(r['issues'])}")

    print(f"{'═'*62}")
    if all_passed:
        print("  ✅ STEP 8 PASSED — Appointments domain COMPLETE")
        print("     Ready to move to Domain 3: Staff Performance")
    else:
        print("  ❌ STEP 8 INCOMPLETE — fix remaining issues above")
    print(f"{'═'*62}\n")

    if output_path:
        import os
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        report = {
            "domain": "appointments", "step": 8,
            "improvised": {"passed": imp_passed, "total": imp_total, "results": imp_results},
            "isolation":  {"passed": iso_passed, "total": iso_total, "results": iso_results},
            "overall_passed": all_passed,
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"  📄 Results saved to: {output_path}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Step 8 — Appointments sign-off")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()
    asyncio.run(run(args.output))
