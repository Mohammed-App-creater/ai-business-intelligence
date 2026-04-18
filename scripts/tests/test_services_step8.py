"""
scripts/tests/test_services_step8.py
=====================================
Step 8 — Services Domain Sign-off

Two tests:
  1. Improvised questions  — 10 new phrasings the AI hasn't seen
  2. Tenant isolation      — business_id=99 must get NO business_id=42 data

Usage:
    python scripts/tests/test_services_step8.py
    python scripts/tests/test_services_step8.py --output results/step8_services.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass, field

import httpx

CHAT_ENDPOINT   = "http://localhost:8000/api/v1/chat"
BUSINESS_ID     = "42"
OTHER_TENANT    = "99"
REQUEST_TIMEOUT = 45.0


# ─────────────────────────────────────────────────────────────────────────────
# 10 improvised questions — different phrasing, edge cases, new angles
# ─────────────────────────────────────────────────────────────────────────────

IMPROVISED: dict[str, dict] = {
    "I1": {
        "text":     "What's making us the most money right now?",
        "note":     "Colloquial revenue-by-service — should mention Facial Treatment",
        "must_contain_one_of": ["facial", "revenue", "most", "$"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "I2": {
        "text":     "Do clients tend to get their nails done together — mani and pedi?",
        "note":     "Natural co-occurrence question without using technical terms",
        "must_contain_one_of": ["manicure", "pedicure", "together", "combo", "pair", "yes"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "I3": {
        "text":     "I feel like Hair Color appointments always take forever — is that true?",
        "note":     "Casual duration question — should cite actual vs scheduled",
        "must_contain_one_of": ["hair color", "minutes", "longer", "schedule", "over", "120", "127", "128"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "I4": {
        "text":     "Is there anything on our menu that nobody's buying anymore?",
        "note":     "Dormant service question — should find Hot Stone Therapy",
        "must_contain_one_of": ["hot stone", "dormant", "no sales", "hasn't sold", "not sold"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "I5": {
        "text":     "Which of our treatments gives us the worst margins?",
        "note":     "Margin question with colloquial phrasing",
        "must_contain_one_of": ["hair color", "margin", "commission", "20%", "worst", "lowest"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "I6": {
        "text":     "How's the Express Facial doing since we launched it?",
        "note":     "New service performance — tests new-this-year retrieval",
        "must_contain_one_of": ["express facial", "new", "growing", "added", "$", "revenue", "performed"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "I7": {
        "text":     "Who on my team does the most massages?",
        "note":     "Staff x service question — should find James Carter or Aisha",
        "must_contain_one_of": ["james", "aisha", "carter", "nwosu", "massage"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "I8": {
        "text":     "Are we giving too many discounts on any service?",
        "note":     "Discount detection — should flag Hair Color at 10%",
        "must_contain_one_of": ["hair color", "discount", "10%", "%"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "I9": {
        "text":     "What should first-time clients try when they come in?",
        "note":     "First service for new clients — should mention Facial Treatment",
        "must_contain_one_of": ["facial", "first", "new client", "popular", "recommend"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "I10": {
        "text":     "How do my services compare at Main St versus Westside?",
        "note":     "Location comparison — should cite both locations with numbers",
        "must_contain_one_of": ["main st", "westside"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
}

# Tenant isolation: known data markers from business_id=42 that must NOT
# appear in answers for business_id=99
TENANT_42_MARKERS = [
    "facial treatment", "swedish massage", "hair color", "manicure", "pedicure",
    "express facial", "hot stone", "maria", "james", "aisha", "tom rivera",
    "main st", "westside", "$2,496", "$3,978", "$2,964",
]


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

async def ask_question(
    endpoint: str,
    business_id: str,
    question: str,
    timeout: float = REQUEST_TIMEOUT,
) -> tuple[str, float]:
    payload = {
        "business_id": business_id,
        "org_id":      business_id,
        "session_id":  "step8-services-signoff",
        "question":    question,
    }
    t0 = time.perf_counter()
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(endpoint, json=payload)
        resp.raise_for_status()
        body = resp.json()
    latency = (time.perf_counter() - t0) * 1000

    answer = ""
    if isinstance(body, dict):
        answer = (
            body.get("answer")
            or body.get("response")
            or body.get("message")
            or body.get("content")
            or ""
        )
        if not answer and "data" in body and isinstance(body["data"], dict):
            answer = body["data"].get("answer", "")
    elif isinstance(body, str):
        answer = body

    return str(answer), latency


async def run_step8(
    endpoint: str = CHAT_ENDPOINT,
    business_id: str = BUSINESS_ID,
    output_path: str | None = None,
) -> None:

    print(f"\n{'='*62}")
    print(f"  LEO AI BI — Step 8: Services Domain Sign-off")
    print(f"  Endpoint : {endpoint}")
    print(f"{'='*62}")

    # ── Part 1: Improvised questions ──────────────────────────────────
    print(f"\n  PART 1 — Improvised questions (10 new phrasings)\n")

    imp_results: list[dict] = []

    for q_id, spec in IMPROVISED.items():
        print(f"  -> [{q_id}] {spec['text']}")
        print(f"       ({spec['note']})")

        try:
            answer, latency = await ask_question(endpoint, business_id, spec["text"])
            a_lower = answer.lower()
            issues = []

            # Must contain at least one expected keyword
            must_kw = spec.get("must_contain_one_of", [])
            if must_kw and not any(kw.lower() in a_lower for kw in must_kw):
                issues.append(f"missing_keyword(need one of: {must_kw})")

            # Must NOT contain refusal signals
            must_not = spec.get("must_not_contain", [])
            for bad in must_not:
                if bad.lower() in a_lower:
                    issues.append(f"contains_forbidden({bad})")
                    break

            passed = len(issues) == 0
            status = "PASS" if passed else "FAIL"
            print(f"  {'[PASS]' if passed else '[FAIL]'} [{q_id}] latency={latency:>6.0f}ms  issues: {', '.join(issues) or 'none'}")

            if not passed:
                preview = answer[:200].replace("\n", " ")
                print(f"         Answer: {preview}{'...' if len(answer) > 200 else ''}")

        except Exception as exc:
            passed = False
            issues = [f"request_error: {exc}"]
            answer = ""
            latency = 0
            print(f"  [FAIL] [{q_id}] ERROR: {exc}")

        imp_results.append({
            "q_id": q_id, "question": spec["text"], "note": spec["note"],
            "passed": passed, "issues": issues,
            "latency_ms": round(latency, 1), "answer": answer,
        })

    # ── Part 2: Tenant isolation ──────────────────────────────────────
    print(f"\n\n  PART 2 — Tenant isolation (business_id={OTHER_TENANT} "
          f"must not see business_id={business_id} data)\n")

    iso_questions = [
        ("ISO1", "Which service generates the most revenue?",
         "must not see business_id=42 service names or revenue figures"),
        ("ISO2", "What are my most popular services?",
         "must not see business_id=42 service names"),
        ("ISO3", "Are any services dormant?",
         "must not see Hot Stone Therapy or any business_id=42 service"),
    ]

    iso_results: list[dict] = []

    for check_id, question, note in iso_questions:
        print(f"  -> [{check_id}] {question} (tenant={OTHER_TENANT})")
        print(f"       ({note})")

        try:
            answer, latency = await ask_question(endpoint, OTHER_TENANT, question)
            a_lower = answer.lower()
            issues = []

            # Check no business_id=42 markers leak
            for marker in TENANT_42_MARKERS:
                if marker.lower() in a_lower:
                    issues.append(f"LEAK: found '{marker}' in tenant {OTHER_TENANT} answer")

            passed = len(issues) == 0
            status = "[PASS]" if passed else "[FAIL]"
            print(f"  {status} [{check_id}] latency={latency:>6.0f}ms  issues: {', '.join(issues) or 'none'}")

            if not passed:
                preview = answer[:200].replace("\n", " ")
                print(f"         Answer: {preview}{'...' if len(answer) > 200 else ''}")
            else:
                preview = answer[:120].replace("\n", " ")
                print(f"       Answer (first 120 chars): {preview}...")

        except Exception as exc:
            passed = False
            issues = [f"request_error: {exc}"]
            answer = ""
            latency = 0
            print(f"  [FAIL] [{check_id}] ERROR: {exc}")

        iso_results.append({
            "check": check_id, "tenant": OTHER_TENANT,
            "question": question, "note": note,
            "passed": passed, "issues": issues,
            "latency_ms": round(latency, 1), "answer": answer,
        })

    # ── Summary ───────────────────────────────────────────────────────
    imp_passed = sum(1 for r in imp_results if r["passed"])
    imp_total  = len(imp_results)
    iso_passed = sum(1 for r in iso_results if r["passed"])
    iso_total  = len(iso_results)
    all_passed = imp_passed == imp_total and iso_passed == iso_total

    print(f"\n{'='*62}")
    print(f"  Step 8 Sign-off Results — Services Domain")
    print(f"{'='*62}")
    print(f"  Improvised questions : {imp_passed}/{imp_total} passed")
    print(f"  Tenant isolation     : {iso_passed}/{iso_total} passed")

    if imp_passed < imp_total:
        print(f"\n  -- Improvised failures -------------------")
        for r in imp_results:
            if not r["passed"]:
                print(f"    FAIL [{r['q_id']}] {r['question']}")
                print(f"         Issues: {', '.join(r['issues'])}")

    if iso_passed < iso_total:
        print(f"\n  -- ISOLATION FAILURES --------------------")
        for r in iso_results:
            if not r["passed"]:
                print(f"    FAIL [{r['check']}] tenant={r['tenant']}: {', '.join(r['issues'])}")

    print(f"{'='*62}")
    if all_passed:
        print("  STEP 8 PASSED — Services domain COMPLETE")
        print("     Ready to move to Domain 5: Clients")
    else:
        print("  STEP 8 INCOMPLETE — fix remaining issues above")
    print(f"{'='*62}\n")

    # ── JSON output ───────────────────────────────────────────────────
    if output_path:
        import os
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        report = {
            "domain": "services",
            "step": 8,
            "business_id": business_id,
            "other_tenant": OTHER_TENANT,
            "endpoint": endpoint,
            "improvised": {
                "total": imp_total, "passed": imp_passed,
                "results": imp_results,
            },
            "tenant_isolation": {
                "total": iso_total, "passed": iso_passed,
                "results": iso_results,
            },
            "all_passed": all_passed,
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"  Results saved to: {output_path}\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Step 8 — Services domain sign-off"
    )
    parser.add_argument(
        "--endpoint", type=str, default=CHAT_ENDPOINT,
        help=f"Chat endpoint URL (default: {CHAT_ENDPOINT})",
    )
    parser.add_argument(
        "--business-id", type=str, default=BUSINESS_ID, dest="business_id",
        help=f"Business ID (default: {BUSINESS_ID})",
    )
    parser.add_argument(
        "--output", type=str, default=None, metavar="PATH",
        help="Save JSON results to this path",
    )
    args = parser.parse_args()

    asyncio.run(
        run_step8(
            endpoint=args.endpoint,
            business_id=args.business_id,
            output_path=args.output,
        )
    )