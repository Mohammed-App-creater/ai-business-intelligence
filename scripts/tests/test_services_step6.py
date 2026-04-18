"""
scripts/tests/test_services_step6.py
=====================================
Step 6 — Services Domain Test Harness

Runs all 30 acceptance questions against the live chat endpoint,
scores each answer on: non-empty, contains numbers, keyword match,
no hallucination signals.

Usage:
    python scripts/tests/test_services_step6.py
    python scripts/tests/test_services_step6.py --question Q1
    python scripts/tests/test_services_step6.py --output results/step6_services.json
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

CHAT_ENDPOINT   = "http://localhost:8000/api/v1/chat"
BUSINESS_ID     = "42"
REQUEST_TIMEOUT = 45.0


# ─────────────────────────────────────────────────────────────────────────────
# 30 acceptance questions — one per Step 1 question
# ─────────────────────────────────────────────────────────────────────────────

QUESTIONS: dict[str, dict] = {

    # ── Popularity & Volume ───────────────────────────────────────────────
    "Q1": {
        "text":     "Which service is booked the most last month?",
        "category": "Popularity",
        "expect_numbers": True,
        "must_contain_one_of": ["facial", "booked", "most", "popular"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "Q2": {
        "text":     "Which service was actually performed the most last month?",
        "category": "Popularity",
        "expect_numbers": True,
        "must_contain_one_of": ["facial", "performed", "completed"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q3": {
        "text":     "What are my top 5 services by revenue this quarter?",
        "category": "Popularity",
        "expect_numbers": True,
        "must_contain_one_of": ["facial", "massage", "revenue", "top"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q4": {
        "text":     "Which service is booked the least?",
        "category": "Popularity",
        "expect_numbers": False,
        "must_contain_one_of": ["hair color", "pedicure", "least", "fewest", "lowest"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q5": {
        "text":     "How many services did we perform in total last month?",
        "category": "Popularity",
        "expect_numbers": True,
        "must_contain_one_of": ["total", "performed", "completed"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Revenue & Pricing ─────────────────────────────────────────────────
    "Q6": {
        "text":     "How much total revenue did Facial Treatment generate last month?",
        "category": "Revenue",
        "expect_numbers": True,
        "must_contain_one_of": ["facial", "$", "revenue"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q7": {
        "text":     "What is the average price actually charged for a Facial Treatment versus the list price?",
        "category": "Revenue",
        "expect_numbers": True,
        "must_contain_one_of": ["$78", "$80", "78", "80", "charged", "list price", "discount"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q8": {
        "text":     "Which service brings in the most revenue per appointment?",
        "category": "Revenue",
        "expect_numbers": True,
        "must_contain_one_of": ["massage", "swedish", "per appointment", "per visit", "highest"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q9": {
        "text":     "Which services are we discounting most heavily?",
        "category": "Revenue",
        "expect_numbers": True,
        "must_contain_one_of": ["hair color", "10%", "discount", "discounted"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q10": {
        "text":     "What's the revenue split across service categories last month?",
        "category": "Revenue",
        "expect_numbers": True,
        "must_contain_one_of": ["skincare", "massage", "hair", "nails", "category"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Margin & Economics ────────────────────────────────────────────────
    "Q11": {
        "text":     "After commission, which service is the most profitable?",
        "category": "Margin",
        "expect_numbers": True,
        "must_contain_one_of": ["facial", "margin", "profitable", "profit", "$"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q12": {
        "text":     "What's my average margin per service category?",
        "category": "Margin",
        "expect_numbers": True,
        "must_contain_one_of": ["margin", "category", "skincare", "massage", "nails", "$"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q13": {
        "text":     "Which services have the highest commission cost as a percentage of revenue?",
        "category": "Margin",
        "expect_numbers": True,
        "must_contain_one_of": ["hair color", "20%", "commission", "highest"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Trends Over Time ──────────────────────────────────────────────────
    "Q14": {
        "text":     "Is Facial Treatment trending up or down over the last 3 months?",
        "category": "Trends",
        "expect_numbers": False,
        "must_contain_one_of": ["up", "growing", "increasing", "trend", "growth", "%"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q15": {
        "text":     "Which service had the biggest jump in bookings last month?",
        "category": "Trends",
        "expect_numbers": True,
        "must_contain_one_of": ["express facial", "jump", "increase", "growth", "%", "biggest"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q16": {
        "text":     "Are any services declining that used to be popular?",
        "category": "Trends",
        "expect_numbers": False,
        "must_contain_one_of": ["no", "none", "growing", "all", "declining", "not declining"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "Q17": {
        "text":     "What were my top services last year vs now?",
        "category": "Trends",
        "expect_numbers": False,
        "must_contain_one_of": ["facial", "massage", "2025", "2026", "top", "compare"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Service × Client Behavior ─────────────────────────────────────────
    "Q18": {
        "text":     "Which services have the most repeat clients?",
        "category": "Clients",
        "expect_numbers": True,
        "must_contain_one_of": ["facial", "repeat", "returning", "clients"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q19": {
        "text":     "Which services are most commonly booked together?",
        "category": "Clients",
        "expect_numbers": True,
        "must_contain_one_of": ["manicure", "pedicure", "together", "combo", "pair"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q20": {
        "text":     "What's the typical first service a new client books?",
        "category": "Clients",
        "expect_numbers": False,
        "must_contain_one_of": ["facial", "first", "new client"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Service × Staff ───────────────────────────────────────────────────
    "Q21": {
        "text":     "Which staff member performs Facial Treatment the most?",
        "category": "Staff",
        "expect_numbers": True,
        "must_contain_one_of": ["maria", "lopez", "facial"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q22": {
        "text":     "Are there services that only one staff member can do?",
        "category": "Staff",
        "expect_numbers": False,
        "must_contain_one_of": ["hair color", "james", "only", "single", "one staff"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q23": {
        "text":     "Which services does Maria specialize in?",
        "category": "Staff",
        "expect_numbers": False,
        "must_contain_one_of": ["facial", "manicure", "maria"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Service × Location ────────────────────────────────────────────────
    "Q24": {
        "text":     "Which services are most popular at my Main St branch?",
        "category": "Location",
        "expect_numbers": True,
        "must_contain_one_of": ["facial", "main st", "popular", "booked"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q25": {
        "text":     "Are any services booked at one location but not the other?",
        "category": "Location",
        "expect_numbers": False,
        "must_contain_one_of": ["hair color", "main st", "only", "not offered", "one location"],
        "must_not_contain":    ["no data available", "don't have"],
    },
    "Q26": {
        "text":     "Where should I offer Facial Treatment more — which location underperforms?",
        "category": "Location",
        "expect_numbers": True,
        "must_contain_one_of": ["westside", "main st", "facial", "more"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Duration Reality ──────────────────────────────────────────────────
    "Q27": {
        "text":     "Which services consistently run longer than their scheduled time?",
        "category": "Duration",
        "expect_numbers": True,
        "must_contain_one_of": ["hair color", "longer", "over", "schedule", "minutes"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q28": {
        "text":     "What's the average actual duration for a Swedish Massage?",
        "category": "Duration",
        "expect_numbers": True,
        "must_contain_one_of": ["92", "93", "90", "minutes", "duration", "massage"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Lifecycle & Catalog Health ────────────────────────────────────────
    "Q29": {
        "text":     "Are there any services in my menu that haven't sold in 60 days?",
        "category": "Catalog",
        "expect_numbers": False,
        "must_contain_one_of": ["hot stone", "dormant", "no sales", "hasn't sold", "inactive"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q30": {
        "text":     "Which services did I add this year — how are they performing?",
        "category": "Catalog",
        "expect_numbers": True,
        "must_contain_one_of": ["express facial", "new", "added", "2026", "growing"],
        "must_not_contain":    ["no data", "don't have"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class QuestionResult:
    q_id:        str
    category:    str
    question:    str
    passed:      bool
    issues:      list[str] = field(default_factory=list)
    latency_ms:  float     = 0.0
    answer:      str       = ""


def score_answer(q_id: str, spec: dict, answer: str) -> QuestionResult:
    issues = []
    a_lower = answer.lower()

    # 1. Non-empty
    if not answer.strip() or len(answer.strip()) < 15:
        issues.append("empty_or_too_short")

    # 2. Contains numbers (if expected)
    if spec.get("expect_numbers") and not re.search(r"\d", answer):
        issues.append("missing_numbers")

    # 3. Must contain at least one keyword
    must_kw = spec.get("must_contain_one_of", [])
    if must_kw and not any(kw.lower() in a_lower for kw in must_kw):
        issues.append(f"missing_keyword(need one of: {must_kw})")

    # 4. Must NOT contain hallucination / refusal signals
    must_not = spec.get("must_not_contain", [])
    for bad in must_not:
        if bad.lower() in a_lower:
            issues.append(f"contains_forbidden({bad})")
            break

    return QuestionResult(
        q_id=q_id,
        category=spec.get("category", ""),
        question=spec["text"],
        passed=len(issues) == 0,
        issues=issues,
        answer=answer,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

async def ask_question(
    endpoint: str,
    business_id: str,
    question: str,
    timeout: float = REQUEST_TIMEOUT,
) -> tuple[str, float]:
    """Send a question to the chat endpoint and return (answer, latency_ms)."""
    payload = {
        "business_id": business_id,
        "org_id":      business_id,
        "session_id":  "step6-services-test",
        "question":    question,
    }
    t0 = time.perf_counter()
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(endpoint, json=payload)
        resp.raise_for_status()
        body = resp.json()
    latency = (time.perf_counter() - t0) * 1000

    # Extract answer — handle different response shapes
    answer = ""
    if isinstance(body, dict):
        answer = (
            body.get("answer")
            or body.get("response")
            or body.get("message")
            or body.get("content")
            or ""
        )
        # Some endpoints nest: {"data": {"answer": "..."}}
        if not answer and "data" in body and isinstance(body["data"], dict):
            answer = body["data"].get("answer", "")
    elif isinstance(body, str):
        answer = body

    return str(answer), latency


async def run_tests(
    endpoint: str = CHAT_ENDPOINT,
    business_id: str = BUSINESS_ID,
    question_id: str | None = None,
    output_path: str | None = None,
) -> list[QuestionResult]:
    """Run all (or one) test questions and return results."""

    questions = QUESTIONS
    if question_id:
        q_upper = question_id.upper()
        if q_upper not in questions:
            print(f"Unknown question ID: {q_upper}")
            print(f"Available: {', '.join(sorted(questions.keys()))}")
            sys.exit(1)
        questions = {q_upper: questions[q_upper]}

    print(f"\n{'═'*62}")
    print(f"  LEO AI BI — Step 6: Services Domain Test")
    print(f"  Questions   : {len(questions)}")
    print(f"  Endpoint    : {endpoint}")
    print(f"  business_id : {business_id}")
    print(f"{'═'*62}")

    results: list[QuestionResult] = []

    for q_id, spec in questions.items():
        print(f"  → [{q_id}] {spec['text']}")
        try:
            answer, latency = await ask_question(endpoint, business_id, spec["text"])
            result = score_answer(q_id, spec, answer)
            result.latency_ms = latency

            status = "✅" if result.passed else "❌"
            issues_str = ", ".join(result.issues) if result.issues else "—"
            print(
                f"  {status} [{q_id:<4}] {spec['category']:<14} "
                f"latency={latency:>6.0f}ms  issues: {issues_str}"
            )

            if not result.passed:
                preview = answer[:200].replace("\n", " ")
                print(f"         Answer: {preview}{'...' if len(answer) > 200 else ''}")

        except Exception as exc:
            result = QuestionResult(
                q_id=q_id,
                category=spec.get("category", ""),
                question=spec["text"],
                passed=False,
                issues=[f"request_error: {exc}"],
            )
            print(f"  ❌ [{q_id:<4}] ERROR: {exc}")

        results.append(result)

    # ── Summary ───────────────────────────────────────────────────────────
    total   = len(results)
    passed  = sum(1 for r in results if r.passed)
    failed  = total - passed
    avg_lat = sum(r.latency_ms for r in results) / total if total else 0

    # By category
    categories: dict[str, tuple[int, int]] = {}
    for r in results:
        p, t = categories.get(r.category, (0, 0))
        categories[r.category] = (p + (1 if r.passed else 0), t + 1)

    print(f"\n{'═'*62}")
    print(f"  Step 6 Results — Services Domain")
    print(f"{'═'*62}")
    print(f"  Overall     : {passed}/{total} passed ({passed/total*100:.0f}%)")
    print(f"  Avg latency : {avg_lat:.0f}ms")
    print(f"  By category :")
    for cat, (cp, ct) in sorted(categories.items()):
        bar = "█" * cp + "░" * (ct - cp)
        print(f"    {cat:<14} {bar}  {cp}/{ct}")

    if failed:
        print(f"\n  ── Failed ({failed}) ──────────────────────────────")
        for r in results:
            if not r.passed:
                print(f"    ❌ [{r.q_id}] {r.question}")
                print(f"         Issues: {', '.join(r.issues)}")

    print(f"{'═'*62}")
    if passed == total:
        print("  ✅ STEP 6 PASSED — Services domain ready for Step 7 sign-off")
    else:
        print(f"  ❌ STEP 6 INCOMPLETE — {failed} question(s) failing")
        print("     Fix gaps, then re-run before Step 7.")
    print(f"{'═'*62}\n")

    # ── JSON output ───────────────────────────────────────────────────────
    if output_path:
        import os
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        report = {
            "domain":         "services",
            "step":           6,
            "business_id":    business_id,
            "endpoint":       endpoint,
            "total":          total,
            "passed":         passed,
            "failed":         failed,
            "pass_rate_pct":  round(passed / total * 100, 1) if total else 0,
            "avg_latency_ms": round(avg_lat, 1),
            "by_category": {
                cat: {"passed": cp, "total": ct}
                for cat, (cp, ct) in categories.items()
            },
            "results": [
                {
                    "q_id":       r.q_id,
                    "category":   r.category,
                    "question":   r.question,
                    "passed":     r.passed,
                    "issues":     r.issues,
                    "latency_ms": round(r.latency_ms, 1),
                    "answer":     r.answer,
                }
                for r in results
            ],
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"  📄 Results saved to: {output_path}\n")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Step 6 — Services domain test harness"
    )
    parser.add_argument(
        "--question", type=str, default=None, metavar="QID",
        help="Run a single question by ID (e.g. Q1, Q19). Omit to run all 30.",
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
        run_tests(
            endpoint=args.endpoint,
            business_id=args.business_id,
            question_id=args.question,
            output_path=args.output,
        )
    )