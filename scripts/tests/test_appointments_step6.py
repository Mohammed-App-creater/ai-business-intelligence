"""
scripts/tests/test_appointments_step6.py
=========================================
Step 6 — Appointments Domain Test

Fires all 29 Step-1 questions through the live FastAPI chat endpoint
at http://localhost:8000/api/v1/chat and scores each answer against
4 criteria:

    ✅ Non-empty answer     — AI returned something
    ✅ Contains a number    — answer is data-driven, not generic advice
    ✅ Period keywords      — mentions expected time language
    ✅ No refusal language  — AI didn't say "I don't have that data"

Usage:
    # Run all 29 questions:
    python scripts/tests/test_appointments_step6.py

    # Run a single question by ID:
    python scripts/tests/test_appointments_step6.py --question Q1
    python scripts/tests/test_appointments_step6.py --question Q13

    # Save results to JSON:
    python scripts/tests/test_appointments_step6.py --output results/step6_appointments.json

    # Use a different business_id or endpoint:
    python scripts/tests/test_appointments_step6.py --business-id 99
    python scripts/tests/test_appointments_step6.py --endpoint http://localhost:8000/api/v1/chat

Payload sent to chat endpoint:
    { "business_id": "42", "question": "..." }
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import httpx

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

CHAT_ENDPOINT  = "http://localhost:8000/api/v1/chat"
BUSINESS_ID    = "42"
REQUEST_TIMEOUT = 30.0

# ─────────────────────────────────────────────────────────────────────────────
# 29 Test questions — all from Step 1
# ─────────────────────────────────────────────────────────────────────────────

QUESTIONS: dict[str, dict] = {

    # ── Volume & Counts ───────────────────────────────────────────────────────
    "Q1": {
        "text":     "How many appointments did we have this month?",
        "category": "Volume & Counts",
        "expect_numbers": True,
        "period_keywords": ["month", "this month"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q2": {
        "text":     "How many appointments were completed vs cancelled last month?",
        "category": "Volume & Counts",
        "expect_numbers": True,
        "period_keywords": ["completed", "cancelled", "canceled"],  # answer gives month name not "last month"
        "must_not_contain": ["don't have", "no data"],
    },
    "Q3": {
        "text":     "What was our total appointment count in Q1?",
        "category": "Volume & Counts",
        "expect_numbers": True,
        "period_keywords": ["q1", "quarter", "january", "march", "first quarter"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q4": {
        "text":     "How many appointments did we have this month?",
        "category": "Volume & Counts",
        "expect_numbers": True,
        "period_keywords": ["month", "this month"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },

    # ── Trends ────────────────────────────────────────────────────────────────
    "Q5": {
        "text":     "Are our bookings trending up or down compared to last month?",
        "category": "Trends",
        "expect_numbers": True,
        "period_keywords": ["last month", "previous month", "month"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q6": {
        "text":     "Which month had the most appointments this year?",
        "category": "Trends",
        "expect_numbers": True,
        "period_keywords": ["month", "year", "2025", "june", "march", "most"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q7": {
        "text":     "How has our cancellation rate changed over the last 3 months?",
        "category": "Trends",
        "expect_numbers": True,
        "period_keywords": ["month", "cancellation", "cancel", "rate", "%"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q8": {
        "text":     "Which month had the highest cancellation rate?",
        "category": "Trends",
        "expect_numbers": True,
        "period_keywords": ["month", "cancellation", "cancel", "rate", "%", "february", "highest"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q9": {
        "text":     "How has the average service duration changed over the past 6 months?",
        "category": "Trends",
        "expect_numbers": True,
        "period_keywords": ["month", "duration", "minutes", "min", "average"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q10": {
        "text":     "Which staff members have seen a decline in appointments over time?",
        "category": "Trends",
        "expect_numbers": False,   # May answer with names only
        "period_keywords": ["decline", "declining", "drop", "decrease", "month"],
        "must_not_contain": ["don't have", "no data"],
    },

    # ── Time Slot Distribution ────────────────────────────────────────────────
    "Q11": {
        "text":     "How are appointments distributed across morning, afternoon, and evening?",
        "category": "Time Slots",
        "expect_numbers": True,
        "period_keywords": ["morning", "afternoon", "evening"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q12": {
        "text":     "Which locations have the most appointments on weekends vs weekdays?",
        "category": "Time Slots",
        "expect_numbers": True,
        "period_keywords": ["weekend", "weekday", "location", "branch"],
        "must_not_contain": ["don't have", "no data"],
    },

    # ── Staff Breakdown ───────────────────────────────────────────────────────
    "Q13": {
        "text":     "Which staff member had the most appointments last month?",
        "category": "Staff",
        "expect_numbers": True,
        "period_keywords": ["last month", "month"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q14": {
        "text":     "How many appointments did Maria Lopez complete this month?",
        "category": "Staff",
        "expect_numbers": True,
        "period_keywords": ["maria", "lopez", "month", "complete"],
        "must_not_contain": ["don't have", "no data", "no staff"],
    },
    "Q15": {
        "text":     "Which employee has the highest no-show rate?",
        "category": "Staff",
        "expect_numbers": True,
        "period_keywords": ["no-show", "no show", "rate", "%"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q16": {
        "text":     "How many appointments did each staff member handle per service type?",
        "category": "Staff",
        "expect_numbers": True,
        "period_keywords": ["staff", "service", "appointment", "facial", "massage", "manicure"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q17": {
        "text":     "Which staff members have seen their appointment count decline over time?",
        "category": "Staff",
        "expect_numbers": False,
        "period_keywords": ["decline", "declining", "drop", "decrease", "tom", "rivera"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q18": {
        "text":     "Which location generates the most completed appointments per staff member?",
        "category": "Staff",
        "expect_numbers": True,
        "period_keywords": ["location", "branch", "main st", "westside", "staff", "completed"],
        "must_not_contain": ["don't have", "no data"],
    },

    # ── Service Breakdown ─────────────────────────────────────────────────────
    "Q19": {
        "text":     "Which service is booked the most?",
        "category": "Services",
        "expect_numbers": True,
        "period_keywords": ["facial", "massage", "manicure", "service", "most", "booked"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q20": {
        "text":     "How many appointments were booked for Facial Treatment last month?",
        "category": "Services",
        "expect_numbers": True,
        "period_keywords": ["facial", "treatment", "last month", "month"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q21": {
        "text":     "What is the average service duration for each service type?",
        "category": "Services",
        "expect_numbers": True,
        "period_keywords": ["duration", "minutes", "min", "average", "facial", "massage"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q22": {
        "text":     "Which services have the highest booking frequency this year?",
        "category": "Services",
        "expect_numbers": True,
        "period_keywords": ["service", "booking", "facial", "massage", "manicure"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q23": {
        "text":     "Which services attract the most repeat clients?",
        "category": "Services",
        "expect_numbers": True,
        "period_keywords": ["repeat", "client", "service", "facial", "massage"],
        "must_not_contain": ["don't have", "no data"],
    },

    # ── Cancellation Analysis ─────────────────────────────────────────────────
    "Q24": {
        "text":     "What is our cancellation rate this month?",
        "category": "Cancellations",
        "expect_numbers": True,
        "period_keywords": ["cancellation", "cancel", "rate", "%", "month"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q25": {
        "text":     "Are no-shows getting worse over time?",
        "category": "Cancellations",
        "expect_numbers": True,
        "period_keywords": ["no-show", "no show", "rate", "%", "month"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q26": {
        "text":     "Are there patterns in cancellations for specific services or employees?",
        "category": "Cancellations",
        "expect_numbers": True,
        "period_keywords": ["cancellation", "cancel", "service", "staff", "hair", "color"],
        "must_not_contain": ["don't have", "no data"],
    },

    # ── Location Breakdown ────────────────────────────────────────────────────
    "Q27": {
        "text":     "Which branch had the most appointments last month?",
        "category": "Location",
        "expect_numbers": True,
        "period_keywords": ["last month", "month", "main st", "westside", "branch", "location"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q28": {
        "text":     "Which location has the highest cancellation rate this month?",
        "category": "Location",
        "expect_numbers": True,
        "period_keywords": ["cancellation", "cancel", "rate", "%", "location", "branch", "westside", "main st"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q29": {
        "text":     "How does each location's appointment volume compare to last month?",
        "category": "Location",
        "expect_numbers": True,
        "period_keywords": ["last month", "month", "main st", "westside", "location"],
        "must_not_contain": ["don't have", "no data"],
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class QuestionResult:
    q_id:           str
    category:       str
    question:       str
    answer:         str
    passed:         bool
    latency_ms:     float
    issues:         list[str] = field(default_factory=list)
    http_status:    int = 200


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helper
# ─────────────────────────────────────────────────────────────────────────────

async def ask(
    client: httpx.AsyncClient,
    question: str,
    endpoint: str,
    business_id: str,
) -> tuple[str, int, float]:
    """
    Send one question to the chat endpoint.
    Returns (answer_text, http_status, latency_ms).
    """
    payload = {
        "business_id": business_id,
        "org_id":      business_id,   # endpoint requires both fields
        "question":    question,
    }
    t0 = time.perf_counter()
    try:
        resp = await client.post(endpoint, json=payload, timeout=REQUEST_TIMEOUT)
        latency_ms = (time.perf_counter() - t0) * 1000
        if resp.status_code != 200:
            return f"HTTP {resp.status_code}: {resp.text}", resp.status_code, latency_ms
        body = resp.json()
        # Support both "answer" and "response" field names
        answer = body.get("answer") or body.get("response") or json.dumps(body)
        return answer, resp.status_code, latency_ms
    except httpx.TimeoutException:
        latency_ms = (time.perf_counter() - t0) * 1000
        return "TIMEOUT", 408, latency_ms
    except Exception as e:
        latency_ms = (time.perf_counter() - t0) * 1000
        return f"ERROR: {e}", 500, latency_ms


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

NUMBER_RE = re.compile(r"\b\d[\d,\.]*%?\b")

REFUSAL_PHRASES = [
    "i don't have", "i do not have", "no data available",
    "unable to", "cannot provide", "don't have access",
    "no information", "not available", "i'm sorry",
    "i apologize", "cannot answer",
]

def score(q_id: str, spec: dict, answer: str, http_status: int, latency_ms: float) -> QuestionResult:
    issues = []
    answer_lower = answer.lower()

    # Check 1 — non-empty answer
    if not answer.strip() or http_status != 200:
        issues.append("empty_or_error")

    # Check 2 — contains a number (if expected)
    if spec.get("expect_numbers", True):
        if not NUMBER_RE.search(answer):
            issues.append("no_number")

    # Check 3 — period / domain keywords
    period_kws = spec.get("period_keywords", [])
    matched_kws = [kw for kw in period_kws if kw.lower() in answer_lower]
    if period_kws and not matched_kws:
        issues.append("missing_period_keyword")

    # Check 4 — no refusal language
    refusals_found = [p for p in REFUSAL_PHRASES if p in answer_lower]
    if refusals_found:
        issues.append(f"refusal({refusals_found[0]!r})")

    # Check 5 — must_not_contain
    for phrase in spec.get("must_not_contain", []):
        if phrase.lower() in answer_lower:
            issues.append(f"contains_forbidden({phrase!r})")

    passed = len(issues) == 0

    return QuestionResult(
        q_id=q_id,
        category=spec["category"],
        question=spec["text"],
        answer=answer,
        passed=passed,
        latency_ms=latency_ms,
        issues=issues,
        http_status=http_status,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main runner
# ─────────────────────────────────────────────────────────────────────────────

async def run(
    q_filter: Optional[str],
    endpoint: str,
    business_id: str,
    output_path: Optional[str],
) -> list[QuestionResult]:

    questions_to_run = {
        qid: spec for qid, spec in QUESTIONS.items()
        if q_filter is None or qid == q_filter
    }

    print(f"\n{'═'*62}")
    print(f"  LEO AI BI — Step 6: Appointments Domain Test")
    print(f"  Questions   : {len(questions_to_run)}")
    print(f"  Endpoint    : {endpoint}")
    print(f"  business_id : {business_id}")
    print(f"{'═'*62}\n")

    results: list[QuestionResult] = []

    async with httpx.AsyncClient() as client:
        for q_id, spec in questions_to_run.items():
            q_text = spec["text"]
            category = spec["category"]
            print(f"  → [{q_id}] {q_text[:65]}{'...' if len(q_text) > 65 else ''}")

            answer, http_status, latency_ms = await ask(
                client, q_text, endpoint, business_id
            )

            result = score(q_id, spec, answer, http_status, latency_ms)
            results.append(result)

            status_icon = "✅" if result.passed else "❌"
            issues_str  = ", ".join(result.issues) if result.issues else "—"
            print(
                f"  {status_icon} [{q_id:<3}] {category:<20} "
                f"latency={latency_ms:>6.0f}ms  issues: {issues_str}"
            )

            if not result.passed:
                # Show trimmed answer for failures
                preview = answer[:200].replace("\n", " ")
                print(f"         Answer: {preview}{'...' if len(answer) > 200 else ''}")

    # ── Summary ───────────────────────────────────────────────────────────────
    total   = len(results)
    passed  = sum(1 for r in results if r.passed)
    failed  = total - passed
    avg_lat = sum(r.latency_ms for r in results) / total if total else 0

    # By category
    categories: dict[str, tuple[int,int]] = {}
    for r in results:
        p, t = categories.get(r.category, (0, 0))
        categories[r.category] = (p + (1 if r.passed else 0), t + 1)

    print(f"\n{'═'*62}")
    print(f"  Step 6 Results — Appointments Domain")
    print(f"{'═'*62}")
    print(f"  Overall     : {passed}/{total} passed ({passed/total*100:.0f}%)")
    print(f"  Avg latency : {avg_lat:.0f}ms")
    print(f"  By category :")
    for cat, (cp, ct) in sorted(categories.items()):
        bar = "█" * cp + "░" * (ct - cp)
        print(f"    {cat:<22} {bar}  {cp}/{ct}")

    if failed:
        print(f"\n  ── Failed ({failed}) ──────────────────────────────")
        for r in results:
            if not r.passed:
                print(f"    ❌ [{r.q_id}] {r.question}")
                print(f"         Issues: {', '.join(r.issues)}")

    print(f"{'═'*62}")
    if passed == total:
        print("  ✅ STEP 6 PASSED — Appointments domain ready for Step 7 sign-off")
    else:
        print(f"  ❌ STEP 6 INCOMPLETE — {failed} question(s) failing")
        print("     Fix gaps, then re-run to confirm all pass before Step 7.")
    print(f"{'═'*62}\n")

    # ── JSON output ───────────────────────────────────────────────────────────
    if output_path:
        import os
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        report = {
            "domain":       "appointments",
            "step":         6,
            "business_id":  business_id,
            "endpoint":     endpoint,
            "total":        total,
            "passed":       passed,
            "failed":       failed,
            "pass_rate_pct": round(passed / total * 100, 1) if total else 0,
            "avg_latency_ms": round(avg_lat, 1),
            "by_category":  {
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
        description="Step 6 — Appointments domain test harness"
    )
    parser.add_argument(
        "--question",
        type=str,
        default=None,
        metavar="QID",
        help="Run a single question by ID (e.g. Q1, Q13). Omit to run all 29.",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default=CHAT_ENDPOINT,
        help=f"Chat endpoint URL (default: {CHAT_ENDPOINT})",
    )
    parser.add_argument(
        "--business-id",
        type=str,
        default=BUSINESS_ID,
        dest="business_id",
        help=f"Business ID string to send (default: {BUSINESS_ID})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        metavar="PATH",
        help="Save JSON results to this path (e.g. results/step6_appointments.json)",
    )
    args = parser.parse_args()

    # Validate question ID if provided
    if args.question and args.question not in QUESTIONS:
        print(f"❌ Unknown question ID: {args.question}")
        print(f"   Valid IDs: {', '.join(QUESTIONS.keys())}")
        sys.exit(1)

    asyncio.run(run(
        q_filter=args.question,
        endpoint=args.endpoint,
        business_id=args.business_id,
        output_path=args.output,
    ))