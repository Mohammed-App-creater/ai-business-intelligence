"""
scripts/tests/test_staff_step6.py
==================================
Step 6 — Staff Performance Domain Test

Fires all 40 Step-1 questions through the live FastAPI chat endpoint
at http://localhost:8000/api/v1/chat and scores each answer against
4 criteria:

    ✅ Non-empty answer     — AI returned something
    ✅ Contains a number    — answer is data-driven, not generic advice
    ✅ Period keywords      — mentions expected content (staff name, metric, period)
    ✅ No refusal language  — AI didn't say "I don't have that data"

Fixture data used (business_id=42, salon_123):
    Top revenue staff:  Maria Lopez ($68.50/visit, 15% commission, 4.8 rating)
    #2 staff:           James Carter ($74.80/visit, 13% commission, 4.6 rating)
    Westside staff:     Aisha Nwosu ($72.20/visit, 14% commission, 4.7 rating)
    Inactive staff:     Tom Rivera (left after Jun 2025, 4.2 rating — lowest)
    Locations:          Main St (Maria + James), Westside (Aisha)
    Best period:        June 2025 (peak revenue)
    Worst period:       February 2025 (cancellation spike)

Usage:
    # Run all 40 questions:
    python scripts/tests/test_staff_step6.py

    # Run a single question by ID:
    python scripts/tests/test_staff_step6.py --question Q1
    python scripts/tests/test_staff_step6.py --question Q27

    # Save results to JSON:
    python scripts/tests/test_staff_step6.py --output results/step6_staff.json

    # Use a different business_id or endpoint:
    python scripts/tests/test_staff_step6.py --business-id 99
    python scripts/tests/test_staff_step6.py --endpoint http://localhost:8000/api/v1/chat

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
from dataclasses import dataclass, field
from typing import Optional

import httpx

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

CHAT_ENDPOINT   = "http://localhost:8000/api/v1/chat"
BUSINESS_ID     = "42"
REQUEST_TIMEOUT = 30.0

# ─────────────────────────────────────────────────────────────────────────────
# 40 Test questions — all from Step 1
# ─────────────────────────────────────────────────────────────────────────────

QUESTIONS: dict[str, dict] = {

    # ══ Category 1 — Basic Lookups ════════════════════════════════════════════
    # NOTE: Using actual fixture staff names (Maria Lopez, James Carter,
    # Aisha Nwosu, Tom Rivera). Fixture data covers Jan 2025 – Mar 2026.
    # "Last month" = March 2026 (current date April 2026).

    "Q1": {
        "text":           "How many appointments did Maria Lopez complete last month?",
        "category":       "Basic Lookups",
        "expect_numbers": True,
        "period_keywords": ["maria", "completed", "appointments", "month"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q2": {
        "text":           "How much revenue did James Carter generate in Q1?",
        "category":       "Basic Lookups",
        "expect_numbers": True,
        "period_keywords": ["james", "revenue", "quarter", "q1"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q3": {
        "text":           "What is Maria Lopez's average customer rating?",
        "category":       "Basic Lookups",
        "expect_numbers": True,
        "period_keywords": ["maria", "rating", "average"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q4": {
        "text":           "Show me all staff members and their total revenue last month.",
        "category":       "Basic Lookups",
        "expect_numbers": True,
        "period_keywords": ["staff", "revenue", "month", "maria", "james", "aisha"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q5": {
        "text":           "Show me Tom Rivera's performance history and when he was last active.",
        "category":       "Basic Lookups",
        "expect_numbers": True,
        "period_keywords": ["tom", "rivera", "revenue", "active"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },

    # ══ Category 2 — Rankings ═════════════════════════════════════════════════

    "Q6": {
        "text":           "Who is my top-performing staff member last month by revenue?",
        "category":       "Rankings",
        "expect_numbers": True,
        "period_keywords": ["maria", "revenue", "top", "month"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q7": {
        "text":           "Who completed the most appointments in the second half of 2025?",
        "category":       "Rankings",
        "expect_numbers": True,
        "period_keywords": ["maria", "appointments", "completed", "2025"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q8": {
        "text":           "Which staff member has the highest average customer rating?",
        "category":       "Rankings",
        "expect_numbers": True,
        "period_keywords": ["maria", "rating", "highest", "4.8"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q9": {
        "text":           "Rank all my staff by revenue generated this year.",
        "category":       "Rankings",
        "expect_numbers": True,
        "period_keywords": ["maria", "james", "aisha", "revenue", "rank"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q10": {
        "text":           "Who has the lowest rating on my team?",
        "category":       "Rankings",
        "expect_numbers": True,
        "period_keywords": ["tom", "rivera", "rating", "lowest", "4.2"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },

    # ══ Category 3 — Trends ═══════════════════════════════════════════════════

    "Q11": {
        "text":           "Has Maria Lopez's revenue been increasing or decreasing over the last 3 months?",
        "category":       "Trends",
        "expect_numbers": True,
        "period_keywords": ["maria", "revenue", "month", "increasing", "growing"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q12": {
        "text":           "Which staff member improved the most in bookings from February to March?",
        "category":       "Trends",
        "expect_numbers": True,
        "period_keywords": ["month", "improved", "bookings", "appointments"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q13": {
        "text":           "Show me James Carter's performance month by month for the past 6 months.",
        "category":       "Trends",
        "expect_numbers": True,
        "period_keywords": ["james", "month", "revenue", "performance"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q14": {
        "text":           "Did any staff member's revenue drop significantly last month compared to the month before?",
        "category":       "Trends",
        "expect_numbers": True,
        "period_keywords": ["revenue", "month", "drop", "decline"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },

    # ══ Category 4 — Location Breakdown ══════════════════════════════════════

    "Q15": {
        "text":           "Who is the top performer at my Main St location?",
        "category":       "Location",
        "expect_numbers": True,
        "period_keywords": ["main st", "revenue", "performer", "top"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q16": {
        "text":           "Show me revenue per location for the team last month.",
        "category":       "Location",
        "expect_numbers": True,
        "period_keywords": ["main st", "westside", "revenue"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q17": {
        "text":           "Show me all staff working at the Main St branch and their revenue last month.",
        "category":       "Location",
        "expect_numbers": True,
        "period_keywords": ["main st", "maria", "james", "revenue", "month"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q18": {
        "text":           "Which location has the strongest team overall?",
        "category":       "Location",
        "expect_numbers": True,
        "period_keywords": ["main st", "westside", "location", "revenue"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q19": {
        "text":           "Show me Aisha Nwosu's revenue per location last month.",
        "category":       "Location",
        "expect_numbers": True,
        "period_keywords": ["aisha", "westside", "revenue"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },

    # ══ Category 5 — Edge Cases ════════════════════════════════════════════════

    "Q20": {
        "text":           "What about a staff member who had zero visits last month — do they still show up?",
        "category":       "Edge Cases",
        "expect_numbers": False,
        "period_keywords": ["staff", "zero", "visits", "month"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q21": {
        "text":           "What if a staff member was deactivated mid-month — does their partial data still count?",
        "category":       "Edge Cases",
        "expect_numbers": False,
        "period_keywords": ["deactivated", "staff", "data", "active"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q22": {
        "text":           "Show me performance for staff who joined this year only.",
        "category":       "Edge Cases",
        "expect_numbers": True,
        "period_keywords": ["staff", "joined", "year", "revenue", "aisha"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q23": {
        "text":           "What happens if a visit has no assigned staff member — where does that revenue go?",
        "category":       "Edge Cases",
        "expect_numbers": False,
        "period_keywords": ["staff", "visit", "revenue", "assigned"],
        "must_not_contain": ["don't have", "unable to"],
    },
    "Q24": {
        "text":           "A staff member processed a visit but it was later refunded — does that revenue still count?",
        "category":       "Edge Cases",
        "expect_numbers": False,
        "period_keywords": ["refunded", "revenue", "visit", "staff"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },

    # ══ Category 6 — Vocabulary Variants ══════════════════════════════════════

    "Q25": {
        "text":           "Who are my best workers?",
        "category":       "Vocabulary",
        "expect_numbers": True,
        "period_keywords": ["maria", "revenue", "best", "performer"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q26": {
        "text":           "Which employee made the most money for us?",
        "category":       "Vocabulary",
        "expect_numbers": True,
        "period_keywords": ["maria", "revenue", "most"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q27": {
        "text":           "Who's been slacking lately?",
        "category":       "Vocabulary",
        "expect_numbers": True,
        "period_keywords": ["tom", "rivera", "decline", "revenue"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q28": {
        "text":           "Which stylist got the best reviews?",
        "category":       "Vocabulary",
        "expect_numbers": True,
        "period_keywords": ["maria", "rating", "reviews", "4.8"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q29": {
        "text":           "How's my team doing?",
        "category":       "Vocabulary",
        "expect_numbers": True,
        "period_keywords": ["staff", "revenue", "team", "maria"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q30": {
        "text":           "Who's my MVP last month?",
        "category":       "Vocabulary",
        "expect_numbers": True,
        "period_keywords": ["maria", "revenue", "top", "month"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q31": {
        "text":           "Give me the team's numbers.",
        "category":       "Vocabulary",
        "expect_numbers": True,
        "period_keywords": ["staff", "revenue", "team", "visits"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q32": {
        "text":           "Which technician handled the most clients?",
        "category":       "Vocabulary",
        "expect_numbers": True,
        "period_keywords": ["maria", "clients", "customers", "most"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q33": {
        "text":           "Who clocked the most hours?",
        "category":       "Vocabulary",
        "expect_numbers": True,
        "period_keywords": ["maria", "hours", "most"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },

    # ══ Category 7 — Commission & Pay ═════════════════════════════════════════

    "Q34": {
        "text":           "How much commission did each staff member earn last month?",
        "category":       "Commission",
        "expect_numbers": True,
        "period_keywords": ["commission", "staff", "last month", "month"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q35": {
        "text":           "Show me each staff member's revenue last month.",
        "category":       "Commission",
        "expect_numbers": True,
        "period_keywords": ["james", "maria", "aisha", "revenue"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q36": {
        "text":           "Which staff member earns the highest commission rate?",
        "category":       "Commission",
        "expect_numbers": True,
        "period_keywords": ["commission", "rate", "staff", "%"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q37": {
        "text":           "Show me commission earned per staff member last month.",
        "category":       "Commission",
        "expect_numbers": True,
        "period_keywords": ["commission", "staff", "month", "maria"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },

    # ══ Category 8 — Root Cause ════════════════════════════════════════════════

    "Q38": {
        "text":           "Why did revenue drop last month — was it a staffing issue?",
        "category":       "Root Cause",
        "expect_numbers": True,
        "period_keywords": ["revenue", "staff", "month", "drop"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q39": {
        "text":           "Is there a staff member causing a high number of cancellations?",
        "category":       "Root Cause",
        "expect_numbers": True,
        "period_keywords": ["staff", "cancellations", "cancelled", "cancelled"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q40": {
        "text":           "Which staff member has the most no-shows linked to them?",
        "category":       "Root Cause",
        "expect_numbers": True,
        "period_keywords": ["no-show", "no show", "staff", "tom", "rivera"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Result dataclass (identical to appointments version)
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
    payload = {
        "business_id": business_id,
        "org_id":      business_id,
        "question":    question,
    }
    t0 = time.perf_counter()
    try:
        resp = await client.post(endpoint, json=payload, timeout=REQUEST_TIMEOUT)
        latency_ms = (time.perf_counter() - t0) * 1000
        if resp.status_code != 200:
            return f"HTTP {resp.status_code}: {resp.text}", resp.status_code, latency_ms
        body = resp.json()
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


def score(
    q_id: str,
    spec: dict,
    answer: str,
    http_status: int,
    latency_ms: float,
) -> QuestionResult:
    issues = []
    answer_lower = answer.lower()

    # Check 1 — non-empty answer
    if not answer.strip() or http_status != 200:
        issues.append("empty_or_error")

    # Check 2 — contains a number (if expected)
    if spec.get("expect_numbers", True):
        if not NUMBER_RE.search(answer):
            issues.append("no_number")

    # Check 3 — period / domain keywords (any one match is sufficient)
    period_kws = spec.get("period_keywords", [])
    matched_kws = [kw for kw in period_kws if kw.lower() in answer_lower]
    if period_kws and not matched_kws:
        issues.append("missing_keyword")

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
    print(f"  LEO AI BI — Step 6: Staff Performance Domain Test")
    print(f"  Questions   : {len(questions_to_run)}")
    print(f"  Endpoint    : {endpoint}")
    print(f"  business_id : {business_id}")
    print(f"{'═'*62}\n")

    results: list[QuestionResult] = []

    async with httpx.AsyncClient() as client:
        for q_id, spec in questions_to_run.items():
            q_text   = spec["text"]
            category = spec["category"]
            print(
                f"  → [{q_id}] {q_text[:65]}"
                f"{'...' if len(q_text) > 65 else ''}"
            )

            answer, http_status, latency_ms = await ask(
                client, q_text, endpoint, business_id
            )

            result = score(q_id, spec, answer, http_status, latency_ms)
            results.append(result)

            status_icon = "✅" if result.passed else "❌"
            issues_str  = ", ".join(result.issues) if result.issues else "—"
            print(
                f"  {status_icon} [{q_id:<3}] {category:<18} "
                f"latency={latency_ms:>6.0f}ms  issues: {issues_str}"
            )

            if not result.passed:
                preview = answer[:200].replace("\n", " ")
                print(
                    f"         Answer: {preview}"
                    f"{'...' if len(answer) > 200 else ''}"
                )

    # ── Summary ───────────────────────────────────────────────────────────────
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
    print(f"  Step 6 Results — Staff Performance Domain")
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
        print("  ✅ STEP 6 PASSED — Staff domain ready for Step 7 sign-off")
    else:
        print(f"  ❌ STEP 6 INCOMPLETE — {failed} question(s) failing")
        print("     Fix gaps, then re-run before Step 7.")
    print(f"{'═'*62}\n")

    # ── JSON output ───────────────────────────────────────────────────────────
    if output_path:
        import os
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        report = {
            "domain":         "staff",
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
        description="Step 6 — Staff Performance domain test harness"
    )
    parser.add_argument(
        "--question",
        type=str,
        default=None,
        metavar="QID",
        help="Run a single question by ID (e.g. Q1, Q27). Omit to run all 40.",
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
        help="Save JSON results to this path (e.g. results/step6_staff.json)",
    )
    args = parser.parse_args()

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