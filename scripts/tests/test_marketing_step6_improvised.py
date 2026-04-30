"""
scripts/tests/test_marketing_step6_improvised.py
==================================================
Step 6 / Step 8 sign-off — Marketing improvised question test.

Purpose: stress-test the chat pipeline with 8 questions that are NOT
near-rephrases of the 34 prepared acceptance questions. Each probes a
different angle the prepared set may not have fully tested.

These questions catch issues that the prepared set can miss:
  - Casual/colloquial phrasing (vs the formal Step-1 wording)
  - Inverse questions (worst, lowest — opposite of "best", "highest")
  - Compound multi-part asks
  - Entity name references ("Express Facial Launch" by name)
  - Cross-domain probes (marketing → revenue overlap)
  - Counterfactual / what-if framing
  - Genuine data gaps (the system should say "no data" honestly, not hallucinate)
  - Ambiguous period references

Usage:
    python scripts/tests/test_marketing_step6_improvised.py
    python scripts/tests/test_marketing_step6_improvised.py --question I3
    python scripts/tests/test_marketing_step6_improvised.py \\
        --output results/step6_marketing_improvised.json

Sign-off target: 7/8 pass minimum (one unexpected miss is acceptable —
improvised questions probe stricter angles than the prepared set).
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
REQUEST_TIMEOUT = 45.0


# ─────────────────────────────────────────────────────────────────────────────
# 8 improvised questions — one per "challenge angle"
# ─────────────────────────────────────────────────────────────────────────────

QUESTIONS: dict[str, dict] = {

    # ── I1: Casual/colloquial phrasing ─────────────────────────────────────
    # Probes whether the retriever handles natural slang vs the formal
    # "What was the open rate" style of the prepared questions.
    "I1": {
        "text":     "How are my emails doing lately?",
        "category": "Colloquial",
        "rationale": "Natural slang. Should map to email performance / trends.",
        "expect_numbers": True,
        "must_contain_one_of": [
            "email", "open", "click", "delivered", "%", "trend",
        ],
        "must_not_contain": ["no data", "don't have", "insufficient"],
    },

    # ── I2: Inverse / negative ────────────────────────────────────────────
    # "Worst" vs "best" — does the pipeline retrieve the same chunks
    # and surface the bottom of the ranking?
    "I2": {
        "text":     "Which of my campaigns this year had the WORST open rate?",
        "category": "Inverse",
        "rationale": (
            "Tests bottom-of-ranking. From fixtures: Holiday Promo had 22% open "
            "(Dec) — worst email open of 2025/2026. Or in 2026 alone: "
            "the lowest is the Re-engagement / Welcome Series Feb-2025 era at ~20-25%."
        ),
        "expect_numbers": True,
        "must_contain_one_of": [
            "holiday", "lowest", "worst", "open rate", "%",
            "re-engagement", "weakest",
        ],
        "must_not_contain": ["no data", "don't have", "insufficient"],
    },

    # ── I3: Compound multi-part question ───────────────────────────────────
    # Two distinct asks in one — does the prompt handle both halves?
    "I3": {
        "text":     "How many emails went out last month, and what percentage of recipients clicked?",
        "category": "Compound",
        "rationale": (
            "Mar 2026: 2418 emails, click rate ~4.34%. Both halves should "
            "be addressed in the answer — not just one."
        ),
        "expect_numbers": True,
        "must_contain_one_of": ["2418", "2,418", "email", "click"],
        # Must reference BOTH halves of the question — at least one click-related
        # phrase to prove the second half wasn't ignored
        "must_contain_secondary": ["click", "%", "4.3", "4.34"],
        "must_not_contain": ["no data", "don't have"],
    },

    # ── I4: Specific named campaign ────────────────────────────────────────
    # User mentions a campaign by name. Does retrieval find the spotlight chunk?
    "I4": {
        "text":     "Tell me how the Express Facial Launch campaign did.",
        "category": "Named Entity",
        "rationale": (
            "Express Facial Launch ran Feb 2026 with 49.19% open rate "
            "(top of 2026). campaign_spotlight chunk should match this query."
        ),
        "expect_numbers": True,
        "must_contain_one_of": [
            "express facial", "49", "open rate", "facial",
        ],
        "must_not_contain": ["no data", "don't have"],
    },

    # ── I5: Cross-domain probe ─────────────────────────────────────────────
    # Marketing question that shades into Revenue. Tests multi-domain retrieval.
    "I5": {
        "text":     "Are my marketing campaigns actually driving revenue, or just sending emails into the void?",
        "category": "Cross-Domain",
        "rationale": (
            "Should pull both marketing performance AND ROI/attribution. "
            "Tests that we don't get one-sided answer (only sends OR only revenue)."
        ),
        "expect_numbers": True,
        "must_contain_one_of": [
            "revenue", "redemption", "promo", "$", "campaign",
        ],
        "must_not_contain": ["no data", "don't have"],
    },

    # ── I6: Counterfactual / what-if ───────────────────────────────────────
    # "What would have happened if..." — system should ground reasoning
    # in actual data, not hallucinate alternative-history numbers.
    "I6": {
        "text":     "If I had skipped the SUMMER20 promotion, how much money would I have saved on discounts?",
        "category": "Counterfactual",
        "rationale": (
            "SUMMER20 H1 2025: $480 discount (MS) + $360 discount (WS) = ~$840 "
            "discount given. The savings figure should reference this."
        ),
        "expect_numbers": True,
        "must_contain_one_of": [
            "summer20", "summer", "discount", "$", "saved", "given",
        ],
        "must_not_contain": ["no data", "don't have"],
    },

    # ── I7: Genuine data gap (honesty check) ───────────────────────────────
    # The system DOES NOT track this — should honestly say "no data" or
    # redirect to what IS tracked. NOT make up numbers.
    "I7": {
        "text":     "What time of day do my campaigns get the most opens?",
        "category": "Data Gap",
        "rationale": (
            "We track day-of-week (Q33) but NOT hour-of-day. Fixtures don't "
            "have execution timestamps. The honest answer is to redirect to "
            "day-of-week, OR say we only track at the day level, NOT to "
            "fabricate hour-of-day numbers."
        ),
        "expect_numbers": False,    # No specific number required
        # Acceptable answers: redirect to day-of-week, OR honestly admit gap
        "must_contain_one_of": [
            "day of the week", "day-of-week", "day of week",
            "don't track", "do not track", "not tracked",
            "no time", "hourly", "hour",
            "data does not", "data doesn't", "isn't available",
            "only have", "we track",
        ],
        # CRITICAL: must NOT hallucinate a fake time-of-day answer
        "must_not_contain": [
            "9am", "9 am", "morning", "afternoon hours",
            "between 8 and 10",
        ],
    },

    # ── I8: Ambiguous period ────────────────────────────────────────────────
    # "Recently" — no specific period. Should pick a sensible recent window
    # and answer with grounded numbers, not punt.
    "I8": {
        "text":     "Has anything stood out about my marketing recently?",
        "category": "Ambiguous Period",
        "rationale": (
            "Open-ended, no period. Should surface a notable signal from "
            "recent months — top performer, surprising click rate, list "
            "growth, etc. — anchored in actual fixture data."
        ),
        "expect_numbers": True,
        "must_contain_one_of": [
            "express facial", "march madness", "welcome",
            "open rate", "click rate", "trend",
            "%",  "campaign", "best",
        ],
        "must_not_contain": ["no data", "don't have", "insufficient"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Scoring — same shape as test_marketing_step6.py for consistency
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class QuestionResult:
    q_id:        str
    category:    str
    rationale:   str
    question:    str
    passed:      bool
    issues:      list[str] = field(default_factory=list)
    latency_ms:  float     = 0.0
    answer:      str       = ""
    route:       str       = ""


def score_answer(
    q_id: str,
    spec: dict,
    answer: str,
    route: str,
) -> QuestionResult:
    issues = []
    a_lower = answer.lower()

    # 1. Non-empty
    if not answer.strip() or len(answer.strip()) < 15:
        issues.append("empty_or_too_short")

    # 2. Contains numbers (if expected)
    if spec.get("expect_numbers") and not re.search(r"\d", answer):
        issues.append("missing_numbers")

    # 3. Must contain at least one primary keyword
    must_kw = spec.get("must_contain_one_of", [])
    if must_kw and not any(kw.lower() in a_lower for kw in must_kw):
        issues.append(f"missing_keyword(need one of: {must_kw[:5]}...)")

    # 4. Compound check — for multi-part questions, both halves must show
    must_secondary = spec.get("must_contain_secondary", [])
    if must_secondary and not any(kw.lower() in a_lower for kw in must_secondary):
        issues.append(
            f"missing_second_half(answer addressed only one half — "
            f"need one of: {must_secondary})"
        )

    # 5. Must NOT contain hallucination signals
    must_not = spec.get("must_not_contain", [])
    for bad in must_not:
        if bad.lower() in a_lower:
            issues.append(f"contains_forbidden({bad})")
            break

    return QuestionResult(
        q_id=q_id,
        category=spec.get("category", ""),
        rationale=spec.get("rationale", ""),
        question=spec["text"],
        passed=len(issues) == 0,
        issues=issues,
        answer=answer,
        route=route,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

async def ask_question(
    endpoint: str,
    business_id: str,
    question: str,
    timeout: float = REQUEST_TIMEOUT,
) -> tuple[str, str, float]:
    """Send a question to the chat endpoint and return (answer, route, latency_ms)."""
    payload = {
        "business_id": business_id,
        "org_id":      business_id,
        "session_id":  "step6-marketing-improvised",
        "question":    question,
    }
    t0 = time.perf_counter()
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(endpoint, json=payload)
        resp.raise_for_status()
        body = resp.json()
    latency = (time.perf_counter() - t0) * 1000

    answer = ""
    route  = ""
    if isinstance(body, dict):
        answer = (
            body.get("answer")
            or body.get("response")
            or body.get("message")
            or body.get("content")
            or ""
        )
        route = body.get("route", "")
    elif isinstance(body, str):
        answer = body

    return str(answer), str(route), latency


async def run_tests(
    endpoint: str = CHAT_ENDPOINT,
    business_id: str = BUSINESS_ID,
    question_id: str | None = None,
    output_path: str | None = None,
) -> list[QuestionResult]:
    """Run all (or one) improvised test questions."""

    questions = QUESTIONS
    if question_id:
        q_upper = question_id.upper()
        if q_upper not in questions:
            print(f"Unknown question ID: {q_upper}")
            print(f"Available: {', '.join(sorted(questions.keys()))}")
            sys.exit(1)
        questions = {q_upper: questions[q_upper]}

    print(f"\n{'═'*68}")
    print(f"  LEO AI BI — Step 6 Improvised: Marketing Stress Test")
    print(f"  Questions   : {len(questions)} (off-script, probe stricter angles)")
    print(f"  Endpoint    : {endpoint}")
    print(f"  business_id : {business_id}")
    print(f"  Sign-off bar: 7/8 pass minimum")
    print(f"{'═'*68}")

    results: list[QuestionResult] = []

    for q_id, spec in questions.items():
        print(f"\n  → [{q_id}] [{spec['category']}] {spec['text']}")
        print(f"         Tests: {spec['rationale'][:80]}{'...' if len(spec['rationale']) > 80 else ''}")
        try:
            answer, route, latency = await ask_question(
                endpoint, business_id, spec["text"]
            )

            # ERROR-route retry — same as the main test harness
            if route == "ERROR":
                print(f"         ⟳ route=ERROR, retrying once after 2s backoff...")
                await asyncio.sleep(2.0)
                answer, route, latency_retry = await ask_question(
                    endpoint, business_id, spec["text"]
                )
                latency += latency_retry

            result = score_answer(q_id, spec, answer, route)
            result.latency_ms = latency

            status = "✅" if result.passed else "❌"
            issues_str = ", ".join(result.issues) if result.issues else "—"
            route_str = f"[{route}]" if route else ""
            print(
                f"  {status} [{q_id:<3}] {spec['category']:<18} "
                f"{route_str:<10} latency={latency:>6.0f}ms"
            )
            if result.issues:
                print(f"         issues: {issues_str}")

            preview = answer[:240].replace("\n", " ")
            print(f"         Answer: {preview}{'...' if len(answer) > 240 else ''}")

        except Exception as exc:
            result = QuestionResult(
                q_id=q_id,
                category=spec.get("category", ""),
                rationale=spec.get("rationale", ""),
                question=spec["text"],
                passed=False,
                issues=[f"request_error: {exc}"],
            )
            print(f"  ❌ [{q_id:<3}] ERROR: {exc}")

        results.append(result)

    # ── Summary ───────────────────────────────────────────────────────────
    total   = len(results)
    passed  = sum(1 for r in results if r.passed)
    failed  = total - passed
    avg_lat = sum(r.latency_ms for r in results) / total if total else 0

    print(f"\n{'═'*68}")
    print(f"  Step 6 Improvised Results — Marketing Domain")
    print(f"{'═'*68}")
    print(f"  Overall     : {passed}/{total} passed ({passed/total*100:.0f}%)")
    print(f"  Avg latency : {avg_lat:.0f}ms")
    print(f"  By angle    :")
    for r in results:
        icon = "✅" if r.passed else "❌"
        print(f"    {icon} {r.q_id} [{r.category}]")

    if failed:
        print(f"\n  ── Failed ({failed}) ──────────────────────────────")
        for r in results:
            if not r.passed:
                print(f"    ❌ [{r.q_id}] {r.question}")
                print(f"         Probes: {r.rationale[:100]}")
                print(f"         Issues: {', '.join(r.issues)}")

    print(f"{'═'*68}")
    if passed >= 7:
        print(f"  ✅ STEP 6 IMPROVISED PASSED — {passed}/8 (sign-off bar = 7)")
        print(f"     Marketing domain ready for Step 8 sign-off.")
    elif passed >= 5:
        print(f"  ⚠️  STEP 6 IMPROVISED MARGINAL — {passed}/8")
        print(f"     Below 7/8 bar. Review failures and decide whether to refine or accept.")
    else:
        print(f"  ❌ STEP 6 IMPROVISED INCOMPLETE — {passed}/8")
        print(f"     Significant gaps. Refine before sign-off.")
    print(f"{'═'*68}\n")

    # ── JSON output ───────────────────────────────────────────────────────
    if output_path:
        import os
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        report = {
            "domain":         "marketing",
            "step":           "6-improvised",
            "business_id":    business_id,
            "endpoint":       endpoint,
            "total":          total,
            "passed":         passed,
            "failed":         failed,
            "pass_rate_pct":  round(passed / total * 100, 1) if total else 0,
            "avg_latency_ms": round(avg_lat, 1),
            "signoff_bar":    7,
            "results": [
                {
                    "q_id":       r.q_id,
                    "category":   r.category,
                    "rationale":  r.rationale,
                    "question":   r.question,
                    "passed":     r.passed,
                    "issues":     r.issues,
                    "latency_ms": round(r.latency_ms, 1),
                    "route":      r.route,
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
        description="Step 6 improvised — Marketing stress test (8 off-script questions)"
    )
    parser.add_argument(
        "--question", type=str, default=None, metavar="QID",
        help="Run a single question by ID (e.g. I1, I7). Omit to run all 8.",
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