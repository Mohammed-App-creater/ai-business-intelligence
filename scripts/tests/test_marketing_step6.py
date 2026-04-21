"""
scripts/tests/test_marketing_step6.py
======================================
Step 6 — Marketing Domain Test Harness

Runs all 34 Step-1 acceptance questions against the live chat endpoint,
scores each answer on: non-empty, contains numbers, keyword match,
no hallucination signals, correct route, optional isolation-mode checks.

Mirrors the structure of test_clients_step6.py (ERROR retry, isolation
scoring, dataclass-based results) with Marketing-specific additions:

  1. `must_not_contain_names` — false here. Marketing fixtures don't
     carry individual customer names (we work with campaign names,
     promo codes, audience counts). No PII leak class is expected.
  2. Isolation-mode leak signatures anchored to the Marketing fixture:
     promo codes (WELCOME10, SUMMER20, HOLIDAY15), unique campaign names
     (Express Facial Launch, March Madness, Appointment Reminder), the
     distinctive 49.19% open rate, and the 38/27 WELCOME10 redemption
     numbers.

Usage:
    python scripts/tests/test_marketing_step6.py
    python scripts/tests/test_marketing_step6.py --question Q5
    python scripts/tests/test_marketing_step6.py --output results/step6_marketing.json

    # Tenant isolation — biz 99 must not see biz 42 data:
    python scripts/tests/test_marketing_step6.py \\
        --business-id 99 --isolation-mode \\
        --output results/step6_marketing_iso.json
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
# 34 acceptance questions — locked in Step 1
# Expected values ARE grounded in the marketing_fixtures.py data for biz 42.
# ─────────────────────────────────────────────────────────────────────────────

QUESTIONS: dict[str, dict] = {

    # ── Category 1: Basic Facts ────────────────────────────────────────────
    "Q1": {
        "text":     "How many marketing campaigns did I run last month?",
        "category": "Basic Facts",
        "expect_numbers": True,
        # Mar 2026 fixture: 4 unique campaigns executed (5 executions total).
        # Also covers variants where the answer uses 5 (executions) or 3/1
        # (email-vs-SMS split).
        "must_contain_one_of": ["4", "5", "campaign"],
        "must_not_contain":    ["no data", "don't have", "insufficient"],
    },
    "Q2": {
        "text":     "How many emails did I send last month?",
        "category": "Basic Facts",
        "expect_numbers": True,
        # Mar 2026 emails_sent = 2418
        "must_contain_one_of": ["2418", "2,418", "emails", "email"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q3": {
        "text":     "How many SMS messages did I send last month?",
        "category": "Basic Facts",
        "expect_numbers": True,
        # Mar 2026 sms_sent = 525
        "must_contain_one_of": ["525", "sms", "text"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q4": {
        "text":     "How many active campaigns did I have last month?",
        "category": "Basic Facts",
        "expect_numbers": True,
        # 4 active campaigns executed in Mar 2026. Excludes CID 508 (expired).
        "must_contain_one_of": ["4", "active", "campaign"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Category 2: Performance KPIs ───────────────────────────────────────
    "Q5": {
        "text":     "What was the open rate on my last campaign?",
        "category": "Performance",
        "expect_numbers": True,
        # Last = Appointment Reminder (SMS, no open rate) OR latest promo =
        # March Madness 34.17%. Either answer is valid — but the honest one
        # should note SMS has no open tracking if it picks the SMS.
        "must_contain_one_of": [
            "34", "open rate", "march madness", "appointment reminder",
            "no open", "not tracked", "sms",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q6": {
        "text":     "What was the click rate on my last campaign?",
        "category": "Performance",
        "expect_numbers": True,
        # Most recent = Appointment Reminder SMS (click 8.88) OR
        # March Madness (click 4.67). Either is acceptable.
        "must_contain_one_of": ["4.67", "8.88", "click", "%"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q7": {
        "text":     "What was the delivery success rate on my campaigns last month?",
        "category": "Performance",
        "expect_numbers": True,
        # Mar 2026: sent ≈ 2943, delivered ≈ 2865, delivery rate ~97%+
        "must_contain_one_of": ["97", "98", "delivery", "delivered", "%"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q8": {
        "text":     "How many emails failed to deliver last month, and which campaign had the worst failure rate?",
        "category": "Performance",
        "expect_numbers": True,
        # Mar 2026 failed total ~78. Worst failure rate campaign:
        # Holiday Promo in Dec (20%) — but last month is March.
        # March's worst: March Madness had 45 failed of ~1950 sent.
        "must_contain_one_of": ["45", "march madness", "failed", "failure"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q9": {
        "text":     "How many customers did my last campaign reach?",
        "category": "Performance",
        "expect_numbers": True,
        # Latest = Appointment Reminder SMS delivered 259, OR
        # latest promo = March Madness delivered 1905
        "must_contain_one_of": ["259", "1905", "1,905", "reached", "delivered"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Category 3: Rankings ───────────────────────────────────────────────
    "Q10": {
        "text":     "Which campaign had the highest open rate this year?",
        "category": "Rankings",
        "expect_numbers": True,
        # 2026 best open rate: Express Facial Launch at 49.19%
        "must_contain_one_of": ["express facial", "49", "highest", "open rate"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q11": {
        "text":     "Which campaign had the highest click rate this year?",
        "category": "Rankings",
        "expect_numbers": True,
        # 2026 best click rate is Welcome Series at 8.11% (Mar) or
        # one of the other email campaigns. Any top-performer + rate is fine.
        "must_contain_one_of": ["welcome series", "click rate", "8", "highest"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q12": {
        "text":     "What was my best-performing campaign last quarter and why?",
        "category": "Rankings",
        "expect_numbers": True,
        # Q1 2026 winner: Express Facial Launch (49.19% open) OR
        # March Madness (high reach). Either narrative is valid.
        "must_contain_one_of": ["express facial", "march madness", "best", "top"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q13": {
        "text":     "Which campaign reached the most customers?",
        "category": "Rankings",
        "expect_numbers": True,
        # March Madness delivered 1905 — biggest single reach in 2026
        "must_contain_one_of": ["march madness", "1905", "1,905", "reached", "most"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q14": {
        "text":     "Which of my recurring campaigns performs best over time?",
        "category": "Rankings",
        "expect_numbers": True,
        # Recurring campaigns: Welcome Series (email, best opens),
        # Appointment Reminder (SMS, good clicks),
        # Re-engagement Win Back (email)
        "must_contain_one_of": [
            "welcome series", "recurring", "best", "appointment reminder",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Category 4: ROI / Attribution ──────────────────────────────────────
    "Q15": {
        "text":     "How much revenue did my last promo campaign generate?",
        "category": "ROI",
        "expect_numbers": True,
        # Last promo campaign that had redemptions = March Madness, Mar 2026.
        # Alternatively Welcome Series (recurring) $877 net in Mar.
        "must_contain_one_of": [
            "877", "march madness", "welcome series",
            "revenue", "net", "$",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q16": {
        "text":     "Which campaign promo code was redeemed the most last month?",
        "category": "ROI",
        "expect_numbers": True,
        # Mar 2026: Welcome Series (WELCOME10) had 10 redemptions
        "must_contain_one_of": ["welcome10", "welcome series", "10", "redemption"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q17": {
        "text":     "What is my average revenue per campaign sent this year?",
        "category": "ROI",
        "expect_numbers": True,
        # Average across 2026 campaigns — needs division or formulaic answer.
        # Must at least talk about revenue + campaign context.
        "must_contain_one_of": ["revenue", "per campaign", "average", "$"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q18": {
        "text":     "Did the March promotion pay for itself?",
        "category": "ROI",
        "expect_numbers": True,
        # March Madness: does the net revenue exceed discount given?
        # The answer should address revenue vs discount given.
        "must_contain_one_of": [
            "march madness", "yes", "paid", "net", "revenue", "discount",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Category 5: Trends ─────────────────────────────────────────────────
    "Q19": {
        "text":     "Are my open rates going up or down over the last 6 months?",
        "category": "Trends",
        "expect_numbers": True,
        "must_contain_one_of": ["open rate", "up", "down", "trend", "%", "improving"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q20": {
        "text":     "Is my click rate improving?",
        "category": "Trends",
        "expect_numbers": True,
        "must_contain_one_of": ["click rate", "improv", "up", "down", "trend", "%"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q21": {
        "text":     "How has my email send volume changed month over month?",
        "category": "Trends",
        "expect_numbers": True,
        # Mar vs Feb: should reference both periods and a delta
        "must_contain_one_of": [
            "email", "mom", "month over month", "volume", "change", "%",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q22": {
        "text":     "Am I sending more SMS or more emails this year compared to last year?",
        "category": "Trends",
        "expect_numbers": True,
        "must_contain_one_of": [
            "email", "sms", "more", "compared", "year",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Category 6: Channel Mix ────────────────────────────────────────────
    "Q23": {
        "text":     "Do my SMS campaigns get better engagement than my email campaigns?",
        "category": "Channel Mix",
        "expect_numbers": True,
        "must_contain_one_of": [
            "sms", "email", "engagement", "click", "better", "compare",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q24": {
        "text":     "What percentage of my outreach last month was email vs SMS?",
        "category": "Channel Mix",
        "expect_numbers": True,
        # Mar 2026: email ≈ 82% (2418/2943), sms ≈ 18% (525/2943)
        "must_contain_one_of": ["82", "18", "email", "sms", "%", "outreach"],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Category 7: Recurring / Templates ──────────────────────────────────
    "Q25": {
        "text":     "Is my recurring campaign still performing — when did it last run and what were the results?",
        "category": "Recurring",
        "expect_numbers": True,
        "must_contain_one_of": [
            "welcome series", "appointment reminder", "last run",
            "recurring", "still",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q26": {
        "text":     "Which campaign template format gets the best open and click rates?",
        "category": "Recurring",
        "expect_numbers": True,
        "must_contain_one_of": [
            "template", "service launch", "best", "open", "click",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Category 8: List Health ────────────────────────────────────────────
    "Q27": {
        "text":     "How many customers unsubscribed from email and SMS last month?",
        "category": "List Health",
        "expect_numbers": True,
        # Mar 2026 email net_unsub_delta = 7 new opt-outs
        "must_contain_one_of": ["7", "unsubscribe", "opt out", "email", "sms"],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q28": {
        "text":     "Is my email-contactable list growing or shrinking over the last 6 months?",
        "category": "List Health",
        "expect_numbers": True,
        "must_contain_one_of": [
            "contactable", "growing", "shrinking", "list",
            "email", "trend",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Category 9: Location / Branch ──────────────────────────────────────
    "Q29": {
        "text":     "Which location had the highest rate of promo redemptions from my campaigns last month?",
        "category": "Location",
        "expect_numbers": True,
        "must_contain_one_of": [
            "location", "branch", "main st", "westside", "highest",
            "redemption",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q30": {
        "text":     "How much promo-driven revenue did each branch get from campaigns?",
        "category": "Location",
        "expect_numbers": True,
        "must_contain_one_of": [
            "location", "branch", "main st", "westside", "$",
            "revenue",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },

    # ── Category 10: Why / Advice ──────────────────────────────────────────
    "Q31": {
        "text":     "Why did my last campaign underperform compared to the previous one?",
        "category": "Advice",
        "expect_numbers": True,
        # Compares two most recent campaigns on open/click rates
        "must_contain_one_of": [
            "compare", "previous", "underperform", "open rate",
            "click rate", "lower",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q32": {
        "text":     "What can I do to improve my email open rates?",
        "category": "Advice",
        "expect_numbers": False,
        "must_contain_one_of": [
            "subject", "send time", "template", "day of the week",
            "segment", "list", "open rate",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q33": {
        "text":     "Which day of the week has historically given me the best open rates?",
        "category": "Advice",
        "expect_numbers": True,
        "must_contain_one_of": [
            "monday", "tuesday", "wednesday", "thursday", "friday",
            "saturday", "sunday", "day of", "best day", "%",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },
    "Q34": {
        "text":     "Am I over-sending to my customers?",
        "category": "Advice",
        "expect_numbers": True,
        "must_contain_one_of": [
            "send", "volume", "unsubscribe", "opt out",
            "over", "fatigue", "too many",
        ],
        "must_not_contain":    ["no data", "don't have"],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Biz 42 leak signatures — used in isolation mode (biz 99)
# If any of these appear in a biz 99 answer, it's a cross-tenant leak.
# ─────────────────────────────────────────────────────────────────────────────

_BIZ42_LEAK_SIGNATURES = [
    # Unique campaign names from biz 42 fixture
    "welcome series", "summer spa special", "holiday promo",
    "express facial launch", "march madness",
    "re-engagement win back", "appointment reminder",
    "client survey",
    # Promo codes
    "welcome10", "summer20", "holiday15",
    # Distinctive numeric signatures
    "49.19", "49.2",       # Express Facial top open rate
    "34.17",               # March Madness open rate
    "40.54",               # Welcome Series Mar 2026 open
    "2418", "2,418",       # Mar 2026 email volume
    "1905", "1,905",       # March Madness delivery
    # WELCOME10 H1 2025 redemption numbers
    "38 redemption", "27 redemption",
    # Location names unique to biz 42
    "main st", "westside",
]


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
    route:       str       = ""


def score_answer(
    q_id: str,
    spec: dict,
    answer: str,
    route: str,
    isolation_mode: bool = False,
) -> QuestionResult:
    # Isolation-mode scoring: biz has NO embedded data, so "insufficient data"
    # or generic advice is correct. Only fail on biz 42 data leaks.
    if isolation_mode:
        return _score_isolation(q_id, spec, answer, route)

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

    # 5. Route check (optional — if Marketing ever adds a blocked route later)
    expected_route = spec.get("expected_route")
    if expected_route and route != expected_route:
        issues.append(f"wrong_route(expected={expected_route}, got={route})")

    return QuestionResult(
        q_id=q_id,
        category=spec.get("category", ""),
        question=spec["text"],
        passed=len(issues) == 0,
        issues=issues,
        answer=answer,
        route=route,
    )


def _score_isolation(
    q_id: str,
    spec: dict,
    answer: str,
    route: str,
) -> QuestionResult:
    """
    Score an answer under tenant-isolation mode.

    Correct behavior for biz 99 (no embedded data):
      - Either "insufficient data" / refusal (preferred)
      - Or generic advice with no biz 42 facts
    A failure is:
      - Any biz 42 campaign/promo signature appearing
      - Any biz 42-specific number (e.g. 49.19, 2418, 1905)
      - Biz 42 location names ("Main St", "Westside")
    """
    issues = []
    a_lower = answer.lower()

    for sig in _BIZ42_LEAK_SIGNATURES:
        if sig in a_lower:
            issues.append(f"TENANT_LEAK_signature({sig!r} appeared)")
            break

    return QuestionResult(
        q_id=q_id,
        category=spec.get("category", ""),
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
        "session_id":  "step6-marketing-test",
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
        if not answer and "data" in body and isinstance(body["data"], dict):
            answer = body["data"].get("answer", "")
            route  = body["data"].get("route", route)
    elif isinstance(body, str):
        answer = body

    return str(answer), str(route), latency


async def run_tests(
    endpoint: str = CHAT_ENDPOINT,
    business_id: str = BUSINESS_ID,
    question_id: str | None = None,
    output_path: str | None = None,
    isolation_mode: bool = False,
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

    mode_label = "TENANT ISOLATION" if isolation_mode else "Marketing Domain"
    print(f"\n{'═'*62}")
    print(f"  LEO AI BI — Step 6: {mode_label} Test")
    print(f"  Questions   : {len(questions)}")
    print(f"  Endpoint    : {endpoint}")
    print(f"  business_id : {business_id}")
    if isolation_mode:
        print(f"  Mode        : ISOLATION — success = no biz 42 data leaks")
    print(f"{'═'*62}")

    results: list[QuestionResult] = []

    for q_id, spec in questions.items():
        print(f"  → [{q_id}] {spec['text']}")
        try:
            answer, route, latency = await ask_question(
                endpoint, business_id, spec["text"]
            )

            # Transient-flake retry: if the chat service returned an ERROR
            # route (caught exception in ChatService.handle), retry once
            # after a short backoff.
            if route == "ERROR":
                print(f"         ⟳ route=ERROR, retrying once after 2s backoff...")
                await asyncio.sleep(2.0)
                answer, route, latency_retry = await ask_question(
                    endpoint, business_id, spec["text"]
                )
                latency += latency_retry

            result = score_answer(
                q_id, spec, answer, route, isolation_mode=isolation_mode
            )
            result.latency_ms = latency

            status = "✅" if result.passed else "❌"
            issues_str = ", ".join(result.issues) if result.issues else "—"
            route_str = f"[{route}]" if route else ""
            print(
                f"  {status} [{q_id:<4}] {spec['category']:<14} "
                f"{route_str:<14} latency={latency:>6.0f}ms  issues: {issues_str}"
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

    categories: dict[str, tuple[int, int]] = {}
    for r in results:
        p, t = categories.get(r.category, (0, 0))
        categories[r.category] = (p + (1 if r.passed else 0), t + 1)

    print(f"\n{'═'*62}")
    print(f"  Step 6 Results — Marketing Domain")
    print(f"{'═'*62}")
    print(f"  Overall     : {passed}/{total} passed ({passed/total*100:.0f}%)")
    print(f"  Avg latency : {avg_lat:.0f}ms")
    print(f"  By category :")
    for cat, (cp, ct) in sorted(categories.items()):
        bar = "█" * cp + "░" * (ct - cp)
        print(f"    {cat:<14} {bar}  {cp}/{ct}")

    # Tenant leak summary (prominent in isolation mode)
    tenant_leaks = [r for r in results if any("TENANT_LEAK" in i for i in r.issues)]
    if tenant_leaks:
        print(f"\n  ⚠️  TENANT LEAKS DETECTED: {len(tenant_leaks)} question(s)")
        for r in tenant_leaks:
            leak_issue = next(i for i in r.issues if "TENANT_LEAK" in i)
            print(f"    ⚠️  [{r.q_id}] {leak_issue}")

    if failed:
        print(f"\n  ── Failed ({failed}) ──────────────────────────────")
        for r in results:
            if not r.passed:
                print(f"    ❌ [{r.q_id}] {r.question}")
                print(f"         Issues: {', '.join(r.issues)}")

    print(f"{'═'*62}")
    if passed == total:
        mode = "isolation" if isolation_mode else "marketing"
        print(f"  ✅ STEP 6 PASSED — {mode} domain ready for Step 7 sign-off")
    else:
        print(f"  ❌ STEP 6 INCOMPLETE — {failed} question(s) failing")
        print("     Fix gaps, then re-run before Step 7.")
    print(f"{'═'*62}\n")

    # ── JSON output ───────────────────────────────────────────────────────
    if output_path:
        import os
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        report = {
            "domain":         "marketing",
            "step":           6,
            "business_id":    business_id,
            "endpoint":       endpoint,
            "isolation_mode": isolation_mode,
            "total":          total,
            "passed":         passed,
            "failed":         failed,
            "pass_rate_pct":  round(passed / total * 100, 1) if total else 0,
            "avg_latency_ms": round(avg_lat, 1),
            "tenant_leaks":   len(tenant_leaks),
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
        description="Step 6 — Marketing domain test harness"
    )
    parser.add_argument(
        "--question", type=str, default=None, metavar="QID",
        help="Run a single question by ID (e.g. Q1, Q18). Omit to run all 34.",
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
    parser.add_argument(
        "--isolation-mode", action="store_true", dest="isolation_mode",
        help="Tenant-isolation scoring: pass = no biz 42 data leaks. "
             "Use with --business-id 99 to verify tenant separation.",
    )
    args = parser.parse_args()

    asyncio.run(
        run_tests(
            endpoint=args.endpoint,
            business_id=args.business_id,
            question_id=args.question,
            output_path=args.output,
            isolation_mode=args.isolation_mode,
        )
    )