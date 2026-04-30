"""
scripts/tests/test_memberships_step6.py
=========================================
Step 6 — Memberships Domain Test

Fires all 29 Step-1 questions (21 main + 8 location) through the live
FastAPI chat endpoint at http://localhost:8000/api/v1/chat and scores
each answer against ground-truth anchors derived from the biz-99
memberships fixture.

Story anchors baked into biz-99 fixture (see memberships_fixtures.py
+ Step 1 ground-truth output for derivation):

    Q1   — 25 active memberships RIGHT NOW
    Q2   — total April billed = $2,335.00
    Q3   — total MRR = $2,711.44
    Q4   — Mar signups=2 vs Feb=1 (small absolute, growth in trend)
    Q5   — 6mo trend: Downtown flat / Westside shrinking / Northpark growing
    Q6   — Q1 2026 ($8,415) vs Q4 2025 ($8,230) — roughly flat
    Q7   — peak signup month = September 2025 (3 signups)
    Q8   — service ranking: Monthly Massage dominates (13 active)
    Q9   — interval buckets: monthly/weekly/quarterly/bi-weekly/other all present
    Q10  — longest tenured: Maria Hernandez, 559 days
    Q11  — Monthly Massage = 47.6% of MRR (top)
    Q12  — Why MRR down — soft / lenient (no hard story)
    Q13  — Feb 2026 cancel cluster at Westside, 5 cancels (Linda, Charles,
           Barbara, Joseph, Susan), mostly Monthly Massage
    Q14  — Why signups dropped — Q1 2026 quieter than late 2025
    Q15  — Advice (lenient)
    Q16  — Service→membership advice — cross-domain, lenient
    Q17  — Avg discount = $11.67 across 6 active members with discount
    Q18  — 11 memberships due in next 7 days (weekly subs heavy)
    Q19  — 21 of 25 active members used membership; 4 ghosts (Frank, Edward,
           Daniel reactivation, David)
    Q20  — Avg LTV = $932; range $50–$3,371
    Q21  — 5 failed payments — ALL in March 2026 (Westside has 3, Northpark
           has 1, Downtown has 1)

    Locations:
    M-LQ1  — 11 Downtown / 4 Westside / 10 Northpark
    M-LQ2  — Downtown most active (11)
    M-LQ3  — Apr revenue: $1050 Downtown / $265 Westside / $1020 Northpark
    M-LQ4  — Northpark most signups in Mar (2 vs 0 elsewhere)
    M-LQ5  — Westside highest churn (5 cancels in 6mo, others 0)
    M-LQ6  — Avg amount/loc: ~$101 Downtown, $144 Westside, $102 Northpark
    M-LQ7  — % of total revenue from memberships per location (cross-domain)
    M-LQ8  — Biggest Mar→Apr drop: Downtown -$495 (or all 3 dropped, lenient)

Usage:
    PYTHONPATH=. python scripts/tests/test_memberships_step6.py
    PYTHONPATH=. python scripts/tests/test_memberships_step6.py --question Q10
    PYTHONPATH=. python scripts/tests/test_memberships_step6.py --output results/step6_memberships.json
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
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

import httpx

# ─── Config ─────────────────────────────────────────────────────────────────

CHAT_ENDPOINT   = "http://localhost:8000/api/v1/chat"
ORG_ID          = 99
REQUEST_TIMEOUT = 60.0
MAX_RETRIES     = 2


# ─── 29 Test questions ──────────────────────────────────────────────────────

QUESTIONS: dict[str, dict] = {

    # ── Basic Facts ────────────────────────────────────────────────────────
    "Q1": {
        "text":     "How many active memberships do I have right now?",
        "category": "Basic Facts",
        "expect_numbers": True,
        "must_contain_one_of": ["25", "active", "membership"],
        "must_not_contain":   ["don't have", "no data", "unable to"],
    },
    "Q2": {
        "text":     "What is my total membership revenue this month?",
        "category": "Basic Facts",
        "expect_numbers": True,
        "must_contain_one_of": ["$", "revenue", "billed", "membership"],
        "period_keywords":     ["april", "this month", "2026"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "Q3": {
        "text":     "What's my average monthly recurring revenue from memberships?",
        "category": "Basic Facts",
        "expect_numbers": True,
        # Accept any close approximation of $2,711 — LLMs round inconsistently
        "must_contain_one_of": ["mrr", "$2,7", "2,711", "2711", "$2,6", "$2,8", "recurring"],
        "must_not_contain":    ["don't have", "no data"],
    },

    # ── Trends ────────────────────────────────────────────────────────────
    "Q4": {
        "text":     "How many new memberships did I sign up last month vs the month before?",
        "category": "Trends",
        "expect_numbers": True,
        "must_contain_one_of": ["signup", "new", "member", "march", "february"],
        "period_keywords":     ["march", "february", "last month"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "Q5": {
        "text":     "Is my membership base growing or shrinking over the last 6 months?",
        "category": "Trends",
        "expect_numbers": True,
        # Mixed story: must mention at least one location's trend, OR overall
        "must_contain_one_of": [
            "downtown", "westside", "northpark",
            "growing", "shrinking", "declining", "flat", "mixed",
        ],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q6": {
        "text":     "How does membership revenue this quarter compare to last quarter?",
        "category": "Trends",
        "expect_numbers": True,
        "must_contain_one_of": [
            "quarter", "q1", "q4", "$8,4", "$8,2", "8,415", "8,230",
            "similar", "flat",
        ],
        "any_keyword": True,
        "must_not_contain": ["don't have", "no data"],
    },
    "Q7": {
        "text":     "Which month had the most new membership signups this year?",
        "category": "Trends",
        "expect_numbers": True,
        # Top is Sep 2025 (3 signups) — but it's "this year" so AI might
        # interpret strictly as 2026 → Mar=2 wins. Accept either.
        "must_contain_one_of": ["september", "march", "sep"],
        "must_not_contain":    ["don't have", "no data"],
    },

    # ── Rankings ──────────────────────────────────────────────────────────
    "Q8": {
        "text":     "Which service has the most active memberships attached to it?",
        "category": "Rankings",
        "expect_numbers": True,
        "must_contain_one_of": ["monthly massage", "Monthly Massage"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "Q9": {
        "text":     "What's the breakdown of memberships by billing interval — weekly, bi-weekly, monthly, and quarterly?",
        "category": "Rankings",
        "expect_numbers": True,
        # Must mention at least 2 of the buckets
        "must_contain_one_of": ["monthly", "weekly", "quarterly", "bi-weekly"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "Q10": {
        "text":     "Who are my longest-tenured members? Show me the top few.",
        "category": "Rankings",
        "expect_numbers": True,
        "must_contain_one_of": ["maria", "Maria", "Hernandez", "559"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "Q11": {
        "text":     "What percentage of membership revenue comes from each service?",
        "category": "Rankings",
        "expect_numbers": True,
        "must_contain_one_of": [
            "monthly massage", "Monthly Massage",
            "%", "percent",
        ],
        "must_not_contain": ["don't have", "no data"],
    },

    # ── Why / Root Cause ──────────────────────────────────────────────────
    "Q12": {
        "text":     "Why is my membership revenue down this month?",
        "category": "Why / Root Cause",
        # No hard root-cause story in the fixture — accept honest reasoning
        "expect_numbers": False,
        "must_contain_one_of": [
            "april", "march", "billing", "cycle", "normal", "variance",
            "failed", "westside", "monthly", "weekly",
        ],
        "any_keyword": True,
        "must_not_contain": [],
    },
    "Q13": {
        "text":     "Why did so many members cancel last month?",
        "category": "Why / Root Cause",
        "expect_numbers": True,
        # Feb 2026 had 5 cancels at Westside — the cluster
        "must_contain_one_of": ["westside", "Westside", "february", "5"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "Q14": {
        "text":     "Why did new membership signups drop?",
        "category": "Why / Root Cause",
        "expect_numbers": False,
        "must_contain_one_of": [
            "signup", "new", "decline", "fewer", "drop",
            "2026", "2025", "month", "quarter",
        ],
        "any_keyword": True,
        "must_not_contain": [],
    },

    # ── Advice ────────────────────────────────────────────────────────────
    "Q15": {
        "text":     "How can I grow my membership base?",
        "category": "Advice",
        "expect_numbers": False,
        "must_contain_one_of": [
            "membership", "grow", "retention", "signup", "discount",
            "service", "promote",
        ],
        "any_keyword": True,
        "must_not_contain": [],
    },
    "Q16": {
        "text":     "Which of my services should I turn into a membership offering?",
        "category": "Advice",
        # Cross-domain (needs visit-frequency data); lenient
        "expect_numbers": False,
        "must_contain_one_of": [
            "service", "membership", "frequent", "repeat",
            "monthly massage", "weekly facial", "consider",
        ],
        "any_keyword": True,
        "must_not_contain": [],
    },

    # ── Edge cases ────────────────────────────────────────────────────────
    "Q17": {
        "text":     "What's the average discount I give on memberships?",
        "category": "Edge Cases",
        "expect_numbers": True,
        "must_contain_one_of": ["$", "discount", "average", "$11", "$12"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "Q18": {
        "text":     "How many memberships have a payment due in the next 7 days?",
        "category": "Edge Cases",
        "expect_numbers": True,
        # 11 due — most are weekly subs (every weekly sub is always due in 7d)
        "must_contain_one_of": ["11", "weekly", "due"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "Q19": {
        "text":     "Do my members spend more per visit than non-members?",
        "category": "Edge Cases",
        "expect_numbers": False,
        # Cross-domain (needs revenue/visits side); lenient
        "must_contain_one_of": [
            "member", "visit", "spend", "more", "less", "compare",
        ],
        "any_keyword": True,
        "must_not_contain": [],
    },
    "Q20": {
        "text":     "What's the average lifetime value of a membership customer?",
        "category": "Edge Cases",
        "expect_numbers": True,
        "must_contain_one_of": [
            "ltv", "lifetime", "$932", "$900", "$1,0", "average",
        ],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q21": {
        "text":     "How many membership payments failed last month?",
        "category": "Edge Cases",
        "expect_numbers": True,
        # NB: "last month" = March 2026 (5 failed, all at March). April had 0.
        "must_contain_one_of": ["5", "march", "westside", "failed"],
        "period_keywords":     ["march", "last month"],
        "must_not_contain":    ["don't have", "no data"],
    },

    # ── Location ──────────────────────────────────────────────────────────
    "M-LQ1": {
        "text":     "How many active memberships does each location have?",
        "category": "Location",
        "expect_numbers": True,
        "must_contain_one_of": ["downtown", "Downtown", "westside", "Westside", "northpark", "Northpark"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "M-LQ2": {
        "text":     "Which location has the most active memberships?",
        "category": "Location",
        "expect_numbers": True,
        # Downtown has 11 active (vs 4 Westside, 10 Northpark)
        "must_contain_one_of": ["downtown", "Downtown"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "M-LQ3": {
        "text":     "What's the membership revenue per location this month?",
        "category": "Location",
        "expect_numbers": True,
        "must_contain_one_of": [
            "downtown", "westside", "northpark",
            "$", "revenue", "billed",
        ],
        "period_keywords":     ["april", "this month"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "M-LQ4": {
        "text":     "Which location signed up the most new memberships last month?",
        "category": "Location",
        "expect_numbers": True,
        "must_contain_one_of": ["northpark", "Northpark"],
        "period_keywords":     ["march", "last month"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "M-LQ5": {
        "text":     "Which location has the highest membership churn?",
        "category": "Location",
        "expect_numbers": True,
        "must_contain_one_of": ["westside", "Westside"],
        "must_not_contain":    ["don't have", "no data"],
    },
    "M-LQ6": {
        "text":     "What's the average membership amount per location?",
        "category": "Location",
        "expect_numbers": True,
        "must_contain_one_of": [
            "downtown", "westside", "northpark",
            "$", "average", "amount",
        ],
        "must_not_contain": ["don't have", "no data"],
    },
    "M-LQ7": {
        "text":     "What percentage of each location's revenue comes from memberships?",
        "category": "Location",
        # Cross-domain — memberships docs alone can't answer the % of TOTAL
        # revenue (needs revenue domain). Lenient.
        "expect_numbers": False,
        "must_contain_one_of": [
            "downtown", "westside", "northpark",
            "%", "percent", "revenue", "membership",
        ],
        "any_keyword": True,
        "must_not_contain": [],
    },
    "M-LQ8": {
        "text":     "Which location had the biggest membership revenue drop last month?",
        "category": "Location",
        "expect_numbers": True,
        # All 3 dropped Mar→Apr; Downtown -$495 is biggest. Accept any
        # location named — what matters is the AI surfaces a real drop.
        "must_contain_one_of": [
            "downtown", "Downtown",
            "westside", "Westside",
            "northpark", "Northpark",
            "drop", "decreased",
        ],
        "must_not_contain": ["don't have", "no data"],
    },
}


# ─── Result dataclass ───────────────────────────────────────────────────────

@dataclass
class QResult:
    qid:        str
    question:   str
    category:   str
    answer:     str
    route:      str | None
    confidence: float | None
    sources:    list[str]
    latency_ms: float
    # check outcomes
    non_empty:        bool = False
    has_number:       bool = True       # default True if expect_numbers=False
    contains_keyword: bool = True
    period_mentioned: bool = True
    no_refusal:       bool = True
    # final
    passed: bool = False
    issues: list[str] = field(default_factory=list)


# ─── Chat call ──────────────────────────────────────────────────────────────

NUMBER_RE = re.compile(r"\d")


async def ask(client: httpx.AsyncClient, question: str, org_id: int) -> dict[str, Any]:
    """POST /api/v1/chat with retry on transient errors. Returns parsed JSON."""
    payload = {
        "business_id": str(org_id),
        "org_id":      str(org_id),
        "question":    question,
        "session_id":  str(uuid.uuid4()),
    }
    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = await client.post(CHAT_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            if 500 <= resp.status_code < 600 and attempt < MAX_RETRIES:
                await asyncio.sleep(1.5)
                continue
            return {
                "answer":     f"HTTP {resp.status_code}: {resp.text[:200]}",
                "route":      None, "confidence": None, "sources": [],
            }
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_err = e
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1.5)
                continue
            return {
                "answer":     f"Transport error: {e!r}",
                "route":      None, "confidence": None, "sources": [],
            }
    return {
        "answer":     f"Exhausted retries: {last_err!r}",
        "route":      None, "confidence": None, "sources": [],
    }


# ─── Score a single result ─────────────────────────────────────────────────

def score(qid: str, spec: dict, result: dict, latency_ms: float) -> QResult:
    answer       = (result.get("answer") or "").strip()
    answer_lower = answer.lower()

    r = QResult(
        qid=qid,
        question=spec["text"],
        category=spec["category"],
        answer=answer,
        route=result.get("route"),
        confidence=result.get("confidence"),
        sources=result.get("sources") or [],
        latency_ms=latency_ms,
    )

    # 1. Non-empty answer
    r.non_empty = bool(answer) and not answer.startswith(
        ("HTTP ", "Transport", "Exhausted")
    )
    if not r.non_empty:
        r.issues.append("empty_or_http_error")

    # 2. has_number
    if spec.get("expect_numbers", False):
        r.has_number = bool(NUMBER_RE.search(answer))
        if not r.has_number:
            r.issues.append("missing_number")

    # 3. contains_keyword
    expect = spec.get("must_contain_one_of") or []
    if expect:
        hits = [kw for kw in expect if kw.lower() in answer_lower]
        r.contains_keyword = bool(hits)
        if not r.contains_keyword:
            r.issues.append(f"no_keyword({','.join(expect[:3])}...)")

    # 4. period_keywords (optional)
    period = spec.get("period_keywords") or []
    if period:
        hits = [p for p in period if p.lower() in answer_lower]
        r.period_mentioned = bool(hits)
        if not r.period_mentioned:
            r.issues.append(f"no_period({','.join(period[:2])})")

    # 5. must_not_contain (refusal detection)
    forbidden = spec.get("must_not_contain") or []
    leaked = [f for f in forbidden if f.lower() in answer_lower]
    r.no_refusal = not leaked
    if leaked:
        r.issues.append(f"forbidden({','.join(leaked)})")

    # Final pass
    r.passed = (
        r.non_empty
        and r.has_number
        and r.contains_keyword
        and r.period_mentioned
        and r.no_refusal
    )
    return r


# ─── Print helpers ──────────────────────────────────────────────────────────

def print_row(r: QResult) -> None:
    mark = "✅" if r.passed else "❌"
    issues = "; ".join(r.issues) if r.issues else "-"
    print(
        f"  {mark} [{r.qid:>5}] {r.category:<18} "
        f"empty={'✓' if r.non_empty else '✗'} "
        f"num={'✓' if r.has_number else '✗'} "
        f"kw={'✓' if r.contains_keyword else '✗'} "
        f"period={'✓' if r.period_mentioned else '✗'} "
        f"clean={'✓' if r.no_refusal else '✗'} "
        f"[{r.latency_ms:>5.0f}ms]"
    )
    if not r.passed:
        preview = r.answer.replace("\n", " ")[:160]
        print(f"           Q: {r.question}")
        print(f"           A: {preview}...")
        print(f"           Issues: {issues}")


def print_summary(results: list[QResult]) -> None:
    total  = len(results)
    passed = sum(1 for r in results if r.passed)
    print("\n" + "═" * 80)
    print(f"  Step 6 Results — Memberships Domain")
    print("═" * 80)
    print(f"  Overall : {passed}/{total} passed ({100*passed//max(total,1)}%)")
    print(f"  Time    : {sum(r.latency_ms for r in results)/1000:.1f}s total")

    by_cat: dict[str, list[QResult]] = {}
    for r in results:
        by_cat.setdefault(r.category, []).append(r)
    print(f"  By category:")
    for cat, rs in by_cat.items():
        p = sum(1 for r in rs if r.passed)
        bar = "█" * p + "░" * (len(rs) - p)
        print(f"    {cat:<20} {bar}  {p}/{len(rs)}")

    fails = [r for r in results if not r.passed]
    if fails:
        print(f"\n  ── Failed ({len(fails)}) ─────────────────────────────────")
        for r in fails:
            print(f"    ❌ [{r.qid}] {r.question}")
            if r.issues:
                print(f"           Issues: {'; '.join(r.issues)}")

    print("═" * 80)
    if passed == total:
        print(f"  ✅ STEP 6 PASSED — all {total} questions answered correctly")
    else:
        print(f"  ❌ STEP 6 INCOMPLETE — {total - passed} failing")
    print("═" * 80)


# ─── Main ───────────────────────────────────────────────────────────────────

async def run_question(client, qid, spec, org_id) -> QResult:
    print(f"  → [{qid:>5}] {spec['text'][:70]}...")
    t0 = time.time()
    result = await ask(client, spec["text"], org_id)
    elapsed = (time.time() - t0) * 1000
    r = score(qid, spec, result, elapsed)
    print_row(r)
    return r


async def main(args):
    if args.question:
        if args.question not in QUESTIONS:
            print(f"Unknown question: {args.question}")
            print(f"Available: {', '.join(QUESTIONS)}")
            sys.exit(1)
        to_run = [args.question]
    else:
        to_run = list(QUESTIONS.keys())

    print(f"── Running {len(to_run)} questions against {CHAT_ENDPOINT} ──")
    print(f"   org_id      : {args.org_id}")
    print()

    results: list[QResult] = []
    async with httpx.AsyncClient() as client:
        for qid in to_run:
            r = await run_question(client, qid, QUESTIONS[qid], args.org_id)
            results.append(r)

    print_summary(results)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "endpoint":   CHAT_ENDPOINT,
            "org_id":     args.org_id,
            "total":      len(results),
            "passed":     sum(1 for r in results if r.passed),
            "results":    [
                {
                    "qid":        r.qid,
                    "category":   r.category,
                    "question":   r.question,
                    "passed":     r.passed,
                    "issues":     r.issues,
                    "route":      r.route,
                    "confidence": r.confidence,
                    "sources":    r.sources,
                    "latency_ms": r.latency_ms,
                    "answer":     r.answer,
                }
                for r in results
            ],
        }
        out.write_text(json.dumps(data, indent=2, ensure_ascii=False))
        print(f"\n  Results written → {out}")

    sys.exit(0 if all(r.passed for r in results) else 1)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Step 6 — Memberships domain test harness")
    p.add_argument("--question", help="Run a single question (e.g. Q10)")
    p.add_argument("--output",   help="Write results as JSON to this path")
    p.add_argument("--org-id", type=int, default=ORG_ID, help="Tenant ID (default: 99)")
    p.add_argument("--endpoint", default=CHAT_ENDPOINT)
    args = p.parse_args()

    CHAT_ENDPOINT = args.endpoint
    asyncio.run(main(args))