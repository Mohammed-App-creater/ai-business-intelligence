"""
scripts/tests/test_promos_step6.py
====================================
Step 6 — Promos Domain Test

Fires all 26 Step-1 questions through the live FastAPI chat endpoint at
http://localhost:8000/api/v1/chat and scores each answer against domain-
specific criteria.

Scoring criteria per question:
    ✅ Non-empty answer
    ✅ expect_numbers: $ value, % value, or digit present (if required)
    ✅ must_contain_one_of: at least one expected keyword (or 'any_keyword')
    ✅ must_not_contain: forbidden terms (refusals, names, leaks)
    ✅ period_keywords: mentions expected time language (if relevant)

Story anchors baked into biz-42 fixture (must match PR output):
    Q1   — redemptions last month (Mar 2026) = 22ish
    Q2   — total discount Mar 2026 (~$280)
    Q3   — distinct codes 6mo = 6 (5 real + 1 orphan)
    Q4   — total discount YTD (Jan-Apr) aggregate
    Q5   — Mar vs Feb MoM (spike)
    Q6   — 6-month trend (growing: ~5% → ~9% visits-with-promo)
    Q7   — best month = Mar 2026 (highest redemptions)
    Q8   — QoQ Q1 2026 vs no Q4 2025 baseline (window starts Nov)
    Q9   — most redeemed Mar = DM8880 (14 redemptions)
    Q10  — top by total discount window = DM8880
    Q11  — biggest avg discount = DM8880
    Q12  — promo visit % Mar 2026 (promo_visit_pct)
    Q13  — least used Jan-Mar = PM8880 (dropped from 6→2)
    Q14  — why spike in Mar = DM8880 jumped 8→14 (+75%)
    Q15  — which codes lost activity = PM8880
    Q16  — advice: which promos to retire
    Q17  — advice: promo strategy
    Q18  — promos by location Mar 2026
    Q19  — which branch redeems most coupons (by count) = Westside
    Q20  — detailed by-code-by-branch
    Q21  — biggest discount location (by amount) = Main St
    Q22  — dormant promo = DM881 (0 redemptions in window)
    Q23  — active-but-expired = POFL99 (active=1, expired 2025-02-15)
    Q24  — orphan promo (promo_id=999) handling
    Q25  — avg discount per redemption Mar 2026
    Q26  — 6-month promo activity trend

Usage:
    # Run all 26 questions
    PYTHONPATH=. python scripts/tests/test_promos_step6.py

    # Run just one question
    PYTHONPATH=. python scripts/tests/test_promos_step6.py --question Q9

    # Save results to JSON
    PYTHONPATH=. python scripts/tests/test_promos_step6.py --output results/step6_promos.json

    # Use different org_id (e.g. tenant isolation check)
    PYTHONPATH=. python scripts/tests/test_promos_step6.py --org-id 99
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

# ─── Config ────────────────────────────────────────────────────────────────────

CHAT_ENDPOINT   = "http://localhost:8000/api/v1/chat"
ORG_ID          = 42
REQUEST_TIMEOUT = 60.0
MAX_RETRIES     = 2

# ─── 26 Test questions — Step 1 ────────────────────────────────────────────────

QUESTIONS: dict[str, dict] = {

    # ── Basic Facts ─────────────────────────────────────────────────────────
    "Q1": {
        "text":     "How many promos were redeemed last month?",
        "category": "Basic Facts",
        "expect_numbers": True,
        "must_contain_one_of": ["promo", "redemption", "redeemed", "coupon"],
        "period_keywords": ["march", "last month", "2026"],
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q2": {
        "text":     "What was the total discount given through promos last month?",
        "category": "Basic Facts",
        "expect_numbers": True,
        "must_contain_one_of": ["$", "discount", "total"],
        "period_keywords": ["march", "last month"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q3": {
        "text":     "How many distinct promo codes were used in the last 6 months?",
        "category": "Basic Facts",
        "expect_numbers": True,
        "must_contain_one_of": ["distinct", "codes", "different"],
        "must_not_contain": ["don't have", "unable to"],
    },
    "Q4": {
        "text":     "What was the total discount given this year to date?",
        "category": "Basic Facts",
        "expect_numbers": True,
        "must_contain_one_of": ["$", "discount", "ytd", "year"],
        "must_not_contain": ["don't have", "unable to"],
    },

    # ── Trends ──────────────────────────────────────────────────────────────
    "Q5": {
        "text":     "How did promo redemptions in March compare to February?",
        "category": "Trends",
        "expect_numbers": True,
        "must_contain_one_of": ["month", "compared", "vs", "change"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q6": {
        "text":     "Is my promo activity trending up or down over the last 6 months?",
        "category": "Trends",
        "expect_numbers": True,
        "must_contain_one_of": ["trend", "up", "down", "growing", "increasing", "decreasing"],
        "period_keywords": ["6 months", "trend"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q7": {
        "text":     "Which month in the last 6 months had the most promo redemptions?",
        "category": "Trends",
        "expect_numbers": True,
        "must_contain_one_of": ["march", "best", "highest"],
        "period_keywords": ["march", "2026"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q8": {
        "text":     "How does this quarter's promo activity compare to last quarter?",
        "category": "Trends",
        # Window starts Nov 2025 — Q4 2025 baseline is incomplete, so honest
        # "insufficient data" is acceptable here.
        "expect_numbers": False,
        "must_contain_one_of": ["quarter", "q1", "q4", "insufficient", "data"],
        "any_keyword": True,
        "must_not_contain": [],
    },

    # ── Code-Level Rankings ─────────────────────────────────────────────────
    "Q9": {
        "text":     "Which promo code was redeemed the most last month?",
        "category": "Rankings",
        "expect_numbers": True,
        "must_contain_one_of": ["dm8880", "DM8880"],    # story anchor: top code Mar
        "period_keywords": ["march", "last month"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q10": {
        "text":     "Which promo code gave the most total discount over the last 6 months?",
        "category": "Rankings",
        "expect_numbers": True,
        "must_contain_one_of": ["dm8880", "DM8880"],    # story anchor: top by $ discount
        "must_not_contain": ["don't have", "no data"],
    },
    "Q11": {
        "text":     "Which promo code gave the biggest average discount per redemption?",
        "category": "Rankings",
        "expect_numbers": True,
        "must_contain_one_of": ["dm8880", "DM8880", "average"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q12": {
        "text":     "What percentage of visits used a promo code last month?",
        "category": "Rankings",
        "expect_numbers": True,
        "must_contain_one_of": ["%", "percent", "visit"],
        "period_keywords": ["march", "last month"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q13": {
        "text":     "Which promo code has the least usage over the last 3 months?",
        "category": "Rankings",
        "expect_numbers": True,
        "must_contain_one_of": ["pm8880", "PM8880", "least", "dropped"],
        "must_not_contain": ["don't have", "no data"],
    },

    # ── Why / Root Cause ───────────────────────────────────────────────────
    "Q14": {
        "text":     "Why did promo discounts spike last month?",
        "category": "Why / Root Cause",
        "expect_numbers": True,
        "must_contain_one_of": ["dm8880", "DM8880", "increase", "spike", "jumped"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q15": {
        "text":     "Which promo codes dropped in usage recently?",
        "category": "Why / Root Cause",
        "expect_numbers": True,
        "must_contain_one_of": ["pm8880", "PM8880", "dropped", "decrease", "decline"],
        "must_not_contain": ["don't have", "no data"],
    },

    # ── Advice ──────────────────────────────────────────────────────────────
    "Q16": {
        "text":     "Which promo codes should I consider retiring?",
        "category": "Advice",
        # Advice questions may or may not route to RAG. If they do, they
        # should call out DM881 (dormant) and POFL99 (active-but-expired).
        "expect_numbers": False,
        "must_contain_one_of": [
            "dm881", "DM881", "pofl99", "POFL99",
            "dormant", "expired", "retire", "deactivate",
        ],
        "any_keyword": True,
        "must_not_contain": [],
    },
    "Q17": {
        "text":     "Are my promo codes effective — should I keep running promotions?",
        "category": "Advice",
        "expect_numbers": False,
        "must_contain_one_of": [
            "redemption", "discount", "promo", "effective", "performance",
        ],
        "any_keyword": True,
        "must_not_contain": [],
    },

    # ── Location ────────────────────────────────────────────────────────────
    "Q18": {
        "text":     "How many promo redemptions happened at each branch last month?",
        "category": "Location",
        "expect_numbers": True,
        "must_contain_one_of": ["main st", "westside", "Main", "Westside", "location", "branch"],
        "period_keywords": ["march", "last month"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q19": {
        "text":     "Which branch redeemed the most coupons last month?",
        "category": "Location",
        "expect_numbers": True,
        # Story: Westside redeems MORE codes by count
        "must_contain_one_of": ["westside", "Westside"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q20": {
        "text":     "Show me which specific promo codes are most redeemed at Main St versus Westside.",
        "category": "Location",
        "expect_numbers": False,
        "must_contain_one_of": [
            "dm8880", "pm8880", "awan",
            "main st", "westside", "location", "branch",
        ],
        "any_keyword": True,
        "must_not_contain": ["don't have", "no data"],
    },
    "Q21": {
        "text":     "Which location gave the most total discount last month?",
        "category": "Location",
        "expect_numbers": True,
        # Story: Main St gives more total DISCOUNT (while Westside gets more
        # COUNT) — two different answers exercises the count-vs-amount split
        "must_contain_one_of": ["main st", "Main St", "Main"],
        "must_not_contain": ["don't have", "no data"],
    },

    # ── Catalog Health ─────────────────────────────────────────────────────
    "Q22": {
        "text":     "Are there any dormant promo codes that haven't been used recently?",
        "category": "Catalog Health",
        "expect_numbers": False,
        "must_contain_one_of": ["dm881", "DM881", "dormant"],
        "any_keyword": True,
        "must_not_contain": ["don't have", "no data"],
    },
    "Q23": {
        "text":     "Which promo codes are marked active but have already expired?",
        "category": "Catalog Health",
        "expect_numbers": False,
        "must_contain_one_of": ["pofl99", "POFL99", "expired"],
        "any_keyword": True,
        "must_not_contain": ["don't have", "no data"],
    },

    # ── Edge Cases ─────────────────────────────────────────────────────────
    "Q24": {
        "text":     "Are there any promo redemptions referencing promo codes that don't exist in my catalog?",
        "category": "Edge Cases",
        "expect_numbers": True,
        # Story: one Mar 2026 visit references promo_id=999 which is NOT in
        # tbl_promo. The system should surface it as "unknown promo (ID #999)"
        "must_contain_one_of": [
            "999", "unknown", "orphan", "missing", "not in", "catalog",
        ],
        "any_keyword": True,
        "must_not_contain": ["don't have", "no data", "unable to"],
    },
    "Q25": {
        "text":     "What is the average discount per redemption last month?",
        "category": "Edge Cases",
        "expect_numbers": True,
        "must_contain_one_of": ["$", "average", "per redemption"],
        "period_keywords": ["march", "last month"],
        "must_not_contain": ["don't have", "no data"],
    },
    "Q26": {
        "text":     "Show me the trend of promo usage over the last 6 months.",
        "category": "Edge Cases",
        "expect_numbers": True,
        "must_contain_one_of": ["trend", "month", "increase", "decrease", "growing"],
        "period_keywords": ["6 months", "trend"],
        "must_not_contain": ["don't have", "no data"],
    },
}


# ─── Result dataclass ────────────────────────────────────────────────────────

@dataclass
class QResult:
    qid: str
    question: str
    category: str
    answer: str
    route: str | None
    confidence: float | None
    sources: list[str]
    latency_ms: float
    # check outcomes
    non_empty: bool = False
    has_number: bool = True       # default True if expect_numbers=False
    contains_keyword: bool = True
    period_mentioned: bool = True
    no_refusal: bool = True
    # final
    passed: bool = False
    issues: list[str] = field(default_factory=list)


# ─── Chat call ───────────────────────────────────────────────────────────────

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
            resp = await client.post(
                CHAT_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json()
            # Retry on 5xx only
            if 500 <= resp.status_code < 600 and attempt < MAX_RETRIES:
                await asyncio.sleep(1.5)
                continue
            return {
                "answer": f"HTTP {resp.status_code}: {resp.text[:200]}",
                "route": None, "confidence": None, "sources": [],
            }
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_err = e
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1.5)
                continue
            return {
                "answer": f"Transport error: {e!r}",
                "route": None, "confidence": None, "sources": [],
            }
    return {
        "answer": f"Exhausted retries: {last_err!r}",
        "route": None, "confidence": None, "sources": [],
    }


# ─── Score a single result ──────────────────────────────────────────────────

def score(qid: str, spec: dict, result: dict, latency_ms: float) -> QResult:
    answer = (result.get("answer") or "").strip()
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

    # ── 1. Non-empty answer
    r.non_empty = bool(answer) and not answer.startswith(("HTTP ", "Transport", "Exhausted"))
    if not r.non_empty:
        r.issues.append("empty_or_http_error")

    # ── 2. has_number
    if spec.get("expect_numbers", False):
        r.has_number = bool(NUMBER_RE.search(answer))
        if not r.has_number:
            r.issues.append("missing_number")

    # ── 3. contains_keyword
    expect = spec.get("must_contain_one_of") or []
    if expect:
        hits = [kw for kw in expect if kw.lower() in answer_lower]
        r.contains_keyword = bool(hits)
        if not r.contains_keyword:
            r.issues.append(f"no_keyword({','.join(expect[:3])}...)")

    # ── 4. period_keywords (optional)
    period = spec.get("period_keywords") or []
    if period:
        hits = [p for p in period if p.lower() in answer_lower]
        r.period_mentioned = bool(hits)
        if not r.period_mentioned:
            r.issues.append(f"no_period({','.join(period[:2])})")

    # ── 5. must_not_contain (refusal detection)
    forbidden = spec.get("must_not_contain") or []
    leaked = [f for f in forbidden if f.lower() in answer_lower]
    r.no_refusal = not leaked
    if leaked:
        r.issues.append(f"forbidden({','.join(leaked)})")

    # Final pass = all checks green
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
        f"  {mark} [{r.qid:>4}] {r.category:<18} "
        f"empty={'✓' if r.non_empty else '✗'} "
        f"num={'✓' if r.has_number else '✗'} "
        f"kw={'✓' if r.contains_keyword else '✗'} "
        f"period={'✓' if r.period_mentioned else '✗'} "
        f"clean={'✓' if r.no_refusal else '✗'} "
        f"[{r.latency_ms:>5.0f}ms]"
    )
    if not r.passed:
        preview = r.answer.replace("\n", " ")[:160]
        print(f"         Q: {r.question}")
        print(f"         A: {preview}...")
        print(f"         Issues: {issues}")


def print_summary(results: list[QResult]) -> None:
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    print("\n" + "═" * 78)
    print(f"  Step 6 Results — Promos Domain")
    print("═" * 78)
    print(f"  Overall : {passed}/{total} passed ({100*passed//max(total,1)}%)")
    print(f"  Time    : {sum(r.latency_ms for r in results)/1000:.1f}s total")

    # By category
    by_cat: dict[str, list[QResult]] = {}
    for r in results:
        by_cat.setdefault(r.category, []).append(r)
    print(f"  By category:")
    for cat, rs in by_cat.items():
        p = sum(1 for r in rs if r.passed)
        bar = "█" * p + "░" * (len(rs) - p)
        print(f"    {cat:<20} {bar}  {p}/{len(rs)}")

    # Failures
    fails = [r for r in results if not r.passed]
    if fails:
        print(f"\n  ── Failed ({len(fails)}) ─────────────────────────────────")
        for r in fails:
            print(f"    ❌ [{r.qid}] {r.question}")
            if r.issues:
                print(f"         Issues: {'; '.join(r.issues)}")

    print("═" * 78)
    if passed == total:
        print(f"  ✅ STEP 6 PASSED — all {total} questions answered correctly")
    else:
        print(f"  ❌ STEP 6 INCOMPLETE — {total - passed} failing")
    print("═" * 78)


# ─── Main ──────────────────────────────────────────────────────────────────

async def run_question(client, qid, spec, org_id) -> QResult:
    print(f"  → [{qid}] {spec['text'][:70]}...")
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
    p = argparse.ArgumentParser(description="Step 6 — Promos domain test harness")
    p.add_argument("--question", help="Run a single question (e.g. Q9)")
    p.add_argument("--output", help="Write results as JSON to this path")
    p.add_argument("--org-id", type=int, default=ORG_ID, help="Tenant ID")
    p.add_argument("--endpoint", default=CHAT_ENDPOINT)
    args = p.parse_args()

    CHAT_ENDPOINT = args.endpoint
    asyncio.run(main(args))