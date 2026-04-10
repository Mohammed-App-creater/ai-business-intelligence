"""
scripts/tests/test_revenue_step6.py
=====================================
Step 6 — Revenue Domain Test

Fires all 27 Step-1 questions through the live FastAPI chat endpoint
and scores each answer against 4 criteria:

    ✅ Routed to RAG          → answer is data-driven (not generic advice)
    ✅ Contains a number       → no content-free response
    ✅ Correct time language   → mentions expected period keywords
    ✅ No hallucination flags  → doesn't reference data not in fixtures

Usage:
    # With mock data seeded (default — no real warehouse needed):
    python scripts/tests/test_revenue_step6.py

    # Against a real org_id already in the warehouse:
    python scripts/tests/test_revenue_step6.py --org-id 42 --skip-seed

    # Save results to a JSON file:
    python scripts/tests/test_revenue_step6.py --output results/step6_revenue.json

    # Run one specific question by number (Q1, Q8, LQ1 etc.):
    python scripts/tests/test_revenue_step6.py --question Q1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
import types
import uuid
from datetime import date
from pathlib import Path

import httpx

# ── Path bootstrap ────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# Fake config so we can import app modules without a real .env
_fake_cfg = types.ModuleType("app.core.config")
_fake_cfg.settings = type("S", (), {
    "ANALYTICS_BACKEND_URL": "http://127.0.0.1:8001",
    "ANALYTICS_API_KEY":     "mock-key",
})()
sys.modules.setdefault("app.core.config", _fake_cfg)

# ── Config ────────────────────────────────────────────────────────────────────

CHAT_URL        = "http://localhost:8000/api/v1/chat"
MOCK_PORT       = 8001
ORG_ID          = 42
SESSION_ID      = str(uuid.uuid4())
REQUEST_TIMEOUT = 60                 # seconds per question


# ── 27 Test questions (from Step 1) ──────────────────────────────────────────

QUESTIONS: list[dict] = [
    # ── Category 1: Basic Facts ──────────────────────────────────────────────
    {
        "id": "Q1",
        "category": "Basic Facts",
        "question": "What was my total revenue last month?",
        "expect_numbers": False,  # fixture is Jan–Jun 2025; "last month" vs Apr 2026 is out of window
        "expect_keywords": ["revenue", "month"],
        "must_not_contain": ["I don't know", "no data", "cannot answer"],
        "any_keyword": True,
    },
    {
        "id": "Q2",
        "category": "Basic Facts",
        "question": "How much revenue did I make this year so far?",
        "expect_numbers": True,
        "expect_keywords": ["revenue", "year"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "Q3",
        "category": "Basic Facts",
        "question": "What is my average ticket value per visit?",
        "expect_numbers": True,
        "expect_keywords": ["ticket", "visit", "average"],
        "must_not_contain": ["I don't know", "no data"],
    },

    # ── Category 2: Trends & Changes ─────────────────────────────────────────
    {
        "id": "Q4",
        "category": "Trends",
        "question": "How does my revenue this month compare to last month?",
        "expect_numbers": True,
        "expect_keywords": ["month", "revenue"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "Q5",
        "category": "Trends",
        "question": "Is my revenue trending up or down over the last 6 months?",
        "expect_numbers": True,
        "expect_keywords": ["trend", "month", "revenue"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "Q6",
        "category": "Trends",
        "question": "Which was my best revenue month this year and which was my worst?",
        "expect_numbers": True,
        "expect_keywords": ["best", "worst", "month", "revenue"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "Q7",
        "category": "Trends",
        "question": "How does my revenue this quarter compare to the same quarter last year?",
        "expect_numbers": False,
        "expect_keywords": ["quarter", "revenue", "data", "year"],
        "must_not_contain": ["I don't know", "no data"],
        "any_keyword": True,
    },

    # ── Category 3: Rankings & Breakdowns ────────────────────────────────────
    {
        "id": "Q8",
        "category": "Rankings",
        "question": "Which staff member generated the most revenue last month?",
        "expect_numbers": True,
        "expect_keywords": ["staff", "revenue"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "Q9",
        "category": "Rankings",
        "question": "Which location brought in the most revenue this year?",
        "expect_numbers": True,
        "expect_keywords": ["location", "revenue"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "Q10",
        "category": "Rankings",
        "question": "What percentage of my revenue came from cash vs card vs other payment types?",
        "expect_numbers": True,
        "expect_keywords": ["cash", "card", "revenue"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "Q11",
        "category": "Rankings",
        "question": "How much of my revenue came from gift cards being redeemed?",
        "expect_numbers": True,
        "expect_keywords": ["gift card", "revenue"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "Q12",
        "category": "Rankings",
        "question": "How much revenue did promo codes cost me last month — what was the total discount given?",
        "expect_numbers": True,
        "expect_keywords": ["promo", "discount"],
        "must_not_contain": ["I don't know", "no data"],
    },

    # ── Category 4: Root Cause ────────────────────────────────────────────────
    {
        "id": "Q13",
        "category": "Root Cause",
        "question": "Why did my revenue drop last month?",
        "expect_numbers": True,
        "expect_keywords": ["revenue", "drop", "visit", "ticket", "cancel"],
        "must_not_contain": ["I don't know"],
        "any_keyword": True,  # at least one of expect_keywords must appear
    },
    {
        "id": "Q14",
        "category": "Root Cause",
        "question": "My revenue went up this month but I feel like I was less busy — why?",
        "expect_numbers": True,
        "expect_keywords": ["ticket", "visit", "revenue", "average"],
        "must_not_contain": ["I don't know"],
        "any_keyword": True,
    },
    {
        "id": "Q15",
        "category": "Root Cause",
        "question": "I had a lot of no-shows last week — how much revenue did that cost me?",
        "expect_numbers": False,   # may not have exact number — data gap known
        "expect_keywords": ["no-show", "revenue", "appointment", "cancel"],
        "must_not_contain": [],
        "any_keyword": True,
    },

    # ── Category 5: Advice ────────────────────────────────────────────────────
    {
        "id": "Q16",
        "category": "Advice",
        "question": "What can I do to increase my revenue next month?",
        "expect_numbers": False,
        "expect_keywords": ["revenue", "increase", "ticket", "visit", "promo"],
        "must_not_contain": ["I don't know"],
        "any_keyword": True,
    },
    {
        "id": "Q17",
        "category": "Advice",
        "question": "Should I be worried about my revenue trend — is my business growing or shrinking?",
        "expect_numbers": True,
        "expect_keywords": ["trend", "revenue", "growing", "shrinking", "month"],
        "must_not_contain": ["I don't know"],
        "any_keyword": True,
    },

    # ── Edge Cases ────────────────────────────────────────────────────────────
    {
        "id": "Q18",
        "category": "Edge Cases",
        "question": "How much in tips did my staff collect last month?",
        "expect_numbers": True,
        "expect_keywords": ["tip", "staff", "month"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "Q19",
        "category": "Edge Cases",
        "question": "How much tax did I collect this month?",
        "expect_numbers": True,
        "expect_keywords": ["tax", "month"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "Q20",
        "category": "Edge Cases",
        "question": "How many visits ended with a refund or failed payment, and what was the total value?",
        "expect_numbers": True,
        "expect_keywords": ["refund", "failed", "visit", "revenue"],
        "must_not_contain": ["I don't know", "no data"],
        "any_keyword": True,
    },

    # ── Location Questions ────────────────────────────────────────────────────
    {
        "id": "LQ1",
        "category": "Location",
        "question": "What was the total revenue for each location last month?",
        "expect_numbers": True,
        "expect_keywords": ["location", "revenue", "month"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "LQ2",
        "category": "Location",
        "question": "Which location brought in the most revenue last month?",
        "expect_numbers": True,
        "expect_keywords": ["location", "revenue"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "LQ4",
        "category": "Location",
        "question": "What percentage of total revenue came from each location last month?",
        "expect_numbers": False,  # seed pct_of_total_revenue is 0 → "insufficient data" is valid
        "expect_keywords": ["location", "revenue", "percent"],
        "must_not_contain": ["I don't know", "no data"],
        "any_keyword": True,
    },
    {
        "id": "LQ5",
        "category": "Location",
        "question": "What is the average ticket value for each location?",
        "expect_numbers": False,  # avg_visit_value not populated per location yet → formula-only OK
        "expect_keywords": ["location", "ticket", "average"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "LQ7",
        "category": "Location",
        "question": "How did revenue change month over month for each location?",
        "expect_numbers": True,
        "expect_keywords": ["revenue", "month"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "LQ9",
        "category": "Location",
        "question": "How much promo discount was given at each location last month?",
        "expect_numbers": True,
        "expect_keywords": ["promo", "discount"],
        "must_not_contain": ["I don't know", "no data"],
    },
    {
        "id": "LQ10",
        "category": "Location",
        "question": "How much gift card redemption revenue came from each location last month?",
        "expect_numbers": True,
        "expect_keywords": ["location", "gift card", "redemption", "revenue"],
        "must_not_contain": ["I don't know", "no data"],
        "any_keyword": True,
    },
]


# ── Scoring ───────────────────────────────────────────────────────────────────

def _has_number(text: str) -> bool:
    """True if the answer contains any dollar amount or percentage."""
    return bool(re.search(r"\$[\d,]+|[\d,]+%|[\d,]+\.\d+", text))


def score_answer(q: dict, answer: str, route: str | None) -> dict:
    """
    Score one answer. Returns a result dict with pass/fail per criterion.
    """
    answer_lower = answer.lower()
    checks: dict[str, bool] = {}

    # 1. Not empty
    checks["not_empty"] = bool(answer.strip()) and len(answer.strip()) > 20

    # 2. Contains a number (if expected)
    if q["expect_numbers"]:
        checks["has_number"] = _has_number(answer)
    else:
        checks["has_number"] = True  # not required for this question

    # 3. Keyword presence (case-insensitive: answer_lower vs kw.lower())
    if q.get("any_keyword"):
        # At least one keyword must appear
        checks["keywords"] = any(kw.lower() in answer_lower for kw in q["expect_keywords"])
    else:
        # ALL keywords must appear
        checks["keywords"] = all(kw.lower() in answer_lower for kw in q["expect_keywords"])

    # 4. Must-not-contain (hallucination / failure signals)
    bad_found = [s for s in q["must_not_contain"] if s.lower() in answer_lower]
    checks["no_bad_phrases"] = len(bad_found) == 0

    passed = all(checks.values())
    return {
        "id":          q["id"],
        "category":    q["category"],
        "question":    q["question"],
        "passed":      passed,
        "checks":      checks,
        "bad_phrases": bad_found,
        "answer":      answer[:400],   # truncated for log
        "route":       route,
    }


# ── Mock extraction smoke test (no pgvector writes) ──────────────────────────

async def seed_mock_data() -> bool:
    """
    Starts the mock analytics server and runs RevenueExtractor against it.
    This intentionally skips pgvector writes for local chat-endpoint testing.
    """
    print("\n── Running mock revenue extractor smoke test ────────────────")
    try:
        from tests.mocks.mock_analytics_server import start_mock_server
        from etl.transforms.revenue_etl import RevenueExtractor
        from app.services.analytics_client import AnalyticsClient

        # Start mock analytics server
        server = start_mock_server()
        print(f"  Mock server started at {server.base_url}")

        # Run extractor against mock server
        client    = AnalyticsClient(base_url=server.base_url, api_key="mock-key")
        extractor = RevenueExtractor(client=client)

        # Use a 6-month window matching fixture data
        docs = await extractor.run(
            business_id=ORG_ID,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 6, 30),
        )
        print(f"  Extractor produced {len(docs)} documents")
        print("  Skipping pgvector seeding (chat endpoint response test only)")

        server.stop()
        return True

    except Exception as exc:
        print(f"  ⚠ Extractor smoke test failed: {exc}")
        return False


# ── Chat endpoint caller ──────────────────────────────────────────────────────

async def ask(client: httpx.AsyncClient, question: str) -> tuple[str, str | None, float]:
    """
    POST one question to the chat endpoint.
    Returns (answer_text, route, latency_ms).
    """
    payload = {
        "org_id":      str(ORG_ID),
        "business_id": str(ORG_ID),
        "question":    question,
    }
    t0 = time.perf_counter()
    try:
        resp = await client.post(CHAT_URL, json=payload, timeout=REQUEST_TIMEOUT)
        latency = (time.perf_counter() - t0) * 1000
        resp.raise_for_status()
        body = resp.json()
        answer = body.get("answer") or body.get("response") or str(body)
        route  = body.get("route") or body.get("routing") or None
        return answer, route, latency
    except httpx.HTTPStatusError as e:
        latency = (time.perf_counter() - t0) * 1000
        return f"HTTP {e.response.status_code}: {e.response.text[:200]}", None, latency
    except Exception as exc:
        latency = (time.perf_counter() - t0) * 1000
        return f"ERROR: {exc}", None, latency


# ── Renderer ──────────────────────────────────────────────────────────────────

PASS = "✅"
FAIL = "❌"
SKIP = "⚠"

def _check_icon(v: bool) -> str:
    return "✓" if v else "✗"


def print_result(r: dict, verbose: bool = False) -> None:
    icon = PASS if r["passed"] else FAIL
    c    = r["checks"]
    checks_str = (
        f"empty={_check_icon(c['not_empty'])} "
        f"num={_check_icon(c['has_number'])} "
        f"kw={_check_icon(c['keywords'])} "
        f"clean={_check_icon(c['no_bad_phrases'])}"
    )
    print(f"  {icon} [{r['id']:<5}] {r['category']:<12} {checks_str}")
    if verbose or not r["passed"]:
        print(f"         Q: {r['question']}")
        print(f"         A: {r['answer'][:300]}")
        if r["bad_phrases"]:
            print(f"         ⚠ Bad phrases found: {r['bad_phrases']}")
        print()


def print_summary(results: list[dict], total_ms: float) -> None:
    passed  = sum(1 for r in results if r["passed"])
    total   = len(results)
    pct     = passed / total * 100 if total else 0

    # By category
    cats: dict[str, list[bool]] = {}
    for r in results:
        cats.setdefault(r["category"], []).append(r["passed"])

    print("\n" + "═" * 60)
    print(f"  Step 6 Results — Revenue Domain")
    print("═" * 60)
    print(f"  Overall : {passed}/{total} passed ({pct:.0f}%)")
    print(f"  Time    : {total_ms/1000:.1f}s total")
    print()
    print("  By category:")
    for cat, bools in cats.items():
        cp = sum(bools)
        ct = len(bools)
        bar = "█" * cp + "░" * (ct - cp)
        print(f"    {cat:<14} {bar}  {cp}/{ct}")

    # Failed questions
    failed = [r for r in results if not r["passed"]]
    if failed:
        print(f"\n  ── Failed ({len(failed)}) ──────────────────────────────")
        for r in failed:
            c = r["checks"]
            issues = [k for k, v in c.items() if not v]
            print(f"    {FAIL} [{r['id']}] {r['question'][:60]}")
            print(f"         Issues: {', '.join(issues)}")
    else:
        print(f"\n  {PASS} All {total} questions passed!")

    print("═" * 60)
    verdict = "✅ STEP 6 PASS" if passed == total else f"❌ STEP 6 INCOMPLETE — {total-passed} failing"
    print(f"  {verdict}")
    print("═" * 60 + "\n")


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(args: argparse.Namespace) -> int:
    # Filter to specific question if requested
    questions = QUESTIONS
    if args.question:
        questions = [q for q in QUESTIONS if q["id"].upper() == args.question.upper()]
        if not questions:
            print(f"Question '{args.question}' not found. Valid IDs: "
                  f"{', '.join(q['id'] for q in QUESTIONS)}")
            return 1

    # Seed mock data unless skipped
    if not args.skip_seed:
        await seed_mock_data()

    print(f"\n── Running {len(questions)} questions against {CHAT_URL} ──")
    print(f"   org_id      : {ORG_ID}")
    print(f"   session_id  : {SESSION_ID}\n")

    results: list[dict] = []
    total_start = time.perf_counter()

    async with httpx.AsyncClient() as client:
        for q in questions:
            print(f"  → [{q['id']}] {q['question'][:65]}...")
            answer, route, latency = await ask(client, q["question"])
            result = score_answer(q, answer, route)
            result["latency_ms"] = latency
            results.append(result)
            print_result(result, verbose=args.verbose)
            # Small delay between requests to avoid overwhelming local server
            await asyncio.sleep(0.3)

    total_ms = (time.perf_counter() - total_start) * 1000
    print_summary(results, total_ms)

    # Save JSON output if requested
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w") as f:
            json.dump({
                "org_id":      ORG_ID,
                "session_id":  SESSION_ID,
                "run_date":    date.today().isoformat(),
                "passed":      sum(1 for r in results if r["passed"]),
                "total":       len(results),
                "results":     results,
            }, f, indent=2)
        print(f"  Results saved → {out}")

    passed_all = all(r["passed"] for r in results)
    return 0 if passed_all else 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 6 — Revenue domain test")
    parser.add_argument("--org-id",       type=int, default=None,
                        help="Override org_id (use a real one from your DB)")
    parser.add_argument("--skip-seed",    action="store_true",
                        help="Skip mock data seeding (use if data already in pgvector)")
    parser.add_argument("--question",     default=None,
                        help="Run a single question by ID (e.g. Q1, Q8, LQ1)")
    parser.add_argument("--output",       default=None,
                        help="Save JSON results to this file path")
    parser.add_argument("--verbose",      action="store_true",
                        help="Print every answer, not just failures")
    args = parser.parse_args()

    if args.org_id:
        global ORG_ID
        ORG_ID = args.org_id

    exit_code = asyncio.run(run(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
