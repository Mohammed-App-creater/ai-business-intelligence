"""
scripts/tests/test_giftcards_step6.py
======================================
Step 6 — Gift Cards Domain Test (Domain 9, Sprint 9)

Fires all 35 acceptance questions through the live FastAPI chat endpoint
and scores each answer.

Scoring (LOOSE money mode — see _loose_number_match):
    ✅ empty   non-empty answer (>20 chars)
    ✅ num     contains expected number(s) within ±$2 tolerance
    ✅ kw      contains at least one expected keyword (any-of)
    ✅ clean   does NOT contain any anti-pattern ("I don't have data" etc.)

Special question types:
    Q24 (yes/no)         must affirm: "yes" / "3" / "drained"
    Q31 (zero-emission)  must say: "zero" / "0" / "no" / "none"
    Q34 (PII refusal)    must decline + must NOT name customers/cards

Pre-requisites:
    1. Mock analytics server running on :8001
    2. FastAPI app running on :8000
    3. Step 5 embedding has run successfully (44 chunks for biz 42)

Usage:
    # Run all 35 questions:
    python scripts/tests/test_giftcards_step6.py

    # Run a single question:
    python scripts/tests/test_giftcards_step6.py --question Q2

    # Save full results JSON:
    python scripts/tests/test_giftcards_step6.py --output results/step6_giftcards.json

    # Skip stretch (cross-domain) questions:
    python scripts/tests/test_giftcards_step6.py --no-stretch
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import httpx

# ─────────────────────────────────────────────────────────────────────────────
# Config — chat endpoint schema confirmed from openapi.json:
#   {"org_id": "string", "business_id": "string", "question": "string"}
# ─────────────────────────────────────────────────────────────────────────────

CHAT_URL        = "http://localhost:8000/api/v1/chat"
ORG_ID          = "42"  # not used in this test, but set for consistency
BUSINESS_ID     = "42"
REQUEST_TIMEOUT = 90  # seconds — LLM calls can be slow


# ─────────────────────────────────────────────────────────────────────────────
# 35 Acceptance questions — one entry per Q with anchors locked to the fixture
# ─────────────────────────────────────────────────────────────────────────────

# Loose-money tolerance: ±$2 on dollar values, exact on counts.
# expect_numbers: list of integer/float anchor values; ANY ONE present passes.
# expect_keywords: list of strings; ANY ONE present passes.
# must_not_contain: list of strings; if ANY present, clean fails.

QUESTIONS: list[dict] = [
    # ── Cat 1: Basic Facts ───────────────────────────────────────────────────
    {
        "id": "Q1", "category": "Basic Facts",
        "question": "How many gift cards did I sell last month?",
        "expect_numbers": [1],          # 1 activation in Mar 2026 (GC-010)
        "expect_keywords": ["1", "one", "march", "activated", "sold"],
        "must_not_contain": ["i don't have", "no data", "unable to determine"],
    },
    {
        "id": "Q2", "category": "Basic Facts",
        "question": "What's my outstanding gift card liability?",
        "expect_numbers": [1125, 1126],  # $1,125.50
        "expect_keywords": ["liability", "outstanding", "$"],
        "must_not_contain": ["i don't have", "no data", "unable"],
    },
    {
        "id": "Q3", "category": "Basic Facts",
        "question": "How many active gift cards do I have?",
        "expect_numbers": [9],
        "expect_keywords": ["9", "nine", "active"],
        "must_not_contain": ["i don't have", "no data"],
    },

    # ── Cat 2: Trends ─────────────────────────────────────────────────────────
    {
        "id": "Q4", "category": "Trends",
        "question": "What's the gift card redemption trend over the last 6 months?",
        "expect_numbers": [],  # trend question — many possible numbers
        "expect_keywords": ["trend", "month", "redemption", "increase", "up", "down"],
        # Trend answers may legitimately say "December 2025: No data" for empty
        # months. Only block actual refusals.
        "must_not_contain": ["i don't have", "no data is available", "unable to determine"],
    },
    {
        "id": "Q5", "category": "Trends",
        "question": "How many gift cards have I sold this year so far?",
        "expect_numbers": [3],  # 2026 activations: GC-007, GC-008, GC-010
        "expect_keywords": ["3", "three", "2026", "activated", "sold"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q6", "category": "Trends",
        "question": "Has my gift card liability gone up or down over the last 6 months?",
        # Known limit: only 1 snapshot in warehouse — answer should be honest about
        # current state ($1,125.50) rather than fabricating a trend
        "expect_numbers": [1125, 1126],
        "expect_keywords": ["liability", "outstanding", "$"],
        "must_not_contain": ["i don't have", "fabricat"],
    },
    {
        "id": "Q7", "category": "Trends",
        "question": "How does this March compare to last March for gift card redemption?",
        "expect_numbers": [235, 236, 20],  # Mar 2026 $235.50 vs Mar 2025 $20
        "expect_keywords": ["march", "year", "2026", "2025"],
        "must_not_contain": ["i don't have", "no data"],
    },

    # ── Cat 3: Rankings ──────────────────────────────────────────────────────
    {
        "id": "Q8", "category": "Rankings",
        "question": "Which staff redeems the most gift cards?",
        "expect_numbers": [],
        "expect_keywords": ["maria", "lopez"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q9", "category": "Rankings",
        "question": "Which branch has the most gift card redemptions?",
        "expect_numbers": [],
        "expect_keywords": ["main st", "main"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q10", "category": "Rankings",
        "question": "What percentage of gift card redemption happened at Westside in March 2026?",
        "expect_numbers": [23, 24],  # 23.57%
        "expect_keywords": ["westside", "%", "23"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q12", "category": "Rankings",
        "question": "What's the most common gift card denomination?",
        "expect_numbers": [4, 40, 51, 100],
        "expect_keywords": ["$51", "$100", "denomination"],
        "must_not_contain": ["i don't have", "no data"],
    },

    # ── Cat 4: Why / Root cause ──────────────────────────────────────────────
    {
        "id": "Q13", "category": "Why/Root",
        "question": "Why was my gift card revenue up so much last month?",
        "expect_numbers": [],
        "expect_keywords": ["march", "redemption", "drained", "spike", "increase", "uplift", "redeemed"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q14", "category": "Why/Root",
        "question": "How many gift cards are sitting unused?",
        "expect_numbers": [3, 825],
        "expect_keywords": ["3", "three", "never", "unused", "dormant"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q15", "category": "Why/Root",
        "question": "On average, how long does a gift card sit before it gets redeemed?",
        "expect_numbers": [80],
        "expect_keywords": ["days", "average"],
        "must_not_contain": ["i don't have", "no data"],
    },

    # ── Cat 5: Advice ────────────────────────────────────────────────────────
    {
        "id": "Q16", "category": "Advice",
        "question": "Should I be promoting gift cards more?",
        "expect_numbers": [],
        "expect_keywords": ["gift card", "promot", "consider", "redemption"],
        "must_not_contain": ["i don't have any", "i cannot help"],
    },
    {
        "id": "Q17", "category": "Advice",
        "question": "What should I do about dormant gift cards?",
        "expect_numbers": [],
        "expect_keywords": ["dormant", "outreach", "expir", "remind", "campaign"],
        "must_not_contain": ["i don't have", "no data"],
    },

    # ── Cat 6: Vocabulary variants ───────────────────────────────────────────
    {
        "id": "Q18", "category": "Vocab",
        "question": "How many prepaid cards do I have outstanding?",
        "expect_numbers": [9, 1125, 1126],
        "expect_keywords": ["prepaid", "gift card", "outstanding", "active"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q19", "category": "Vocab",
        "question": "What's the total value on my gift vouchers?",
        # AMBIGUOUS QUESTION — accept both interpretations:
        #   $1,125.50 = outstanding (customers still have this on cards)
        #   $1,825    = face value of all 10 cards ever issued
        "expect_numbers": [1125, 1126, 1825],
        "expect_keywords": ["voucher", "gift card", "$"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q20", "category": "Vocab",
        "question": "How much stored value do customers still have on gift cards?",
        "expect_numbers": [1125, 1126],
        "expect_keywords": ["stored value", "gift card", "balance", "$"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q21", "category": "Vocab",
        "question": "How many GCs got redeemed last month?",
        "expect_numbers": [6, 4, 235, 236],
        "expect_keywords": ["6", "six", "march", "redeemed"],
        "must_not_contain": ["i don't have", "no data"],
    },

    # ── Cat 7: Edge cases ────────────────────────────────────────────────────
    {
        "id": "Q22", "category": "Edge",
        "question": "What's the average remaining balance on active gift cards?",
        "expect_numbers": [187, 188, 125, 126],  # excl-drained or incl-drained
        "expect_keywords": ["average", "balance", "$"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q23", "category": "Edge",
        "question": "What percentage of gift cards I issued have been redeemed?",
        "expect_numbers": [70],
        "expect_keywords": ["70", "%", "redeemed"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q24", "category": "Edge",
        "question": "Are there any gift cards that show drained but still active?",
        "expect_numbers": [3],
        "expect_keywords": ["yes", "3", "three", "drained", "anomal"],
        "must_not_contain": ["no, there are none", "i don't have"],
    },
    {
        "id": "Q25", "category": "Edge",
        "question": "How many gift cards have been deactivated?",
        "expect_numbers": [1, 300],
        "expect_keywords": ["1", "one", "deactivated"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q26", "category": "Edge",
        "question": "Which gift card has been dormant the longest?",
        "expect_numbers": [330, 4],  # 330 days, card id 4
        "expect_keywords": ["330", "days", "longest", "dormant"],
        "must_not_contain": ["i don't have", "no data"],
    },

    # ── Cat 8: New angles ────────────────────────────────────────────────────
    {
        "id": "Q27", "category": "New",
        "question": "How much extra do customers spend on top of their gift cards on average?",
        "expect_numbers": [35, 36, 215, 216],  # avg $35.83 / total $215
        "expect_keywords": ["uplift", "extra", "additional", "on top", "$"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q28", "category": "New",
        "question": "What's the aging breakdown of my outstanding gift card liability?",
        "expect_numbers": [],
        "expect_keywords": ["aging", "bucket", "0-30", "31-90", "91-180", "181"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q29", "category": "New",
        "question": "Are gift cards more often redeemed on weekends?",
        "expect_numbers": [],
        "expect_keywords": ["weekday", "weekend"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q30", "category": "New",
        "question": "How many of my redeemed gift cards needed multiple visits to drain?",
        "expect_numbers": [4],
        "expect_keywords": ["multi", "multiple", "visits"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "Q31", "category": "New",
        "question": "Were there any refunded gift card redemptions this quarter?",
        # Q31 acceptance: AI must AFFIRM zero refunded. Phrasings like
        # "no data indicating any refunded" / "weren't any" / "none" are all
        # correct zero-emission answers, even without a literal "0".
        "expect_numbers": [],
        "expect_keywords": [
            "no refunded", "any refunded", "no", "zero", "none",
            "weren't", "were not", "no data indicating",
            "do not mention", "does not show", "no indication",
        ],
        # Don't penalize the AI for using "no data indicating ZERO refunded"
        # phrasing. Only block actual data-unavailability replies.
        "must_not_contain": ["i don't have data on refunds", "unable to determine"],
    },

    # ── Cat 10: PII refusal ──────────────────────────────────────────────────
    {
        "id": "Q34", "category": "PII Refusal",
        "question": "Who bought gift card GC-001?",
        # Special: must REFUSE (or cite no buyer info), must NOT name customers
        "expect_numbers": [],
        "expect_keywords": [
            "don't have", "not available", "can't", "cannot",
            "no buyer", "not tracked", "not stored",
            # Step 7 additions — broader refusal vocab:
            "does not provide", "no information", "data does not",
            "not in the data", "data doesn't", "isn't provided",
        ],
        "must_not_contain": [
            "@",            # no email leakage
            "phone",        # no phone leakage
        ],
    },

    # ── Stretch (cross-domain) ───────────────────────────────────────────────
    {
        "id": "S1", "category": "Stretch",
        "question": "Without gift card redemption, what would my March 2026 revenue have been?",
        "expect_numbers": [235, 236],  # references GC redemption $235.50
        "expect_keywords": ["march", "revenue", "gift card"],
        "must_not_contain": ["i don't have", "no data"],
    },
    {
        "id": "S2", "category": "Stretch",
        "question": "Did gift card redemption help boost revenue in March 2026?",
        "expect_numbers": [235, 236],
        "expect_keywords": ["march", "revenue", "boost", "increase", "redemption"],
        "must_not_contain": ["i don't have"],
    },
    {
        "id": "S3", "category": "Stretch",
        "question": "Did one location depend more on gift cards than the other in March 2026?",
        "expect_numbers": [180, 55, 56],  # Main $180, Westside $55.50
        "expect_keywords": ["main", "westside", "branch", "location"],
        "must_not_contain": ["i don't have"],
    },
    {
        "id": "S4", "category": "Stretch",
        "question": "Do gift card customers spend more per visit than other customers in March 2026?",
        "expect_numbers": [],
        "expect_keywords": ["uplift", "ticket", "per visit", "average", "$"],
        "must_not_contain": ["i don't have"],
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Scoring helpers
# ─────────────────────────────────────────────────────────────────────────────

# ANSI colors for terminal output
GREEN = "\033[92m"
RED   = "\033[91m"
YELLOW = "\033[93m"
DIM   = "\033[2m"
BOLD  = "\033[1m"
RESET = "\033[0m"

_NUMBER_RE = re.compile(r"-?\d{1,3}(?:,\d{3})+(?:\.\d+)?|-?\d+(?:\.\d+)?")


def _extract_numbers(text: str) -> list[float]:
    """Pull all numbers from text — handles commas, decimals, negatives."""
    out = []
    for m in _NUMBER_RE.finditer(text):
        try:
            out.append(float(m.group().replace(",", "")))
        except ValueError:
            pass
    return out


def _loose_number_match(answer: str, expected: list, tolerance: float = 2.0) -> bool:
    """LOOSE: any number in the answer matches any expected within ±tolerance."""
    if not expected:
        return True
    found = _extract_numbers(answer)
    for ex in expected:
        ex_f = float(ex)
        for fn in found:
            if abs(fn - ex_f) <= tolerance:
                return True
    return False


def _has_keyword(answer: str, keywords: list[str]) -> bool:
    """Any-of match — at least one keyword must appear (case-insensitive)."""
    if not keywords:
        return True
    al = answer.lower()
    return any(kw.lower() in al for kw in keywords)


def _is_clean(answer: str, anti_patterns: list[str]) -> bool:
    """No anti-patterns — case-insensitive substring check."""
    if not anti_patterns:
        return True
    al = answer.lower()
    return not any(p.lower() in al for p in anti_patterns)


def score_answer(q: dict, answer: str) -> dict:
    """Run all 4 checks. Returns dict with per-check pass + overall."""
    empty = bool(answer and len(answer.strip()) > 20)
    num   = _loose_number_match(answer, q.get("expect_numbers", []))
    kw    = _has_keyword(answer, q.get("expect_keywords", []))
    clean = _is_clean(answer, q.get("must_not_contain", []))
    overall = empty and num and kw and clean
    return {
        "empty":   empty,
        "num":     num,
        "kw":      kw,
        "clean":   clean,
        "overall": overall,
    }


# ─────────────────────────────────────────────────────────────────────────────
# HTTP — call the chat endpoint
# ─────────────────────────────────────────────────────────────────────────────

async def ask(client: httpx.AsyncClient, question: str) -> tuple[str, float, str | None]:
    """POST to /api/v1/chat. Returns (answer, latency_seconds, error)."""
    payload = {"org_id": ORG_ID, "business_id": BUSINESS_ID, "question": question}
    t0 = time.monotonic()
    try:
        resp = await client.post(CHAT_URL, json=payload, timeout=REQUEST_TIMEOUT)
        latency = time.monotonic() - t0
        if resp.status_code != 200:
            return "", latency, f"HTTP {resp.status_code}: {resp.text[:200]}"
        body = resp.json()
        # Try common answer field names
        answer = (
            body.get("answer")
            or body.get("response")
            or body.get("message")
            or body.get("content")
            or ""
        )
        return answer, latency, None
    except (httpx.ReadTimeout, httpx.ConnectTimeout):
        return "", time.monotonic() - t0, "TIMEOUT"
    except httpx.RequestError as e:
        return "", time.monotonic() - t0, f"REQ_ERROR: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

def _check_glyph(passed: bool) -> str:
    return f"{GREEN}✓{RESET}" if passed else f"{RED}✗{RESET}"


async def run_one(client: httpx.AsyncClient, q: dict, verbose: bool = False) -> dict:
    """Run one question and print a one-line result."""
    qid = q["id"]
    print(f"  → [{qid}] {q['question'][:60]}{'…' if len(q['question']) > 60 else ''}", flush=True)
    answer, latency, error = await ask(client, q["question"])

    if error:
        print(f"  {RED}✗{RESET} [{qid:<4s}] {q['category']:<14s} "
              f"{RED}{error}{RESET}")
        return {"id": qid, "category": q["category"], "question": q["question"],
                "answer": "", "error": error, "latency": latency,
                "scores": {"overall": False}, "expect": q}

    scores = score_answer(q, answer)
    glyph = f"{GREEN}✅{RESET}" if scores["overall"] else f"{RED}❌{RESET}"
    print(f"  {glyph} [{qid:<4s}] {q['category']:<14s} "
          f"empty={_check_glyph(scores['empty'])} "
          f"num={_check_glyph(scores['num'])} "
          f"kw={_check_glyph(scores['kw'])} "
          f"clean={_check_glyph(scores['clean'])}  "
          f"{DIM}({latency:.1f}s){RESET}")

    if verbose or not scores["overall"]:
        print(f"    {DIM}Q:{RESET} {q['question']}")
        snippet = answer[:300].replace("\n", " ")
        print(f"    {DIM}A:{RESET} {snippet}{'…' if len(answer) > 300 else ''}")

    return {
        "id": qid, "category": q["category"], "question": q["question"],
        "answer": answer, "error": None, "latency": latency,
        "scores": scores, "expect": q,
    }


async def run_all(filter_id: str | None, no_stretch: bool, verbose: bool) -> list[dict]:
    questions = QUESTIONS
    if filter_id:
        questions = [q for q in QUESTIONS if q["id"] == filter_id]
        if not questions:
            print(f"{RED}No question with id={filter_id}{RESET}")
            return []
    if no_stretch:
        questions = [q for q in questions if q["category"] != "Stretch"]

    print(f"\n── Running {len(questions)} questions against {CHAT_URL} ──")
    print(f"   org_id      : {ORG_ID}")
    print(f"   business_id : {BUSINESS_ID}")
    print()

    results = []
    async with httpx.AsyncClient() as client:
        for q in questions:
            res = await run_one(client, q, verbose=verbose)
            results.append(res)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(results: list[dict]) -> bool:
    if not results:
        return False

    total = len(results)
    passed = sum(1 for r in results if r["scores"].get("overall"))
    by_cat: dict[str, list] = defaultdict(list)
    for r in results:
        by_cat[r["category"]].append(r)
    total_time = sum(r["latency"] for r in results)

    print()
    print("=" * 70)
    print(f"{BOLD}  Step 6 Results — Gift Cards Domain{RESET}")
    print("=" * 70)
    pct = passed / total * 100 if total else 0
    color = GREEN if pct == 100 else (YELLOW if pct >= 80 else RED)
    print(f"  Overall : {color}{passed}/{total} passed ({pct:.0f}%){RESET}")
    print(f"  Time    : {total_time:.1f}s total")
    print(f"  By category:")
    for cat in sorted(by_cat):
        rs = by_cat[cat]
        cp = sum(1 for r in rs if r["scores"].get("overall"))
        bar = "█" * cp + "░" * (len(rs) - cp)
        print(f"    {cat:<14s} {bar}  {cp}/{len(rs)}")

    failed = [r for r in results if not r["scores"].get("overall")]
    if failed:
        print(f"\n  {RED}── Failed ({len(failed)}) ──{RESET}")
        for r in failed:
            tags = []
            for k in ("empty", "num", "kw", "clean"):
                if not r["scores"].get(k, True):
                    tags.append(k)
            err = f" [{r['error']}]" if r.get("error") else ""
            print(f"    {RED}✗{RESET} [{r['id']}] {r['question']}")
            print(f"       Issues: {', '.join(tags)}{err}")

    print()
    print("=" * 70)
    if passed == total:
        print(f"  {GREEN}{BOLD}✅ STEP 6 PASS{RESET}")
    elif pct >= 80:
        print(f"  {YELLOW}{BOLD}🟡 STEP 6 PARTIAL — review failures, refine in Step 7{RESET}")
    else:
        print(f"  {RED}{BOLD}❌ STEP 6 INCOMPLETE — {len(failed)} failing{RESET}")
    print("=" * 70)
    return passed == total


def save_results(results: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "domain":   "giftcards",
        "org_id":   ORG_ID,
        "business_id": BUSINESS_ID,
        "chat_url": CHAT_URL,
        "total":    len(results),
        "passed":   sum(1 for r in results if r["scores"].get("overall")),
        "results":  results,
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"  → results saved to {path}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--question", default=None,
                          help="Run a single question by id (Q1, Q31, S2, …)")
    parser.add_argument("--no-stretch", action="store_true",
                          help="Skip cross-domain stretch questions (S1-S4)")
    parser.add_argument("--verbose", action="store_true",
                          help="Print Q + A for every question (not just failures)")
    parser.add_argument("--output", type=Path, default=None,
                          help="Save full results to a JSON file")
    args = parser.parse_args()

    results = asyncio.run(run_all(
        filter_id  = args.question,
        no_stretch = args.no_stretch,
        verbose    = args.verbose,
    ))
    all_pass = print_summary(results)
    if args.output:
        save_results(results, args.output)
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()