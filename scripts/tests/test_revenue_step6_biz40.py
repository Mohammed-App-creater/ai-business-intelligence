"""
scripts/tests/test_revenue_step6_biz40.py
==========================================
Step 6 — Revenue Domain Test (biz 40, UAT 2026 Q1 data)

Unlike the biz 42 version (which tested against hardcoded fixture data
with known values), this harness fetches GROUND TRUTH from the real
UAT analytics backend at the start of each run, then validates AI
answers against live numbers. Tests remain valid even if the dev DB
gets reseeded between runs.

Two layers of validation:

  Layer 1 — Structural (every question):
    ✅ Not empty, not a refusal
    ✅ Routed to RAG (not falling back to generic advice)
    ✅ Domain vocabulary present
    ✅ No hallucinated months (outside Jan–Mar 2026 window)

  Layer 2 — Ground truth cross-check (factual questions only):
    ✅ Order-of-magnitude match against live backend data
       (e.g. AI says "$90K" and backend says $90,861.74 → pass)
    ✅ Direction agreement for trend questions

Usage:
    # Default — biz 40, 2026-01-01 to 2026-03-31
    python scripts/tests/test_revenue_step6_biz40.py

    # Verbose — print every answer, not just failures
    python scripts/tests/test_revenue_step6_biz40.py --verbose

    # Run one question by ID
    python scripts/tests/test_revenue_step6_biz40.py --question Q_FEB_REV

    # Different business / range
    python scripts/tests/test_revenue_step6_biz40.py \\
        --business-id 40 \\
        --start 2026-01-01 \\
        --end   2026-03-31

    # Save JSON report
    python scripts/tests/test_revenue_step6_biz40.py \\
        --output results/step6_revenue_biz40.json
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
from datetime import date
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

# ── Path bootstrap ────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve()
for _c in (_HERE.parent, *_HERE.parents):
    if (_c / "app").is_dir():
        if str(_c) not in sys.path:
            sys.path.insert(0, str(_c))
        break

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.core.config import settings
from app.services.analytics_client import AnalyticsClient


# ── ANSI colors ───────────────────────────────────────────────────────────────
class C:
    RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
    RED, GREEN, YELLOW, CYAN, BLUE = "\033[31m", "\033[32m", "\033[33m", "\033[36m", "\033[34m"


def c(t: str, col: str) -> str:
    return f"{col}{t}{C.RESET}"


# ── Config defaults ───────────────────────────────────────────────────────────
CHAT_URL        = "http://localhost:8000/api/v1/chat"
DEFAULT_BIZ     = 40
DEFAULT_START   = "2026-01-01"
DEFAULT_END     = "2026-03-31"
REQUEST_TIMEOUT = 60


# ── Ground truth container ────────────────────────────────────────────────────

@dataclass
class GroundTruth:
    """Live data pulled from UAT at the start of the test run."""
    business_id: int
    start: date
    end: date

    # monthly-summary derived
    monthly_by_period: dict[str, dict] = field(default_factory=dict)  # "2026-02" → row
    total_revenue:     float = 0.0
    total_visits:      int   = 0
    best_period:       Optional[str] = None
    worst_period:      Optional[str] = None
    trend_slope:       float = 0.0
    mom_deltas:        dict[str, Optional[float]] = field(default_factory=dict)

    # payment-types
    payment_types: list[dict] = field(default_factory=list)

    # by-staff
    staff_ranked: list[dict] = field(default_factory=list)  # rank 1..N

    # by-location
    locations_by_period: dict[str, list[dict]] = field(default_factory=dict)  # "2026-02" → rows
    total_locations:     int = 0

    # promo-impact
    promo_rows:          list[dict] = field(default_factory=list)
    has_promo_activity:  bool = False

    # failed-refunds
    failed_refund_rows:  list[dict] = field(default_factory=list)
    total_lost_revenue:  float = 0.0

    @property
    def is_empty(self) -> bool:
        return not self.monthly_by_period

    @property
    def valid_months(self) -> set[str]:
        return set(self.monthly_by_period.keys())

    @property
    def valid_month_labels(self) -> set[str]:
        """Human-readable month names for hallucination checks."""
        out = set()
        MONTHS = {
            "01": "january", "02": "february", "03": "march",
            "04": "april",   "05": "may",      "06": "june",
            "07": "july",    "08": "august",   "09": "september",
            "10": "october", "11": "november", "12": "december",
        }
        for p in self.monthly_by_period:
            _, mm = p.split("-")
            out.add(MONTHS.get(mm, ""))
            out.add(mm)
            out.add(p)
        return {x for x in out if x}


async def fetch_ground_truth(biz: int, start: date, end: date) -> GroundTruth:
    """Pull all 6 revenue slices from UAT and build a GroundTruth snapshot."""
    client = AnalyticsClient(base_url=str(settings.ANALYTICS_BACKEND_URL))

    # monthly-summary + meta
    # Use _post_full to grab meta; fall back to _post if that path isn't used
    # for revenue methods in this version of the client.
    try:
        body = await client._post_full("/api/v1/leo/revenue/monthly-summary", {
            "business_id": biz,
            "start_date":  start.isoformat(),
            "end_date":    end.isoformat(),
            "group_by":    "month",
        })
        monthly_rows = body.get("data", [])
        meta         = body.get("meta", {}) or {}
    except AttributeError:
        monthly_rows = await client.get_revenue_monthly_summary(biz, start, end)
        meta         = {}

    gt = GroundTruth(business_id=biz, start=start, end=end)
    gt.monthly_by_period = {r["period"]: r for r in monthly_rows}
    gt.total_revenue     = float(meta.get("total_service_revenue") or
                                 sum(r.get("service_revenue", 0) for r in monthly_rows))
    gt.total_visits      = int(meta.get("total_visits") or
                               sum(r.get("visit_count", 0) for r in monthly_rows))
    gt.best_period       = meta.get("best_period")
    gt.worst_period      = meta.get("worst_period")
    gt.trend_slope       = float(meta.get("trend_slope", 0.0))
    gt.mom_deltas        = {p: r.get("mom_growth_pct") for p, r in gt.monthly_by_period.items()}

    # payment-types
    gt.payment_types = await client.get_revenue_payment_types(biz, start, end)

    # by-staff
    gt.staff_ranked = await client.get_revenue_by_staff(biz, start, end, limit=50)

    # by-location
    loc_rows = await client.get_revenue_by_location(biz, start, end)
    for r in loc_rows:
        gt.locations_by_period.setdefault(r["period"], []).append(r)
    gt.total_locations = len({r["location_id"] for r in loc_rows})

    # promo-impact
    gt.promo_rows = await client.get_revenue_promo_impact(biz, start, end)
    gt.has_promo_activity = bool(gt.promo_rows)

    # failed-refunds
    gt.failed_refund_rows = await client.get_revenue_failed_refunds(biz, start, end)
    gt.total_lost_revenue = sum(r.get("lost_revenue", 0) for r in gt.failed_refund_rows)

    return gt


# ── Scoring helpers ───────────────────────────────────────────────────────────

def extract_numbers(text: str) -> list[float]:
    """Pull all numeric values from AI answer. Handles $ and , and decimals."""
    out: list[float] = []
    # Dollar amounts — $1,234.56 or $1234
    for m in re.finditer(r"\$\s*([\d,]+(?:\.\d+)?)", text):
        try:
            out.append(float(m.group(1).replace(",", "")))
        except ValueError:
            pass
    # Percentages — 12.5% or 30%
    for m in re.finditer(r"([\-+]?\d+(?:\.\d+)?)\s*%", text):
        try:
            out.append(float(m.group(1)))
        except ValueError:
            pass
    # Bare large numbers (>= 100) — covers "477 visits", "90861"
    for m in re.finditer(r"\b(\d{3,}(?:,\d{3})*(?:\.\d+)?)\b", text):
        try:
            v = float(m.group(1).replace(",", ""))
            if v not in out:
                out.append(v)
        except ValueError:
            pass
    return out


def same_magnitude(ai_nums: list[float], target: float,
                   tolerance: float = 0.5) -> tuple[bool, Optional[float]]:
    """
    Lenient order-of-magnitude check per the user's spec.
    Pass if any AI number is within ±50% of target (covers rounding and
    slightly different interpretations of the same figure).

    Returns (matched, best_ai_value).
    """
    if target == 0:
        # If target is zero, only pass if AI also mentions a zero or "no"
        return (False, None)
    best_diff  = None
    best_value = None
    for n in ai_nums:
        if n <= 0:
            continue
        diff = abs(n - target) / max(target, 1)
        if best_diff is None or diff < best_diff:
            best_diff, best_value = diff, n
    if best_diff is None:
        return (False, None)
    return (best_diff <= tolerance, best_value)


def has_refusal(text: str) -> bool:
    lowered = text.lower()
    refusals = [
        "i don't know",
        "i do not know",
        "i cannot answer",
        "unable to answer",
        "no data available",
        "insufficient data",
        "not available in the provided",
    ]
    return any(r in lowered for r in refusals)


def mentions_any(text: str, terms: list[str]) -> bool:
    t = text.lower()
    return any(term.lower() in t for term in terms)


def has_out_of_range_month(text: str, valid_month_labels: set[str]) -> list[str]:
    """
    Flag month references in the AI answer that aren't in the data window.
    Conservative: only flags months explicitly named by full English word.
    """
    MONTH_WORDS = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ]
    valid_lower = {x.lower() for x in valid_month_labels}
    lowered = text.lower()
    bad = []
    for m in MONTH_WORDS:
        if m in lowered and m not in valid_lower:
            bad.append(m)
    return bad


def v_jan_revenue(answer: str, gt: GroundTruth) -> CheckList:
    row = gt.monthly_by_period.get("2026-01")
    if not row:
        return [("has_jan_data_in_backend", False, "no Jan 2026 row in UAT")]
    target = row["service_revenue"]
    ok, best = same_magnitude(extract_numbers(answer), target)
    detail = (f"AI cited ~${best:,.0f}, backend ${target:,.2f}"
              if best is not None else f"no comparable number (target ${target:,.2f})")
    return [
        ("mentions_january", mentions_any(answer, ["january", "jan", "2026-01"]), None),
        ("jan_revenue_magnitude_match", ok, detail),
    ]


def v_visit_count_feb(answer: str, gt: GroundTruth) -> CheckList:
    row = gt.monthly_by_period.get("2026-02")
    if not row:
        return [("has_feb_data_in_backend", False, "no Feb 2026 row")]
    target = row.get("visit_count", 0)
    ok, best = same_magnitude(extract_numbers(answer), target)
    detail = (f"AI cited ~{best:,.0f}, backend {target}"
              if best is not None else f"no comparable number (target {target})")
    return [("visit_count_magnitude", ok, detail)]


def v_tips_feb(answer: str, gt: GroundTruth) -> CheckList:
    row = gt.monthly_by_period.get("2026-02")
    if not row:
        return [("has_feb_data_in_backend", False, "no Feb 2026 row")]
    target = row.get("total_tips", 0)
    if not target:
        # Feb tips is 0 — expect AI to say so, not make something up
        nums = extract_numbers(answer)
        says_zero = any(n == 0 for n in nums) or any(
            w in answer.lower() for w in ["no tips", "zero", "none"]
        )
        return [("correctly_reports_zero_tips", says_zero, "backend tips=0")]
    ok, best = same_magnitude(extract_numbers(answer), target)
    detail = (f"AI cited ~${best:,.0f}, backend ${target:,.2f}"
              if best is not None else "no comparable number")
    return [("tips_magnitude", ok, detail)]


# ── Question definitions ──────────────────────────────────────────────────────
# Each question carries a validator function that runs Layer 2 checks
# against the live GroundTruth. Validators return a list of (name, passed,
# detail) tuples that get merged into the overall result.

CheckList = list[tuple[str, bool, Optional[str]]]
Validator = Callable[[str, GroundTruth], CheckList]


def noop_validator(answer: str, gt: GroundTruth) -> CheckList:
    return []


def v_feb_revenue(answer: str, gt: GroundTruth) -> CheckList:
    row = gt.monthly_by_period.get("2026-02")
    if not row:
        return [("has_feb_data_in_backend", False, "no Feb 2026 row in UAT")]
    target = row["service_revenue"]
    ok, best = same_magnitude(extract_numbers(answer), target)
    detail = (f"AI cited ~${best:,.0f}, backend ${target:,.2f}"
              if best is not None else f"no comparable number in AI answer (target ${target:,.2f})")
    return [
        ("mentions_february", mentions_any(answer, ["february", "feb", "2026-02"]), None),
        ("revenue_magnitude_match", ok, detail),
    ]


def v_total_revenue(answer: str, gt: GroundTruth) -> CheckList:
    target = gt.total_revenue
    ok, best = same_magnitude(extract_numbers(answer), target)
    detail = (f"AI cited ~${best:,.0f}, total ${target:,.2f}"
              if best is not None else "no number in answer")
    return [("total_revenue_magnitude_match", ok, detail)]


def v_avg_ticket(answer: str, gt: GroundTruth) -> CheckList:
    tickets = [r["avg_ticket"] for r in gt.monthly_by_period.values() if r.get("avg_ticket")]
    if not tickets:
        return [("has_ticket_data", False, None)]
    ai_nums = extract_numbers(answer)
    # Accept any number in the range of per-month avg tickets (±50%)
    for t in tickets:
        ok, _ = same_magnitude(ai_nums, t)
        if ok:
            return [("avg_ticket_in_range", True,
                     f"AI in range of {min(tickets):.0f}–{max(tickets):.0f}")]
    return [("avg_ticket_in_range", False,
             f"AI nums {ai_nums} not near any of {tickets}")]


def v_best_month(answer: str, gt: GroundTruth) -> CheckList:
    if not gt.best_period:
        return [("has_best_period", False, None)]
    year, mm = gt.best_period.split("-")
    MONTHS = ["", "january", "february", "march", "april", "may", "june",
              "july", "august", "september", "october", "november", "december"]
    best_name = MONTHS[int(mm)]
    ok = mentions_any(answer, [best_name, gt.best_period, mm])
    return [("cites_best_month", ok, f"backend best = {gt.best_period} ({best_name})")]


def v_staff_top(answer: str, gt: GroundTruth) -> CheckList:
    if not gt.staff_ranked:
        return [("has_staff_data", False, None)]
    top = gt.staff_ranked[0]
    name = (top.get("staff_name") or "").strip()
    ok_name = bool(name) and name.lower() in answer.lower()
    ok, best = same_magnitude(extract_numbers(answer), top.get("service_revenue", 0))
    return [
        ("mentions_top_staff_name", ok_name,
         f"top = {name} (rank {top.get('revenue_rank')})"),
        ("staff_revenue_magnitude",
         ok,
         f"AI cited ~${best:,.0f}" if best else "no number"),
    ]


def v_location_top(answer: str, gt: GroundTruth) -> CheckList:
    all_locs = {r["location_id"]: r["location_name"]
                for rows in gt.locations_by_period.values() for r in rows}
    if not all_locs:
        return [("has_location_data", False, None)]
    # Aggregate revenue per location across the window
    totals: dict[int, float] = {}
    for rows in gt.locations_by_period.values():
        for r in rows:
            totals[r["location_id"]] = totals.get(r["location_id"], 0) + r["service_revenue"]
    top_loc_id = max(totals, key=totals.get)
    top_loc_name = all_locs[top_loc_id]
    ok = top_loc_name and top_loc_name.lower() in answer.lower()
    return [("mentions_top_location", bool(ok),
             f"top location = {top_loc_name} (${totals[top_loc_id]:,.0f})")]


def v_payment_types(answer: str, gt: GroundTruth) -> CheckList:
    if not gt.payment_types:
        return [("has_payment_data", False, None)]
    # Just check AI mentioned at least TWO distinct payment types
    labels = [str(r.get("payment_type", "")).lower() for r in gt.payment_types]
    mentioned = sum(1 for lbl in labels if lbl and lbl in answer.lower())
    return [("mentions_multiple_payment_types", mentioned >= 2,
             f"backend has {len(labels)}: {labels}, AI mentioned {mentioned}")]


def v_trend_direction(answer: str, gt: GroundTruth) -> CheckList:
    slope = gt.trend_slope

    # STRIP generated advisory sections before keyword matching.
    # The AI's response format is roughly:
    #    <main claim>
    #    <supporting data>
    #    Key factors: ...
    #    Recommendations: Increase X, Improve Y, ...
    # Words like "Increase marketing" in Recommendations must NOT count as
    # the AI claiming revenue is going up. We look only at the analytical
    # portion — text BEFORE the first "Recommendations" / "Key factors"
    # header, falling back to the full text if no markers present.
    t = answer.lower()
    for marker in ("recommendations:", "recommendation:", "key factors:",
                   "suggested actions:", "next steps:"):
        idx = t.find(marker)
        if idx != -1:
            t = t[:idx]

    # Backend-reported direction → which keywords we expect in the claim
    if slope > 0:
        backend_says = "up"
        ai_says_right = any(w in t for w in
                            ["grow", "growing", "increas", "up ", "up.", "up,",
                             "rising", "upward", "trending up", "improv"])
    elif slope < 0:
        backend_says = "down"
        ai_says_right = any(w in t for w in
                            ["decline", "declining", "decreas", "down ", "down.",
                             "down,", "falling", "shrink", "shrinking", "dropp",
                             "trending down", "worsen"])
    else:
        backend_says = "flat"
        ai_says_right = any(w in t for w in
                            ["flat", "stable", "unchanged", "steady",
                             "no change", "consistent"])

    return [("trend_direction_agrees", ai_says_right,
             f"backend slope = {slope:+.2f} ({backend_says}) — "
             f"AI analytical section " + ("agrees" if ai_says_right else "disagrees"))]


def v_failed_refunds(answer: str, gt: GroundTruth) -> CheckList:
    if not gt.failed_refund_rows:
        return [("has_refund_data", False, None)]
    target = gt.total_lost_revenue
    ok, best = same_magnitude(extract_numbers(answer), target)
    return [("lost_revenue_magnitude", ok,
             f"AI cited ~${best:,.0f}, backend total lost ${target:,.2f}"
             if best else "no comparable number")]


def v_promo_impact(answer: str, gt: GroundTruth) -> CheckList:
    if not gt.has_promo_activity:
        # Must say no promos / none / zero — not fabricate a number
        ai_nums = extract_numbers(answer)
        ok_empty = any(w in answer.lower()
                       for w in ["no promo", "no discount", "none", "zero",
                                 "not used", "no promo codes"]) or not ai_nums
        return [("correctly_reports_no_promos", ok_empty,
                 "backend has 0 promo rows — AI must not invent a number")]
    # If promos exist, just check AI mentioned the word and cited something
    return [("mentions_promo", "promo" in answer.lower() or "discount" in answer.lower(), None)]


# ── Question bank ─────────────────────────────────────────────────────────────

@dataclass
class Q:
    id: str
    category: str
    question: str
    domain_terms: list[str]
    validator: Validator = noop_validator
    expect_number: bool = True
    allow_no_data: bool = False          # if True, a clean "no data" response passes
    skip_if_empty_backend: bool = False  # skip entirely if UAT returned no data


QUESTIONS: list[Q] = [
    # ── Basic Facts ───────────────────────────────────────────────────────────
    Q("Q_FEB_REV", "Basic Facts",
      "What was my revenue in February 2026?",
      domain_terms=["revenue", "february"],
      validator=v_feb_revenue),

    Q("Q_JAN_REV", "Basic Facts",
      "What was my revenue in January 2026?",
      domain_terms=["revenue", "january"],
      validator=v_jan_revenue),

    Q("Q_TOTAL_REV", "Basic Facts",
      "What was my total revenue over the period Jan through March 2026?",
      domain_terms=["revenue", "total"],
      validator=v_total_revenue),

    Q("Q_AVG_TICKET", "Basic Facts",
      "What is my average ticket value per visit?",
      domain_terms=["ticket", "visit", "average"],
      validator=v_avg_ticket),

    Q("Q_VISIT_COUNT", "Basic Facts",
      "How many visits did I have in February 2026?",
      domain_terms=["visits", "february"],
      validator=v_visit_count_feb),

    # ── Trends ────────────────────────────────────────────────────────────────
    Q("Q_TREND", "Trends",
      "Is my revenue trending up or down over the last 3 months?",
      domain_terms=["trend", "revenue"],
      validator=v_trend_direction),

    Q("Q_BEST_MONTH", "Trends",
      "Which was my best revenue month in Q1 2026?",
      domain_terms=["best", "month", "revenue"],
      validator=v_best_month),

    Q("Q_MOM_CHANGE", "Trends",
      "How did my revenue change from January to February 2026?",
      domain_terms=["revenue", "change"],
      validator=noop_validator),   # direction check is implicit in trend Q

    # ── Rankings ──────────────────────────────────────────────────────────────
    Q("Q_TOP_STAFF", "Rankings",
      "Which staff member generated the most revenue in Q1 2026?",
      domain_terms=["staff", "revenue"],
      validator=v_staff_top),

    Q("Q_TOP_LOCATION", "Rankings",
      "Which location brought in the most revenue in Q1 2026?",
      domain_terms=["location", "revenue"],
      validator=v_location_top),

    Q("Q_PAYMENT_SPLIT", "Rankings",
      "What percentage of my revenue came from cash vs card vs other payment types?",
      domain_terms=["cash", "card", "revenue"],
      validator=v_payment_types),

    # ── Root Cause / Analytical ───────────────────────────────────────────────
    Q("Q_BIG_SWING", "Root Cause",
      "Why did my revenue change so much between January and February 2026?",
      domain_terms=["revenue", "visit", "ticket"],
      validator=noop_validator,
      expect_number=True),

    # ── Edge Cases ────────────────────────────────────────────────────────────
    Q("Q_TIPS", "Edge Cases",
      "How much in tips did my staff collect in February 2026?",
      domain_terms=["tip", "staff"],
      validator=v_tips_feb),

    Q("Q_TAX", "Edge Cases",
      "How much tax did I collect in Q1 2026?",
      domain_terms=["tax"],
      validator=noop_validator,  # data is inconsistent on UAT — $0 for 2 of 3 months
      allow_no_data=True),

    Q("Q_REFUNDS", "Edge Cases",
      "How many visits ended with a refund or failed payment, and what was the total value?",
      domain_terms=["refund", "failed", "visit"],
      validator=v_failed_refunds),

    Q("Q_PROMOS", "Edge Cases",
      "How much did promo codes cost me in discounts during Q1 2026?",
      domain_terms=["promo", "discount"],
      validator=v_promo_impact,
      allow_no_data=True),

    # ── Advice (structural only) ──────────────────────────────────────────────
    Q("Q_ADVICE", "Advice",
      "What can I do to increase my revenue next month?",
      domain_terms=["revenue", "increase"],
      validator=noop_validator,
      expect_number=False),

    Q("Q_WORRY", "Advice",
      "Should I be worried about my revenue trend — is my business growing or shrinking?",
      domain_terms=["revenue", "trend"],
      validator=v_trend_direction,
      expect_number=False),
]


# ── Scoring ───────────────────────────────────────────────────────────────────

def score(q: Q, answer: str, route: Optional[str], gt: GroundTruth) -> dict:
    checks: CheckList = []

    # Structural — Layer 1
    not_empty = bool(answer.strip()) and len(answer.strip()) > 20
    checks.append(("not_empty", not_empty, None))

    routed_rag = (route or "").upper() == "RAG"
    checks.append(("routed_to_rag", routed_rag, f"route={route!r}"))

    refused = has_refusal(answer)
    if q.allow_no_data and refused:
        # Legitimate "no data" response — skip harder checks
        checks.append(("legitimate_no_data_response", True, None))
        ok = all(passed for _, passed, _ in checks)
        return {
            "id":       q.id,
            "category": q.category,
            "question": q.question,
            "answer":   answer[:400],
            "route":    route,
            "checks":   checks,
            "passed":   ok,
            "skipped":  False,
        }
    else:
        checks.append(("not_refusing", not refused, None))

    if q.expect_number:
        nums = extract_numbers(answer)
        checks.append(("contains_number", bool(nums), f"nums={nums[:5]}"))

    domain_ok = mentions_any(answer, q.domain_terms)
    checks.append(("domain_vocabulary", domain_ok,
                   f"expected any of {q.domain_terms}"))

    bad_months = has_out_of_range_month(answer, gt.valid_month_labels)
    checks.append(("no_hallucinated_months", not bad_months,
                   f"out-of-range months: {bad_months}" if bad_months else None))

    # Ground truth — Layer 2
    try:
        layer2 = q.validator(answer, gt)
        # Some validators return raw (bool, detail) tuples — normalize them
        for item in layer2:
            if len(item) == 2:
                name, passed = item
                detail = None
            else:
                name, passed, detail = item
            checks.append((name, bool(passed), detail))
    except Exception as e:
        checks.append(("ground_truth_validator_crashed", False, f"{type(e).__name__}: {e}"))

    passed = all(p for _, p, _ in checks)
    return {
        "id":       q.id,
        "category": q.category,
        "question": q.question,
        "answer":   answer[:400],
        "route":    route,
        "checks":   checks,
        "passed":   passed,
        "skipped":  False,
    }


# ── Chat caller ───────────────────────────────────────────────────────────────

async def ask(client: httpx.AsyncClient, biz: int,
              question: str) -> tuple[str, Optional[str], float]:
    payload = {
        "business_id": str(biz),
        "org_id":      str(biz),
        "question":    question,
    }
    t0 = time.perf_counter()
    try:
        resp = await client.post(CHAT_URL, json=payload, timeout=REQUEST_TIMEOUT)
        latency = (time.perf_counter() - t0) * 1000
        resp.raise_for_status()
        body = resp.json()
        return (
            body.get("answer") or body.get("response") or str(body),
            body.get("route") or body.get("routing"),
            latency,
        )
    except httpx.HTTPStatusError as e:
        return (f"HTTP {e.response.status_code}: {e.response.text[:200]}",
                None, (time.perf_counter() - t0) * 1000)
    except Exception as exc:
        return f"ERROR: {exc}", None, (time.perf_counter() - t0) * 1000


# ── Rendering ─────────────────────────────────────────────────────────────────

def render_ground_truth(gt: GroundTruth) -> None:
    print(c("\n── Ground truth snapshot (from UAT) ─────────────────────────────", C.CYAN))
    if gt.is_empty:
        print(c(f"  business_id={gt.business_id} has NO data in "
                f"{gt.start} → {gt.end}", C.YELLOW))
        return
    print(c(f"  business_id={gt.business_id}   {gt.start} → {gt.end}", C.DIM))
    print(c(f"  Months with data   : {sorted(gt.valid_months)}", C.DIM))
    print(c(f"  Total revenue      : ${gt.total_revenue:,.2f}", C.DIM))
    print(c(f"  Total visits       : {gt.total_visits}", C.DIM))
    print(c(f"  Best / Worst month : {gt.best_period} / {gt.worst_period}", C.DIM))
    print(c(f"  Trend slope        : {gt.trend_slope:+.2f}  "
            f"({'up' if gt.trend_slope > 0 else 'down' if gt.trend_slope < 0 else 'flat'})",
            C.DIM))
    print(c(f"  Locations          : {gt.total_locations}", C.DIM))
    print(c(f"  Staff ranked       : {len(gt.staff_ranked)}", C.DIM))
    print(c(f"  Payment types      : {len(gt.payment_types)}", C.DIM))
    print(c(f"  Promos             : {len(gt.promo_rows)} rows", C.DIM))
    print(c(f"  Failed/refund total: ${gt.total_lost_revenue:,.2f}", C.DIM))


def render_result(r: dict, verbose: bool) -> None:
    icon = c("✅", C.GREEN) if r["passed"] else c("❌", C.RED)
    print(f"  {icon} [{r['id']:<15}] {r['category']:<12} {r['question'][:60]}")

    if verbose or not r["passed"]:
        for name, passed, detail in r["checks"]:
            mk = c("✓", C.GREEN) if passed else c("✗", C.RED)
            line = f"       {mk} {name}"
            if detail and (verbose or not passed):
                line += c(f"  — {detail}", C.DIM)
            print(line)
        if verbose or not r["passed"]:
            print(c(f"       A: {r['answer'][:300]}", C.DIM))
            if r.get("route"):
                print(c(f"       route={r['route']}", C.DIM))
        print()


def print_summary(results: list[dict], total_ms: float) -> None:
    passed  = sum(1 for r in results if r["passed"])
    skipped = sum(1 for r in results if r.get("skipped"))
    total   = len(results)
    pct     = passed / total * 100 if total else 0

    cats: dict[str, list[bool]] = {}
    for r in results:
        cats.setdefault(r["category"], []).append(r["passed"])

    print()
    print(c("═" * 66, C.CYAN))
    print(c(f"  Step 6 Results — Revenue Domain (biz 40, UAT)", C.BOLD))
    print(c("═" * 66, C.CYAN))
    print(f"  Overall : {passed}/{total} passed ({pct:.0f}%)"
          + (f"  · {skipped} skipped" if skipped else ""))
    print(f"  Time    : {total_ms/1000:.1f}s total")
    print()
    print("  By category:")
    for cat, bools in cats.items():
        cp, ct = sum(bools), len(bools)
        bar = "█" * cp + "░" * (ct - cp)
        print(f"    {cat:<14} {bar}  {cp}/{ct}")

    failed = [r for r in results if not r["passed"]]
    if failed:
        print(c(f"\n  ── Failed ({len(failed)}) ────────────────────────────", C.RED))
        for r in failed:
            failed_checks = [n for n, p, _ in r["checks"] if not p]
            print(c(f"    ❌ [{r['id']}] {r['question'][:50]}", C.RED))
            print(c(f"         Failed: {', '.join(failed_checks)}", C.DIM))

    print()
    print(c("═" * 66, C.CYAN))
    verdict = (c("✅ STEP 6 PASS", C.GREEN + C.BOLD) if passed == total
               else c(f"⚠  STEP 6 INCOMPLETE — {total-passed} failing", C.YELLOW + C.BOLD))
    print(f"  {verdict}")
    print(c("═" * 66, C.CYAN))
    print()


# ── Main ──────────────────────────────────────────────────────────────────────

async def main_async(args: argparse.Namespace) -> int:
    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)

    print(c(f"\n{'═' * 66}", C.CYAN))
    print(c("  Step 6 — Revenue Domain Test (biz 40 / UAT)", C.BOLD))
    print(c(f"  Business : {args.business_id}", C.DIM))
    print(c(f"  Range    : {start} → {end}", C.DIM))
    print(c(f"  Chat URL : {CHAT_URL}", C.DIM))
    print(c(f"  Backend  : {settings.ANALYTICS_BACKEND_URL}", C.DIM))
    print(c("═" * 66, C.CYAN))

    # 1. Fetch live ground truth
    print(c("\n▶ Fetching ground truth from UAT backend...", C.BOLD))
    try:
        gt = await fetch_ground_truth(args.business_id, start, end)
    except Exception as e:
        print(c(f"  ❌ Failed to fetch ground truth: {type(e).__name__}: {e}", C.RED))
        print(c("     Tests cannot run without ground truth. Abort.", C.RED))
        return 2
    render_ground_truth(gt)

    if gt.is_empty and not args.question:
        print(c("\n⚠  Backend returned empty for this biz+range — most questions "
                "will be skipped.", C.YELLOW))

    # 2. Pick questions
    questions = QUESTIONS
    if args.question:
        questions = [q for q in QUESTIONS
                     if q.id.upper() == args.question.upper()]
        if not questions:
            print(c(f"\n  Unknown question ID: {args.question}", C.RED))
            print(c(f"  Valid IDs: {[q.id for q in QUESTIONS]}", C.DIM))
            return 1

    # 3. Fire them at the chat endpoint
    print(c(f"\n▶ Running {len(questions)} questions against the chat endpoint...", C.BOLD))
    session_id = str(uuid.uuid4())
    print(c(f"   session_id = {session_id}\n", C.DIM))

    results: list[dict] = []
    t_total = time.perf_counter()

    async with httpx.AsyncClient() as client:
        for q in questions:
            answer, route, latency = await ask(client, args.business_id, q.question)
            r = score(q, answer, route, gt)
            r["latency_ms"] = latency
            results.append(r)
            render_result(r, verbose=args.verbose)
            await asyncio.sleep(0.3)

    total_ms = (time.perf_counter() - t_total) * 1000
    print_summary(results, total_ms)

    # 4. Optional JSON output
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        serializable_results = []
        for r in results:
            sr = {**r,
                  "checks": [{"name": n, "passed": p, "detail": d}
                             for n, p, d in r["checks"]]}
            serializable_results.append(sr)
        payload = {
            "business_id": args.business_id,
            "start":       args.start,
            "end":         args.end,
            "run_date":    date.today().isoformat(),
            "session_id":  session_id,
            "ground_truth_summary": {
                "months":        sorted(gt.valid_months),
                "total_revenue": gt.total_revenue,
                "total_visits":  gt.total_visits,
                "best_period":   gt.best_period,
                "worst_period":  gt.worst_period,
                "trend_slope":   gt.trend_slope,
                "locations":     gt.total_locations,
                "staff_count":   len(gt.staff_ranked),
                "promo_rows":    len(gt.promo_rows),
            },
            "passed":  sum(1 for r in results if r["passed"]),
            "total":   len(results),
            "results": serializable_results,
        }
        with open(out, "w") as f:
            json.dump(payload, f, indent=2, default=str)
        print(c(f"  Results saved → {out}", C.DIM))
        print()

    return 0 if all(r["passed"] for r in results) else 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Step 6 — Revenue Domain Test (biz 40 / UAT 2026 Q1)"
    )
    parser.add_argument("--business-id", type=int, default=DEFAULT_BIZ,
                        help=f"Business ID to test (default {DEFAULT_BIZ})")
    parser.add_argument("--start",       default=DEFAULT_START,
                        help=f"Window start (YYYY-MM-DD, default {DEFAULT_START})")
    parser.add_argument("--end",         default=DEFAULT_END,
                        help=f"Window end (YYYY-MM-DD, default {DEFAULT_END})")
    parser.add_argument("--question",    default=None,
                        help="Run one question by ID (e.g. Q_FEB_REV)")
    parser.add_argument("--verbose",     action="store_true",
                        help="Print every answer and every check, not just failures")
    parser.add_argument("--output",      default=None,
                        help="Save JSON report to this path")
    args = parser.parse_args()

    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()