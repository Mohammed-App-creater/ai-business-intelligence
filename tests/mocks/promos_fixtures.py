"""
tests/mocks/promos_fixtures.py
================================
Realistic mock response data for the 4 Promos endpoints:
  EP1: /api/v1/leo/promos/monthly          → MONTHLY_SUMMARY
  EP2: /api/v1/leo/promos/codes            → CODES (monthly + window granularity)
  EP3: /api/v1/leo/promos/locations        → LOCATIONS (by_code + rollup shapes)
  EP4: /api/v1/leo/promos/catalog-health   → CATALOG_HEALTH

Based on the same salon business (business_id=42) with 2 locations (Main St +
Westside), consistent with revenue/appointments/staff/services/clients/
marketing/expenses fixtures.

── Time window ───────────────────────────────────────────────────────────────
6 months: 2025-11-01 through 2026-04-30. "last month" = Mar 2026.
Aligns with Expenses fixture window for cross-domain consistency.

── Schema reality (from dev-DB inspection) ───────────────────────────────────
tbl_promo on dev had 7 system-wide codes (no OrganizationId). tbl_visit had
ZERO promo redemptions across all tenants — feature is schema-only on dev.
This fixture simulates the redemption activity that would exist in prod.

FK assumption (unvalidated on dev): tbl_visit.PromoCode (int) → tbl_promo.Id (int)
Validated only at first prod ETL run. Mock fixtures assume the FK works.

── Code roster for biz 42 (5 real codes + 1 orphan) ──────────────────────────
Codes mirror real prod tbl_promo entries observed on dev:
  ID  Code      Label       Active  Expiration   Story role
  3   DM8880    "20% Off"   1       2026-05-04   TOP performer; Mar spike
  5   PM8880    "10% Off"   1       2026-05-04   Mid-tier; both locations
  7   Awan      "10% OFF"   1       2027-01-31   Westside-heavy specialist
  1   POFL99    "test"      1       2025-02-15   ACTIVE-BUT-EXPIRED (Q23)
  4   DM881     "70% Off"   1       2026-04-28   DORMANT — 0 redemptions in window
  999 (orphan)  NULL        NULL    NULL         1 visit references missing promo

Active=1 always — dev table shows all 7 as active. Inactive codes weren't
observed in dev sample, so we don't fabricate any.

── Boundary with Marketing domain (locked decision) ──────────────────────────
Marketing's PROMO_ATTRIBUTION_MONTHLY covers WELCOME10/SUMMER20/HOLIDAY15
(campaign-attributed codes). Promos covers DM8880/PM8880/Awan/POFL99/DM881
(standalone codes — no campaign attribution). Both domains read the same
tbl_visit.PromoCode column in production; mock world keeps them disjoint
to avoid retrieval collisions. The router boundary rule (campaign keywords
→ marketing, otherwise → promos) handles the prod overlap case.

── Stories baked in ──────────────────────────────────────────────────────────
1. DM8880 SPIKE (Mar 2026): 14 redemptions ($186 discount) vs Feb 8 ($104)
   = +75% MoM. Drives Q14 "why did discounts spike in Mar".

2. PM8880 DROP (Feb→Mar 2026): Feb had 6 redemptions, Mar dropped to 2.
   Drives Q15 "which codes lost activity".

3. AWAN WESTSIDE-HEAVY: 90% of Awan redemptions at Westside (loc 2).
   Combined with DM8880 being Main St-heavy:
     - Westside redeems MORE codes by count (Q19)
     - Main St gives MORE total discount (Q21)
   Two different answers exercises Promos count-vs-amount distinction.

4. POFL99 ACTIVE-BUT-EXPIRED: Active=1 but expired 2025-02-15. As of ref
   date 2026-04-01, this is a data quality flag. Drives Q23. ZERO redemptions
   in window (nobody can redeem an expired code).

5. DM881 DORMANT: Active=1, valid until 2026-04-28, but ZERO redemptions in
   the last 90 days (and 0 in window). Drives Q22.

6. ORPHAN PROMO_ID=999: One Mar 2026 visit references a promo that doesn't
   exist in tbl_promo (FK orphan). Per ETL contract, comes back with
   promo_code_string=NULL, promo_label=NULL. Proves orphan handling works
   end-to-end. Single redemption, $20 discount.

7. PROMO USAGE GROWING: % of visits using promos trends from ~5% → ~9%
   over the window. Drives Q6 trend analysis.

8. RECONCILE WITH EXPENSES: Marketing expenses spiked Feb 2026 ($640).
   Promo redemptions DID NOT spike Feb 2026 — slight DROP. This is
   realistic — owner panic-bought ads but standalone codes (DM8880/PM8880)
   weren't tied to those campaigns. The spike came in Mar after one cycle.

── Reference date ────────────────────────────────────────────────────────────
ref_date = 2026-04-01 (LEO_TEST_REF_DATE)
  → "last month" = 2026-03
  → "this month" = 2026-04 (partial — flagged on Q5)
  → 90-day dormancy window: 2026-01-01 → 2026-04-01

── Acceptance: every Step 1 question maps to data in this fixture ────────────
  Q1  redemptions last month         → MONTHLY_SUMMARY Mar row promo_redemptions
  Q2  total discount last month       → MONTHLY_SUMMARY Mar row total_discount_given
  Q3  distinct codes 6mo              → MONTHLY_SUMMARY distinct_codes_used aggregated
  Q4  total discount YTD              → SUM of Jan+Feb+Mar+Apr (partial) discount
  Q5  this month vs last              → MTD partial — flagged
  Q6  6-month trend                   → MONTHLY_SUMMARY full series
  Q7  best month                      → Mar 2026 (highest redemptions)
  Q8  QoQ                             → Q1 2026 from monthly; no Q4 2025 (window starts Nov)
  Q9  most redeemed last month        → CODES Mar: DM8880 = 14 redemptions
  Q10 top by total discount           → CODES window: DM8880
  Q11 biggest avg discount             → CODES window: DM8880 avg
  Q12 % visits using promo last mo    → MONTHLY_SUMMARY Mar promo_visit_pct
  Q13 least used (3 months)           → CODES filtered Jan-Mar: PM8880 dropped
  Q14 why spike in Mar                → CODES per-code monthly: DM8880 jumped
  Q15 which codes lost activity       → CODES per-code: PM8880 Feb→Mar drop
  Q16 retire which codes              → CATALOG_HEALTH: DM881 + POFL99
  Q17 overused codes                  → CODES window: DM8880 highest avg
  Q18 discount per location           → LOCATIONS rollup Mar
  Q19 location redeems most            → LOCATIONS rollup: Westside (count)
  Q20 compare locations                → LOCATIONS rollup
  Q21 most discount this year         → LOCATIONS rollup window: Main St ($)
  Q22 dormant active codes            → CATALOG_HEALTH: is_dormant=1 → DM881, POFL99
  Q23 active but expired              → CATALOG_HEALTH: active_but_expired=1 → POFL99
  Q24 biggest single discount          → CODES window: max_single_discount
  Q25 avg discount per redemption     → MONTHLY_SUMMARY avg_discount_per_redemption
  Q26 % visits with promo this year   → MONTHLY_SUMMARY 2026 rows promo_visit_pct
"""

from __future__ import annotations
from collections import defaultdict
from datetime import date


# ─────────────────────────────────────────────────────────────────────────────
# Window + reference date
# ─────────────────────────────────────────────────────────────────────────────

REF_DATE = date(2026, 4, 1)
WINDOW_START = "2025-11-01"
WINDOW_END = "2026-05-01"  # exclusive — covers up to Apr 30 2026
TODAY_STR = "2026-04-21"   # for catalog-health computations


# ─────────────────────────────────────────────────────────────────────────────
# Code catalog — mirrors observed prod tbl_promo rows
# ─────────────────────────────────────────────────────────────────────────────

_PROMO_CATALOG = {
    1:   {"code": "POFL99", "label": "test",    "amount": 0.99, "active": 1, "expires": "2025-02-15"},  # active-but-expired
    3:   {"code": "DM8880", "label": "20% Off", "amount": 10.00, "active": 1, "expires": "2026-05-04"},
    4:   {"code": "DM881",  "label": "70% Off", "amount": 0.00,  "active": 1, "expires": "2026-04-28"},  # dormant
    5:   {"code": "PM8880", "label": "10% Off", "amount": 10.00, "active": 1, "expires": "2026-05-04"},
    7:   {"code": "Awan",   "label": "10% OFF", "amount": 10.00, "active": 1, "expires": "2027-01-31"},
    # 999 is the orphan — not in catalog
}

_LOC_NAMES = {1: "Main St", 2: "Westside"}


# ─────────────────────────────────────────────────────────────────────────────
# Per-code per-month per-location redemption schedule
# (the source of truth for everything below)
# ─────────────────────────────────────────────────────────────────────────────
# Format: (period, promo_id, location_id) → {redemptions, discounts: [list of $], }
# Discount lists let us compute SUM, AVG, MAX cleanly.
#
# Story tuning:
#   DM8880 (top performer): grows steadily, big spike Mar 2026
#   PM8880 (mid-tier): steady then drops Feb→Mar
#   Awan (Westside specialist): 90% Westside
#   DM881, POFL99: ZERO redemptions in window
#   orphan 999: 1 visit Mar 2026, $20 discount

_REDEMPTIONS_RAW: dict[tuple[str, int, int], list[float]] = {
    # ══ NOV 2025 — baseline month, low promo activity ══════════════════════
    ("2025-11-01", 3, 1): [12.00, 13.50, 11.00, 14.00, 12.50],         # DM8880 Main St: 5 × ~$13
    ("2025-11-01", 3, 2): [10.50, 12.00],                              # DM8880 Westside: 2
    ("2025-11-01", 5, 1): [8.00, 7.50, 9.00],                          # PM8880 Main St: 3
    ("2025-11-01", 5, 2): [7.00, 8.50, 8.00],                          # PM8880 Westside: 3
    ("2025-11-01", 7, 2): [6.50, 7.00, 6.00, 7.50],                    # Awan Westside: 4
    # No Awan at Main St in Nov

    # ══ DEC 2025 — slight dip (post-holiday for promos, ironically) ═══════
    ("2025-12-01", 3, 1): [13.00, 14.00, 12.50, 13.50],                # 4
    ("2025-12-01", 3, 2): [11.50, 12.50, 13.00],                       # 3
    ("2025-12-01", 5, 1): [9.00, 8.50],                                # 2
    ("2025-12-01", 5, 2): [8.00, 9.00, 7.50],                          # 3
    ("2025-12-01", 7, 2): [7.00, 6.50, 7.50, 6.00, 7.00],              # 5
    ("2025-12-01", 7, 1): [6.50],                                      # 1 — first Awan at Main St

    # ══ JAN 2026 — new year recovery ══════════════════════════════════════
    ("2026-01-01", 3, 1): [12.50, 14.00, 13.00, 13.50, 12.00, 14.50],  # 6
    ("2026-01-01", 3, 2): [11.50, 12.00, 13.00],                       # 3
    ("2026-01-01", 5, 1): [8.50, 9.00, 8.00],                          # 3
    ("2026-01-01", 5, 2): [7.50, 8.00, 8.50, 7.00],                    # 4
    ("2026-01-01", 7, 2): [7.00, 6.50, 7.50, 6.00],                    # 4
    ("2026-01-01", 7, 1): [],                                          # 0 — back to Westside-only

    # ══ FEB 2026 — weak month overall (matches other domains) ═════════════
    ("2026-02-01", 3, 1): [13.00, 14.00, 12.50, 13.50, 14.00],         # 5 — slight dip
    ("2026-02-01", 3, 2): [12.00, 11.00, 12.50],                       # 3
    ("2026-02-01", 5, 1): [9.00, 8.50, 9.50, 8.00],                    # 4 — slight increase
    ("2026-02-01", 5, 2): [7.50, 8.00],                                # 2 — Westside drop
    ("2026-02-01", 7, 2): [7.00, 7.50, 6.00],                          # 3 — slight Awan dip
    ("2026-02-01", 7, 1): [],                                          # 0

    # ══ MAR 2026 — recovery + DM8880 SPIKE (story driver) ════════════════
    ("2026-03-01", 3, 1): [13.50, 14.00, 13.00, 14.50, 13.00, 14.00,
                            15.00, 13.50, 14.00, 13.00],                # 10!! — the spike
    ("2026-03-01", 3, 2): [12.00, 13.00, 12.50, 14.00],                # 4
    ("2026-03-01", 5, 1): [8.50, 9.00],                                # 2 — PM8880 DROPPED
    ("2026-03-01", 5, 2): [],                                          # 0 — gone entirely Westside
    ("2026-03-01", 7, 2): [7.00, 6.50, 7.50, 6.00, 7.00],              # 5 — Awan up
    ("2026-03-01", 7, 1): [6.50],                                      # 1
    # ORPHAN: 1 visit Mar 2026 with promo_id=999 (not in catalog)
    # Discount kept in normal range ($12) so orphan proves NULL handling
    # without skewing top-N rankings (Q10/Q11/Q24).
    ("2026-03-01", 999, 1): [12.00],                                   # the orphan

    # ══ APR 2026 (PARTIAL — for "this month" / MTD context) ═══════════════
    # 2 weeks of data — partial, won't appear as a "complete month"
    # but supports MTD questions if/when we route them
    ("2026-04-01", 3, 1): [13.00, 14.00, 13.50],                       # 3 in first half
    ("2026-04-01", 3, 2): [12.50],                                     # 1
    ("2026-04-01", 5, 1): [9.00],                                      # 1
    ("2026-04-01", 7, 2): [7.00, 6.50],                                # 2
}


# ─────────────────────────────────────────────────────────────────────────────
# Total visits per month (matches appointments/revenue fixtures for biz 42)
# Pulled from appointments_fixtures + appointments_fixtures_2026
# ─────────────────────────────────────────────────────────────────────────────
# Used to compute promo_visit_pct (denominator)

_TOTAL_VISITS_BY_MONTH = {
    "2025-11-01": 235,    # synthetic — extrapolated from 2025 trend
    "2025-12-01": 248,    # synthetic
    "2026-01-01": 245,    # synthetic
    "2026-02-01": 234,    # from appointments_fixtures_2026 (completed_count)
    "2026-03-01": 247,    # from appointments_fixtures_2026 (completed_count)
    "2026-04-01": 120,    # partial (15 days)
}

_PERIODS = sorted(set(p for p, _, _ in _REDEMPTIONS_RAW.keys()))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _summarise_per_period():
    """Aggregate redemptions per period across all codes/locations."""
    by_period = defaultdict(lambda: {"redemptions": 0, "discount": 0.0,
                                       "discounts_list": [], "codes": set()})
    for (period, promo_id, _loc), discounts in _REDEMPTIONS_RAW.items():
        if not discounts:
            continue
        by_period[period]["redemptions"] += len(discounts)
        by_period[period]["discount"] += sum(discounts)
        by_period[period]["discounts_list"].extend(discounts)
        if promo_id != 999:  # don't count orphan in distinct_codes_used? actually count it.
            by_period[period]["codes"].add(promo_id)
        else:
            by_period[period]["codes"].add(promo_id)  # count orphan as a distinct used code
    return by_period


def _summarise_per_period_per_code():
    """Aggregate redemptions per (period, promo_id) — combining locations."""
    out = defaultdict(lambda: {"redemptions": 0, "discounts_list": []})
    for (period, promo_id, _loc), discounts in _REDEMPTIONS_RAW.items():
        if not discounts:
            continue
        out[(period, promo_id)]["redemptions"] += len(discounts)
        out[(period, promo_id)]["discounts_list"].extend(discounts)
    return out


def _summarise_per_code_window():
    """Aggregate redemptions per promo_id over the entire window."""
    out = defaultdict(lambda: {"redemptions": 0, "discounts_list": []})
    for (_period, promo_id, _loc), discounts in _REDEMPTIONS_RAW.items():
        if not discounts:
            continue
        out[promo_id]["redemptions"] += len(discounts)
        out[promo_id]["discounts_list"].extend(discounts)
    return out


def _summarise_per_period_per_location():
    """Aggregate redemptions per (period, location_id)."""
    out = defaultdict(lambda: {"redemptions": 0, "discounts_list": [],
                                "codes": set()})
    for (period, promo_id, loc), discounts in _REDEMPTIONS_RAW.items():
        if not discounts:
            continue
        out[(period, loc)]["redemptions"] += len(discounts)
        out[(period, loc)]["discounts_list"].extend(discounts)
        out[(period, loc)]["codes"].add(promo_id)
    return out


def _r(x, n=2):
    """Round helper."""
    return round(x, n) if x is not None else None


# ─────────────────────────────────────────────────────────────────────────────
# EP1: /api/v1/leo/promos/monthly — Monthly Promo Performance Rollup
# Powers: Q1, Q2, Q4, Q5, Q6, Q7, Q8, Q12, Q26
# ─────────────────────────────────────────────────────────────────────────────

def _build_monthly_summary():
    rows = []
    summary = _summarise_per_period()
    prev_redemptions = None
    prev_discount = None

    for period in _PERIODS:
        # Skip Apr 2026 in monthly summary — it's partial, not a complete month.
        # Per L2 (MTD partial-hot), only complete months in this aggregate.
        if period == "2026-04-01":
            continue

        s = summary[period]
        total_visits = _TOTAL_VISITS_BY_MONTH[period]
        promo_redemptions = s["redemptions"]
        total_discount = sum(s["discounts_list"])
        avg_discount = (sum(s["discounts_list"]) / len(s["discounts_list"])
                        if s["discounts_list"] else None)
        promo_visit_pct = (promo_redemptions / total_visits * 100
                           if total_visits else 0)

        rows.append({
            "period_month":                period,
            "total_visits":                total_visits,
            "promo_redemptions":           promo_redemptions,
            "distinct_codes_used":         len(s["codes"]),
            "promo_visit_pct":             _r(promo_visit_pct),
            "total_discount_given":        _r(total_discount),
            "avg_discount_per_redemption": _r(avg_discount),
            "prev_month_redemptions":      prev_redemptions,
            "prev_month_discount":         _r(prev_discount) if prev_discount is not None else None,
        })

        prev_redemptions = promo_redemptions
        prev_discount = total_discount

    return rows


MONTHLY_SUMMARY = {
    "business_id":  42,
    "period_start": WINDOW_START,
    "period_end":   "2026-03-31",
    "generated_at": "2026-04-21T15:00:00Z",
    "data":         _build_monthly_summary(),
}


# ─────────────────────────────────────────────────────────────────────────────
# EP2: /api/v1/leo/promos/codes — Per-Code Performance
# Powers: Q3, Q9, Q10, Q11, Q13, Q14, Q15, Q24, Q25
# Two granularities: monthly + window
# ─────────────────────────────────────────────────────────────────────────────

def _code_meta(promo_id):
    """Get promo metadata from catalog. Returns NULLs for orphans."""
    if promo_id in _PROMO_CATALOG:
        c = _PROMO_CATALOG[promo_id]
        return {
            "promo_code_string":     c["code"],
            "promo_label":           c["label"],
            "promo_amount_metadata": c["amount"],
            "is_active":             c["active"],
            "expiration_date":       c["expires"],
        }
    # Orphan (e.g. 999)
    return {
        "promo_code_string":     None,
        "promo_label":           None,
        "promo_amount_metadata": None,
        "is_active":             None,
        "expiration_date":       None,
    }


def _build_codes_monthly():
    """Per (period, promo_id) granularity — drives Q14, Q15."""
    rows = []
    by_pp = _summarise_per_period_per_code()

    # Skip Apr 2026 partial month for monthly granularity
    sorted_keys = sorted(
        [(p, pid) for (p, pid) in by_pp.keys() if p != "2026-04-01"],
        key=lambda x: (x[0], -by_pp[x]["redemptions"])
    )

    for (period, promo_id) in sorted_keys:
        s = by_pp[(period, promo_id)]
        meta = _code_meta(promo_id)
        discounts = s["discounts_list"]

        rows.append({
            "period_month":         period,
            "promo_id":             promo_id,
            **meta,
            "redemptions":          s["redemptions"],
            "total_discount":       _r(sum(discounts)),
            "avg_discount":         _r(sum(discounts) / len(discounts)) if discounts else None,
            "max_single_discount":  _r(max(discounts)) if discounts else None,
        })

    return rows


def _build_codes_window():
    """Per promo_id over the entire window — drives Q3, Q9 (window), Q10, Q11, Q24."""
    rows = []
    by_code = _summarise_per_code_window()

    # Include codes the tenant has used (any redemption in window)
    # ALSO include 0-redemption codes that are in their catalog?
    # Per spec PR2 Part B reads from promo_visits CTE — only codes with
    # ≥1 redemption appear. So DM881 + POFL99 (zero in window) won't show up here.
    # That's correct — they're surfaced via EP4 (catalog-health) instead.

    today = date.fromisoformat(TODAY_STR)

    for promo_id in sorted(by_code.keys(), key=lambda pid: -by_code[pid]["redemptions"]):
        s = by_code[promo_id]
        meta = _code_meta(promo_id)
        discounts = s["discounts_list"]

        # is_expired_now flag
        if meta["expiration_date"]:
            is_expired = 1 if date.fromisoformat(meta["expiration_date"]) < today else 0
        else:
            is_expired = None

        rows.append({
            "promo_id":             promo_id,
            **meta,
            "total_redemptions":    s["redemptions"],
            "total_discount":       _r(sum(discounts)),
            "avg_discount":         _r(sum(discounts) / len(discounts)) if discounts else None,
            "max_single_discount":  _r(max(discounts)) if discounts else None,
            "is_expired_now":       is_expired,
        })

    return rows


CODES_MONTHLY = {
    "business_id":  42,
    "period_start": WINDOW_START,
    "period_end":   "2026-03-31",
    "granularity":  "monthly",
    "generated_at": "2026-04-21T15:00:00Z",
    "data":         _build_codes_monthly(),
}

CODES_WINDOW = {
    "business_id":  42,
    "period_start": WINDOW_START,
    "period_end":   "2026-03-31",
    "granularity":  "window",
    "generated_at": "2026-04-21T15:00:00Z",
    "data":         _build_codes_window(),
}


# ─────────────────────────────────────────────────────────────────────────────
# EP3: /api/v1/leo/promos/locations — Per-Location Breakdown
# Powers: Q18, Q19, Q20, Q21
# Two shapes: by_code + rollup
# ─────────────────────────────────────────────────────────────────────────────

def _build_locations_by_code():
    """Per (period, location, code) — drives Q20 detailed cross."""
    rows = []
    for (period, promo_id, loc), discounts in _REDEMPTIONS_RAW.items():
        if not discounts or period == "2026-04-01":
            continue
        meta = _code_meta(promo_id)
        rows.append({
            "period_month":     period,
            "location_id":      loc,
            "location_name":    _LOC_NAMES[loc],
            "promo_id":         promo_id,
            "promo_code_string": meta["promo_code_string"],
            "promo_label":      meta["promo_label"],
            "redemptions":      len(discounts),
            "total_discount":   _r(sum(discounts)),
            "avg_discount":     _r(sum(discounts) / len(discounts)),
        })

    rows.sort(key=lambda r: (r["period_month"], r["location_id"], -r["redemptions"]),
              reverse=False)
    return rows


def _build_locations_rollup():
    """Per (period, location) — drives Q18, Q19, Q21."""
    rows = []
    by_pl = _summarise_per_period_per_location()

    for (period, loc) in sorted(by_pl.keys()):
        if period == "2026-04-01":
            continue
        s = by_pl[(period, loc)]
        discounts = s["discounts_list"]

        rows.append({
            "period_month":                  period,
            "location_id":                   loc,
            "location_name":                 _LOC_NAMES[loc],
            "total_promo_redemptions":       s["redemptions"],
            "distinct_codes_used":           len(s["codes"]),
            "total_discount_given":          _r(sum(discounts)),
            "avg_discount_per_redemption":   _r(sum(discounts) / len(discounts))
                                              if discounts else None,
        })

    rows.sort(key=lambda r: (r["period_month"], -r["total_discount_given"]))
    return rows


LOCATIONS_BY_CODE = {
    "business_id":  42,
    "period_start": WINDOW_START,
    "period_end":   "2026-03-31",
    "shape":        "by_code",
    "generated_at": "2026-04-21T15:00:00Z",
    "data":         _build_locations_by_code(),
}

LOCATIONS_ROLLUP = {
    "business_id":  42,
    "period_start": WINDOW_START,
    "period_end":   "2026-03-31",
    "shape":        "rollup",
    "generated_at": "2026-04-21T15:00:00Z",
    "data":         _build_locations_rollup(),
}


# ─────────────────────────────────────────────────────────────────────────────
# EP4: /api/v1/leo/promos/catalog-health — Lifecycle / Health Snapshot
# Powers: Q22, Q23
# ─────────────────────────────────────────────────────────────────────────────

def _build_catalog_health():
    """
    Surfaces ALL codes the tenant has EVER touched (full history, per spec).
    For mock world, "ever touched" = appears in _REDEMPTIONS_RAW + dormant
    codes the tenant has in their catalog (DM881, POFL99) even if 0 redemptions.

    Per spec, the tenant_codes CTE pulls from tbl_visit (full history).
    For codes that exist in their catalog but were never redeemed:
      - In prod: only appears here if they redeemed it AT LEAST ONCE in tbl_visit
      - In our mock: we add DM881 + POFL99 manually because the spec's intent
        is to surface catalog problems, and a code the tenant has never used
        is interesting too (especially if it's expired but active).
    """
    today = date.fromisoformat(TODAY_STR)
    cutoff_90d = date(2026, 1, 1)  # 90 days before TODAY_STR (close enough)

    # Codes that appear in redemptions (window only)
    redeemed_codes = set()
    redemptions_last_90d_by_code = defaultdict(int)
    for (period, promo_id, _loc), discounts in _REDEMPTIONS_RAW.items():
        if not discounts:
            continue
        redeemed_codes.add(promo_id)
        # Count toward 90d if period >= cutoff
        period_date = date.fromisoformat(period)
        if period_date >= cutoff_90d:
            redemptions_last_90d_by_code[promo_id] += len(discounts)

    # Tenant catalog: in mock world, the tenant "owns" the codes they've used
    # PLUS the dormant ones (DM881, POFL99) we manually flag as in their catalog.
    # In prod the tenant_codes CTE would only include codes with ≥1 redemption
    # ever. Since DM881 and POFL99 have 0 redemptions in our window, they
    # technically wouldn't appear via the spec's CTE. To exercise Q22 + Q23
    # we either (a) modify the spec to also include catalog-listed codes, or
    # (b) manually inject DM881 + POFL99 into "tenant codes" for mock testing.
    # We pick (b) — Step 7 documents the limitation and we revisit at sign-off.
    tenant_codes = redeemed_codes | {1, 4}  # add POFL99 and DM881 manually

    rows = []
    for promo_id in sorted(tenant_codes):
        meta = _code_meta(promo_id)
        if meta["expiration_date"]:
            is_expired = 1 if date.fromisoformat(meta["expiration_date"]) < today else 0
        else:
            is_expired = None

        active_but_expired = (1 if (meta["is_active"] == 1 and is_expired == 1)
                              else 0)

        recent = redemptions_last_90d_by_code.get(promo_id, 0)
        is_dormant = 1 if (meta["is_active"] == 1 and recent == 0) else 0

        rows.append({
            "promo_id":             promo_id,
            "promo_code_string":    meta["promo_code_string"],
            "promo_label":          meta["promo_label"],
            "is_active":            meta["is_active"],
            "expiration_date":      meta["expiration_date"],
            "is_expired":           is_expired,
            "active_but_expired":   active_but_expired,
            "redemptions_last_90d": recent,
            "is_dormant":           is_dormant,
        })

    return rows


CATALOG_HEALTH = {
    "business_id":  42,
    "generated_at": "2026-04-21T15:00:00Z",
    "ref_date":     TODAY_STR,
    "data":         _build_catalog_health(),
}


# ─────────────────────────────────────────────────────────────────────────────
# Business 99 — minimal rows for tenant isolation testing
# ─────────────────────────────────────────────────────────────────────────────
# IDs deliberately don't overlap with biz 42's promo_ids.
# These should NEVER appear when business_id=42 is requested.

MONTHLY_SUMMARY_99 = {
    "business_id":  99,
    "period_start": "2026-03-01",
    "period_end":   "2026-03-31",
    "generated_at": "2026-04-21T15:00:00Z",
    "data": [
        {
            "period_month":                "2026-03-01",
            "total_visits":                85,
            "promo_redemptions":           4,
            "distinct_codes_used":         1,
            "promo_visit_pct":             4.71,
            "total_discount_given":        45.00,
            "avg_discount_per_redemption": 11.25,
            "prev_month_redemptions":      None,
            "prev_month_discount":         None,
        },
    ],
}

CODES_WINDOW_99 = {
    "business_id":  99,
    "period_start": "2026-03-01",
    "period_end":   "2026-03-31",
    "granularity":  "window",
    "generated_at": "2026-04-21T15:00:00Z",
    "data": [
        {
            "promo_id":             8001,        # ← biz 99's promo ID, no overlap with biz 42
            "promo_code_string":    "BIZ99CODE",
            "promo_label":          "Biz 99 special",
            "promo_amount_metadata": 5.00,
            "is_active":            1,
            "expiration_date":      "2026-12-31",
            "total_redemptions":    4,
            "total_discount":       45.00,
            "avg_discount":         11.25,
            "max_single_discount":  12.00,
            "is_expired_now":       0,
        },
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Fixture lookup — endpoint path → response
# ─────────────────────────────────────────────────────────────────────────────
# Note: codes + locations endpoints have query-param-driven shapes.
# Mock server reads the body to pick the right fixture variant — see
# mock_analytics_server.py wiring update.

FIXTURES: dict[str, dict] = {
    "/api/v1/leo/promos/monthly":              MONTHLY_SUMMARY,
    "/api/v1/leo/promos/codes":                CODES_MONTHLY,    # default; window via param
    "/api/v1/leo/promos/codes-window":         CODES_WINDOW,     # secondary path for shape switching
    "/api/v1/leo/promos/locations":            LOCATIONS_ROLLUP, # default; by_code via param
    "/api/v1/leo/promos/locations-by-code":    LOCATIONS_BY_CODE,
    "/api/v1/leo/promos/catalog-health":       CATALOG_HEALTH,
}


__all__ = [
    "MONTHLY_SUMMARY", "MONTHLY_SUMMARY_99",
    "CODES_MONTHLY", "CODES_WINDOW", "CODES_WINDOW_99",
    "LOCATIONS_ROLLUP", "LOCATIONS_BY_CODE",
    "CATALOG_HEALTH",
    "FIXTURES",
]


# ─────────────────────────────────────────────────────────────────────────────
# Self-test — verify every Step 1 question has a data answer
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 78)
    print("PROMOS FIXTURE SELF-TEST")
    print("=" * 78)

    # Q1: redemptions last month (Mar 2026)
    mar = next(r for r in MONTHLY_SUMMARY["data"] if r["period_month"] == "2026-03-01")
    print(f"\nQ1  Redemptions Mar 2026: {mar['promo_redemptions']} (expected ~22)")
    print(f"Q2  Total discount Mar 2026: ${mar['total_discount_given']}")
    print(f"Q12 Promo visit % Mar 2026: {mar['promo_visit_pct']}%")
    print(f"Q25 Avg discount/redemption Mar: ${mar['avg_discount_per_redemption']}")

    # Q5: MoM
    feb = next(r for r in MONTHLY_SUMMARY["data"] if r["period_month"] == "2026-02-01")
    print(f"\nQ5  Mar vs Feb: {mar['promo_redemptions']} vs {feb['promo_redemptions']} "
          f"(prev_month_redemptions on Mar row = {mar['prev_month_redemptions']})")

    # Q7: best month
    best = max(MONTHLY_SUMMARY["data"], key=lambda r: r["promo_redemptions"])
    print(f"\nQ7  Best month: {best['period_month']} ({best['promo_redemptions']} redemptions)")

    # Q3: distinct codes used in window
    all_codes_used = set()
    for r in CODES_WINDOW["data"]:
        all_codes_used.add(r["promo_id"])
    print(f"\nQ3  Distinct codes in window: {len(all_codes_used)} "
          f"(expected 4: DM8880, PM8880, Awan, orphan999)")

    # Q9: most redeemed last month
    mar_codes = sorted(
        [r for r in CODES_MONTHLY["data"] if r["period_month"] == "2026-03-01"],
        key=lambda r: -r["redemptions"]
    )
    if mar_codes:
        print(f"\nQ9  Most redeemed Mar 2026: {mar_codes[0]['promo_code_string']} "
              f"({mar_codes[0]['redemptions']} redemptions)")

    # Q10/Q11: top by total/avg discount (window)
    top_total = max(CODES_WINDOW["data"], key=lambda r: r["total_discount"])
    top_avg = max(CODES_WINDOW["data"], key=lambda r: r["avg_discount"])
    print(f"\nQ10 Top by total discount: {top_total['promo_code_string']} "
          f"(${top_total['total_discount']})")
    print(f"Q11 Top by avg discount: {top_avg['promo_code_string']} "
          f"(${top_avg['avg_discount']} avg)")

    # Q14/Q15: spike + drop
    print(f"\nQ14 DM8880 monthly progression:")
    for r in CODES_MONTHLY["data"]:
        if r["promo_code_string"] == "DM8880":
            print(f"    {r['period_month']}: {r['redemptions']} redemptions, "
                  f"${r['total_discount']} discount")

    print(f"\nQ15 PM8880 monthly progression (should show Feb→Mar drop):")
    for r in CODES_MONTHLY["data"]:
        if r["promo_code_string"] == "PM8880":
            print(f"    {r['period_month']}: {r['redemptions']} redemptions, "
                  f"${r['total_discount']} discount")

    # Q18/Q19/Q21: location breakdown
    print(f"\nQ18 Discount per location Mar 2026:")
    for r in LOCATIONS_ROLLUP["data"]:
        if r["period_month"] == "2026-03-01":
            print(f"    {r['location_name']}: {r['total_promo_redemptions']} redemptions, "
                  f"${r['total_discount_given']} discount")

    # Q19 vs Q21 — different answers test
    by_loc_redemptions = defaultdict(int)
    by_loc_discount = defaultdict(float)
    for r in LOCATIONS_ROLLUP["data"]:
        by_loc_redemptions[r["location_name"]] += r["total_promo_redemptions"]
        by_loc_discount[r["location_name"]] += r["total_discount_given"]

    print(f"\nQ19 Most redemptions (count): "
          f"{max(by_loc_redemptions, key=by_loc_redemptions.get)} "
          f"({max(by_loc_redemptions.values())})")
    print(f"Q21 Most discount ($) given: "
          f"{max(by_loc_discount, key=by_loc_discount.get)} "
          f"(${_r(max(by_loc_discount.values()))})")

    # Q22/Q23: catalog health
    print(f"\nQ22 Dormant codes: " + ", ".join(
        r["promo_code_string"] or f"orphan{r['promo_id']}"
        for r in CATALOG_HEALTH["data"] if r["is_dormant"]
    ))
    print(f"Q23 Active-but-expired codes: " + ", ".join(
        r["promo_code_string"] or f"orphan{r['promo_id']}"
        for r in CATALOG_HEALTH["data"] if r["active_but_expired"]
    ))

    # Q24: biggest single discount
    biggest = max(CODES_WINDOW["data"], key=lambda r: r["max_single_discount"] or 0)
    print(f"\nQ24 Biggest single discount: ${biggest['max_single_discount']} "
          f"(via {biggest['promo_code_string']})")

    # Orphan handling
    orphan = [r for r in CODES_WINDOW["data"] if r["promo_id"] == 999]
    if orphan:
        o = orphan[0]
        print(f"\n✓ Orphan 999: code_string={o['promo_code_string']}, "
              f"label={o['promo_label']}, is_active={o['is_active']} "
              f"(all NULL as expected)")

    # Cross-check: monthly redemptions = sum of per-code redemptions per month
    print(f"\n--- Cross-check: monthly totals match per-code sums ---")
    by_pp = _summarise_per_period_per_code()
    for period in _PERIODS:
        if period == "2026-04-01":
            continue
        m_row = next((r for r in MONTHLY_SUMMARY["data"]
                      if r["period_month"] == period), None)
        if m_row is None:
            continue
        sum_redemptions = sum(s["redemptions"] for (p, _), s in by_pp.items() if p == period)
        sum_discount = sum(sum(s["discounts_list"]) for (p, _), s in by_pp.items() if p == period)
        ok_red = "✓" if m_row["promo_redemptions"] == sum_redemptions else "✗"
        ok_dis = "✓" if abs(m_row["total_discount_given"] - sum_discount) < 0.01 else "✗"
        print(f"    {period}: redemptions {m_row['promo_redemptions']}={sum_redemptions} {ok_red}, "
              f"discount ${m_row['total_discount_given']}=${_r(sum_discount)} {ok_dis}")

    # Cross-check: location rollup sums match monthly totals
    print(f"\n--- Cross-check: location rollup sums match monthly ---")
    for period in _PERIODS:
        if period == "2026-04-01":
            continue
        m_row = next((r for r in MONTHLY_SUMMARY["data"]
                      if r["period_month"] == period), None)
        if m_row is None:
            continue
        loc_red = sum(r["total_promo_redemptions"] for r in LOCATIONS_ROLLUP["data"]
                      if r["period_month"] == period)
        loc_dis = sum(r["total_discount_given"] for r in LOCATIONS_ROLLUP["data"]
                      if r["period_month"] == period)
        ok_red = "✓" if m_row["promo_redemptions"] == loc_red else "✗"
        ok_dis = "✓" if abs(m_row["total_discount_given"] - loc_dis) < 0.01 else "✗"
        print(f"    {period}: redemptions {m_row['promo_redemptions']}={loc_red} {ok_red}, "
              f"discount ${m_row['total_discount_given']}=${_r(loc_dis)} {ok_dis}")

    print("\n" + "=" * 78)
    print("SELF-TEST COMPLETE")
    print("=" * 78)