"""
tests/mocks/expenses_fixtures.py
=================================
Realistic mock response data for the 6 Expenses endpoints:
  EP1: /api/v1/leo/expenses/monthly-summary          → MONTHLY_SUMMARY
  EP2: /api/v1/leo/expenses/category-breakdown        → CATEGORY_BREAKDOWN
  EP3: /api/v1/leo/expenses/location-breakdown        → LOCATION_BREAKDOWN
  EP4: /api/v1/leo/expenses/payment-type-breakdown    → PAYMENT_TYPE_BREAKDOWN
  EP5: /api/v1/leo/expenses/staff-attribution         → STAFF_ATTRIBUTION
  EP6: /api/v1/leo/expenses/category-location-cross   → CATEGORY_LOCATION_CROSS

Based on the same salon business (business_id=42) with 2 locations (Main St +
Westside) and 4 staff, consistent with revenue/appointments/staff/services/
clients fixtures.

── Time window ───────────────────────────────────────────────────────────────
6 months: 2025-10-01 through 2026-03-31. "last month" = Mar 2026.
This gives:
  • Complete 2025-Q4 and 2026-Q1 (for QoQ math)
  • 3-month baselines achievable from Dec 2025 onward
  • Office/Admin dormancy triggerable (Jan-Feb-Mar 2026 silent)

── Consistency anchors with existing fixtures ────────────────────────────────
• business_id=42, locations 1=Main St, 2=Westside (same as other domains)
• Staff: 12=Maria Lopez (admin), 15=James Carter, 9=Aisha Nwosu, 21=Tom Rivera
  - Maria logs most expenses (admin role, matches dev-DB AddedBy=3 pattern
    where one person logs most entries for a tenant)
  - James fills in during Feb 2026 stress period
  - Tom (deactivated after Jun 2025) has zero expense entries in any period here
• Feb 2026 = weak revenue month (matches other fixtures) → Marketing spike as
  reactive "panic-ran ads" response; correlates with Feb revenue recovery push

── Category catalog used (biz 42) ────────────────────────────────────────────
Using real CategoryIDs observed in dev-DB sample (tbl_expense_category):
  13  Payroll & Commissions    (~18%)
  14  Rent & Utilities         (~34%)   ← biggest category
  15  Marketing                (~8%)    ← spikes Feb 2026
  17  Software & Subscriptions (~5%)
  18  Office/Admin             (~4%)    ← GOES DORMANT after Dec 2025 (Q28)
  20  Insurance                (~4%)
  24  Products & Supplies      (~22%)
  32  Equipment                (~5%)    ← spikes Dec 2025 (holiday chair)

── Stories baked in ──────────────────────────────────────────────────────────
1. MARKETING SPIKE (Feb 2026): $640 vs 3-mo baseline $350 = +82.9% spike.
   Drives Q22/Q24. AI should say "Marketing spiked in Feb — panic-ads after
   the weak revenue month?"
2. EQUIPMENT SPIKE (Dec 2025): $1,040 vs baseline $152 = +582% spike.
   Includes a $780 new salon chair from "Salon Equipment Pro".
   Drives Q20 explanation ("Dec was up because you bought a $780 chair").
3. OFFICE/ADMIN DORMANCY (Jan-Feb-Mar 2026): zero spending after $170/$230/$250
   run in Oct-Dec 2025. Drives Q28 dormant detection (doc-layer logic).
4. QoQ DECLINE: 2025-Q4 ($13,800) → 2026-Q1 ($12,810) = -7.2%. Drives Q8/Q21.
5. STAFF CONCENTRATION: Only Maria (12) logs expenses in most months. James
   (15) fills in 4 entries in Feb 2026 (the stress month). Drives Q26.
6. NEAR-DUPLICATE (Mar 2026): Within Products & Supplies, two $50 "Zumba Class"
   entries dated 2026-03-15 and 2026-03-18. NOT exposed at aggregate level —
   the AI should refuse Q29 honestly ("I can't detect duplicates from the
   monthly aggregates I have"). Documented here so the ETL implementer knows
   the dupe exists at the row level if they ever add a transaction endpoint.
7. MISCATEGORIZED ROW (Mar 2026): One $60 electricity bill was logged under
   Products & Supplies (24) instead of Rent & Utilities (14). This matches
   the dev-DB pattern where "Zumba Class" appeared under 3 different
   CategoryIDs. Surfaces as slight over-counting in Products and
   under-counting in Rent for Mar — both within noise, not flaggable.
   Reminder of why we tell users "category totals reflect what was logged,
   not semantic truth."

── Vendor mix (per user decision) ────────────────────────────────────────────
Realistic vendor names for fixed-cost categories (Rent, Utilities, Insurance,
Software) — e.g. "Sunset Plaza Realty", "FPL Energy", "State Farm Insurance".
Generic labels for supplies/marketing/etc — e.g. "Product restock", "Ad spend".
This matches real salon data where utilities have vendor statements but
supply purchases are often typed loosely.

Vendor names only surface in the free-text Description column of the raw
table; they are NOT returned by any aggregate endpoint. They're documented
here for fixture clarity and Step 4 ETL doc-generator reference only.

── Monthly total budget ──────────────────────────────────────────────────────
  Oct 2025: $4,100  (baseline)
  Nov 2025: $4,280  (+4.4% MoM)
  Dec 2025: $5,420  (+26.6% MoM — holiday + equipment buy)
  Jan 2026: $3,890  (-28.2% MoM — post-holiday dip)
  Feb 2026: $4,600  (+18.3% MoM — marketing push)
  Mar 2026: $4,320  (-6.1% MoM — normalizing)
  Window total: $26,610
  2025-Q4 total: $13,800 (complete)
  2026-Q1 total: $12,810 (complete, -7.2% QoQ)
  2026 YTD through Mar: $12,810
"""

from __future__ import annotations


# ─────────────────────────────────────────────────────────────────────────────
# Monthly skeleton — drives all downstream breakdowns
# ─────────────────────────────────────────────────────────────────────────────

_MONTHLY_TOTALS = {
    "2025-10-01": 4100.00,
    "2025-11-01": 4280.00,
    "2025-12-01": 5420.00,
    "2026-01-01": 3890.00,
    "2026-02-01": 4600.00,
    "2026-03-01": 4320.00,
}

_CATEGORY_NAMES = {
    13: "Payroll & Commissions",
    14: "Rent & Utilities",
    15: "Marketing",
    17: "Software & Subscriptions",
    18: "Office/Admin",
    20: "Insurance",
    24: "Products & Supplies",
    32: "Equipment",
}

# Category totals per month. Absent key ⇒ no activity that month (triggers
# dormant detection in doc layer).
_CAT_MONTHLY = {
    "2025-10-01": {14:1450.00, 24: 900.00, 13: 740.00, 15: 325.00, 17: 210.00, 20: 160.00, 18: 170.00, 32: 145.00},
    "2025-11-01": {14:1450.00, 24: 950.00, 13: 780.00, 15: 340.00, 17: 210.00, 20: 160.00, 18: 230.00, 32: 160.00},
    "2025-12-01": {14:1500.00, 24:1050.00, 13: 820.00, 15: 390.00, 17: 210.00, 20: 160.00, 18: 250.00, 32:1040.00},
    "2026-01-01": {14:1460.00, 24: 820.00, 13: 720.00, 15: 320.00, 17: 210.00, 20: 160.00,              32: 200.00},
    "2026-02-01": {14:1480.00, 24: 920.00, 13: 750.00, 15: 640.00, 17: 210.00, 20: 160.00,              32: 440.00},
    "2026-03-01": {14:1410.00, 24:1040.00, 13: 760.00, 15: 370.00, 17: 210.00, 20: 160.00,              32: 370.00},
}

_PERIODS = list(_MONTHLY_TOTALS.keys())   # sorted ascending


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _mom(curr, prev):
    """Return (mom_change_pct, mom_direction). None/None when prev absent."""
    if prev is None or prev == 0:
        return None, None
    pct = round((curr - prev) / prev * 100, 2)
    direction = "up" if curr > prev else "down" if curr < prev else "flat"
    return pct, direction


def _anomaly(curr, prior_values):
    """
    Given the current value and a list of prior months' values for the same
    category, compute (baseline_3mo_avg, baseline_months_available,
    pct_vs_baseline, anomaly_flag).
    """
    window = prior_values[-3:] if len(prior_values) >= 3 else prior_values
    n = len(window)
    if n < 2:
        return None, n, None, None
    b = round(sum(window) / len(window), 2)
    if b <= 0:
        return b, n, None, None
    pct = round((curr - b) / b * 100, 2)
    if   pct >=  50: flag = "spike"
    elif pct >=  20: flag = "elevated"
    elif pct >  -20: flag = "normal"
    elif pct >  -50: flag = "low"
    else:            flag = "unusual_low"
    return b, n, pct, flag


# ─────────────────────────────────────────────────────────────────────────────
# EP1: /api/v1/leo/expenses/monthly-summary
# Powers: Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q20, Q21, Q25, S1, S4
# ─────────────────────────────────────────────────────────────────────────────

def _build_monthly_summary():
    rows = []
    totals_list = [_MONTHLY_TOTALS[p] for p in _PERIODS]

    # Quarter totals (calendar) — for current/prev/QoQ
    from collections import defaultdict
    q_totals = defaultdict(lambda: [0.0, 0])
    for p, t in _MONTHLY_TOTALS.items():
        y, m, _ = p.split("-")
        q = (int(m) - 1) // 3 + 1
        q_totals[(int(y), q)][0] += t
        q_totals[(int(y), q)][1] += 1

    def _q_of(period):
        y, m, _ = period.split("-")
        return (int(y), (int(m) - 1) // 3 + 1)

    def _prev_q(y, q):
        return (y - 1, 4) if q == 1 else (y, q - 1)

    running_window = 0.0
    running_ytd_2026 = 0.0

    # 2025-YTD at window start: Jan-Sep 2025 not in window. For 2025 rows we
    # accumulate only what's present in the window. For 2026 rows we reset
    # on Jan 1 and accumulate. This matches the SQL PARTITION BY YEAR(...)
    # behavior — partition is per calendar year.
    running_ytd_2025 = 0.0

    for i, period in enumerate(_PERIODS):
        total = _MONTHLY_TOTALS[period]
        prev_total = _MONTHLY_TOTALS[_PERIODS[i - 1]] if i > 0 else None
        mom_pct, mom_dir = _mom(total, prev_total)

        year = int(period.split("-")[0])
        if year == 2026:
            running_ytd_2026 += total
            ytd_total = round(running_ytd_2026, 2)
        else:
            running_ytd_2025 += total
            ytd_total = round(running_ytd_2025, 2)

        running_window += total

        # Rank in window by total DESC
        rank = sorted(totals_list, reverse=True).index(total) + 1

        # Quarters
        y, q = _q_of(period)
        q_sum, q_months = q_totals[(y, q)]
        current_quarter_total = round(q_sum, 2) if q_months == 3 else None

        py, pq = _prev_q(y, q)
        prev_q_sum, prev_q_months = q_totals.get((py, pq), (0.0, 0))
        prev_quarter_total = round(prev_q_sum, 2) if prev_q_months == 3 else None

        if current_quarter_total is not None and prev_quarter_total and prev_quarter_total > 0:
            qoq_change_pct = round(
                (current_quarter_total - prev_quarter_total) / prev_quarter_total * 100,
                2
            )
        else:
            qoq_change_pct = None

        # Transaction counts — realistic, include story artifacts
        txn_counts = {
            "2025-10-01": 16, "2025-11-01": 17, "2025-12-01": 22,  # holiday +
            "2026-01-01": 15, "2026-02-01": 19,  # +4 ad spend entries
            "2026-03-01": 18,  # +2 dupes + 1 miscategorized
        }
        txn_count = txn_counts[period]
        avg_txn = round(total / txn_count, 2)
        min_txn = 12.00 if period == "2026-02-01" else 15.00 if period == "2026-03-01" else 25.00
        max_txn_by_period = {
            "2025-10-01": 1200.00,   # rent
            "2025-11-01": 1200.00,
            "2025-12-01":  780.00,   # the salon chair
            "2026-01-01": 1100.00,   # rent
            "2026-02-01":  640.00,   # big ad campaign
            "2026-03-01": 1200.00,   # rent
        }
        max_txn = max_txn_by_period[period]

        rows.append({
            "period":                 period,
            "total_expenses":         round(total, 2),
            "transaction_count":      txn_count,
            "avg_transaction":        avg_txn,
            "min_transaction":        min_txn,
            "max_transaction":        max_txn,
            "prev_month_expenses":    round(prev_total, 2) if prev_total is not None else None,
            "mom_change_pct":         mom_pct,
            "mom_direction":          mom_dir,
            "ytd_total":              ytd_total,
            "window_cumulative":      round(running_window, 2),
            "current_quarter_total":  current_quarter_total,
            "prev_quarter_total":     prev_quarter_total,
            "qoq_change_pct":         qoq_change_pct,
            "expense_rank_in_window": rank,
            "avg_monthly_in_window":  round(sum(totals_list) / len(totals_list), 2),
            "months_in_window":       len(_PERIODS),
            "large_txn_count":        0,   # no >$100K in realistic data
            "huge_txn_count":         0,   # no >$1M
        })

    # Return newest first (period DESC) — matches API spec default sort
    rows.reverse()
    return rows


MONTHLY_SUMMARY = {
    "business_id":  42,
    "period_start": "2025-10-01",
    "period_end":   "2026-03-31",
    "generated_at": "2026-04-21T14:32:00Z",
    "data":         _build_monthly_summary(),
}


# ─────────────────────────────────────────────────────────────────────────────
# EP2: /api/v1/leo/expenses/category-breakdown (with optional subcategories)
# Powers: Q9, Q10, Q11, Q12, Q13, Q20, Q21, Q22, Q23, Q24, Q28 (via doc layer)
# ─────────────────────────────────────────────────────────────────────────────

_CAT_TXN_COUNT = {
    14: {"2025-10-01":4, "2025-11-01":4, "2025-12-01":5, "2026-01-01":4, "2026-02-01":4, "2026-03-01":4},
    # Mar Products: 5 normal + 2 dupes + 1 miscategorized = 8 txns, up from typical 5-7
    24: {"2025-10-01":5, "2025-11-01":6, "2025-12-01":7, "2026-01-01":4, "2026-02-01":5, "2026-03-01":8},
    13: {"2025-10-01":2, "2025-11-01":2, "2025-12-01":3, "2026-01-01":2, "2026-02-01":2, "2026-03-01":2},
    # Feb marketing has 4 ad-spend entries (the spike)
    15: {"2025-10-01":1, "2025-11-01":2, "2025-12-01":2, "2026-01-01":1, "2026-02-01":4, "2026-03-01":1},
    17: {"2025-10-01":1, "2025-11-01":1, "2025-12-01":1, "2026-01-01":1, "2026-02-01":1, "2026-03-01":1},
    20: {"2025-10-01":1, "2025-11-01":1, "2025-12-01":1, "2026-01-01":1, "2026-02-01":1, "2026-03-01":1},
    18: {"2025-10-01":1, "2025-11-01":2, "2025-12-01":2},   # dormant after Dec
    # Dec equipment has 2 entries (regular supplies + salon chair)
    32: {"2025-10-01":1, "2025-11-01":1, "2025-12-01":2, "2026-01-01":1, "2026-02-01":2, "2026-03-01":1},
}


def _build_category_breakdown():
    rows = []
    for period in _PERIODS:
        month_total = _MONTHLY_TOTALS[period]
        cats_in_month = _CAT_MONTHLY[period]

        # Rank within month by category_total DESC
        sorted_cats = sorted(cats_in_month.items(), key=lambda kv: -kv[1])

        for rank_idx, (cat_id, cat_total) in enumerate(sorted_cats, start=1):
            prior_periods = [p for p in _PERIODS if p < period]
            prev_total_same_cat = None
            if prior_periods:
                last_p = prior_periods[-1]
                prev_total_same_cat = _CAT_MONTHLY.get(last_p, {}).get(cat_id)

            mom_pct, _ = _mom(cat_total, prev_total_same_cat)

            # Baseline sources: values from the 3 preceding months where
            # the category had activity. Per API spec, SQL window frame
            # ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING includes any row
            # present in the prior 3 months for this category.
            baseline_source = []
            for p in prior_periods[-3:]:
                if cat_id in _CAT_MONTHLY[p]:
                    baseline_source.append(_CAT_MONTHLY[p][cat_id])

            baseline_3mo_avg, baseline_months_available, pct_vs_baseline, anomaly_flag = \
                _anomaly(cat_total, baseline_source)

            row = {
                "period":                     period,
                "category_id":                cat_id,
                "category_name":              _CATEGORY_NAMES[cat_id],
                "category_total":             round(cat_total, 2),
                "transaction_count":          _CAT_TXN_COUNT[cat_id][period],
                "month_total":                round(month_total, 2),
                "pct_of_month":               round(cat_total / month_total * 100, 2),
                "rank_in_month":              rank_idx,
                "prev_month_total":           round(prev_total_same_cat, 2) if prev_total_same_cat is not None else None,
                "mom_change_pct":             mom_pct,
                "baseline_3mo_avg":           baseline_3mo_avg,
                "baseline_months_available":  baseline_months_available,
                "pct_vs_baseline":            pct_vs_baseline,
                "anomaly_flag":               anomaly_flag,
            }
            rows.append(row)

    # Subcategory drill-down for Rent & Utilities (14) in Mar 2026 — for Q13.
    # Only this one category gets subcats in the fixture; it demonstrates
    # the shape and is enough for acceptance testing.
    for row in rows:
        if row["period"] == "2026-03-01" and row["category_id"] == 14:
            row["subcategory_breakdown"] = [
                {"subcategory_id": 26, "subcategory_name": "Rent",
                 "subcategory_total": 1200.00, "transaction_count": 1, "rank_in_category": 1},
                {"subcategory_id": 27, "subcategory_name": "Electricity",
                 "subcategory_total":  140.00, "transaction_count": 1, "rank_in_category": 2},
                {"subcategory_id": 28, "subcategory_name": "Internet",
                 "subcategory_total":   70.00, "transaction_count": 1, "rank_in_category": 3},
                # Note: one $60 electricity bill was miscategorized to
                # Products (24) this month — so Rent & Utilities Electricity
                # shows $140 here, not $200.
            ]

    # Sort period DESC then rank ASC
    rows.sort(key=lambda r: r["rank_in_month"])
    rows.sort(key=lambda r: r["period"], reverse=True)
    return rows


CATEGORY_BREAKDOWN = {
    "business_id":  42,
    "period_start": "2025-10-01",
    "period_end":   "2026-03-31",
    "generated_at": "2026-04-21T14:32:00Z",
    "data":         _build_category_breakdown(),
}


# ─────────────────────────────────────────────────────────────────────────────
# EP3: /api/v1/leo/expenses/location-breakdown
# Powers: Q16, Q17, Q18, Q19 (with EP6), S3
# ─────────────────────────────────────────────────────────────────────────────

# Main St ~60%, Westside ~40%. Minor variance:
#   Dec Main St slightly higher (equipment chair was for Main St floor)
#   Feb Main St slightly higher (marketing ads drove Main St traffic)
# Sums cross-verified against monthly totals.

_LOC_SPLIT = {
    "2025-10-01": {1: (2460.00, 10), 2: (1640.00,  6)},
    "2025-11-01": {1: (2568.00, 10), 2: (1712.00,  7)},
    "2025-12-01": {1: (3420.00, 14), 2: (2000.00,  8)},   # chair went to Main St
    "2026-01-01": {1: (2334.00,  9), 2: (1556.00,  6)},
    "2026-02-01": {1: (2852.00, 12), 2: (1748.00,  7)},   # ads skewed Main St
    "2026-03-01": {1: (2592.00, 11), 2: (1728.00,  7)},
}

_LOC_NAMES = {1: "Main St", 2: "Westside"}


def _build_location_breakdown():
    rows = []
    for period in _PERIODS:
        month_total = _MONTHLY_TOTALS[period]
        loc_in_month = _LOC_SPLIT[period]

        # Sanity check — location sum must match monthly total
        assert abs(sum(v[0] for v in loc_in_month.values()) - month_total) < 0.01, \
            f"Location split for {period} doesn't sum to monthly total"

        sorted_locs = sorted(loc_in_month.items(), key=lambda kv: -kv[1][0])
        rank_by_loc = {loc_id: i + 1 for i, (loc_id, _) in enumerate(sorted_locs)}

        for loc_id, (loc_total, txn_count) in loc_in_month.items():
            prior_periods = [p for p in _PERIODS if p < period]
            prev_loc_total = None
            if prior_periods:
                prev_loc_total = _LOC_SPLIT[prior_periods[-1]].get(loc_id, (None, 0))[0]

            mom_pct, _ = _mom(loc_total, prev_loc_total)

            rows.append({
                "period":           period,
                "location_id":      loc_id,
                "location_name":    _LOC_NAMES[loc_id],
                "location_total":   round(loc_total, 2),
                "transaction_count": txn_count,
                "month_total":      round(month_total, 2),
                "pct_of_month":     round(loc_total / month_total * 100, 2),
                "rank_in_month":    rank_by_loc[loc_id],
                "prev_month_total": round(prev_loc_total, 2) if prev_loc_total is not None else None,
                "mom_change_pct":   mom_pct,
            })

    rows.sort(key=lambda r: r["rank_in_month"])
    rows.sort(key=lambda r: r["period"], reverse=True)
    return rows


LOCATION_BREAKDOWN = {
    "business_id":  42,
    "period_start": "2025-10-01",
    "period_end":   "2026-03-31",
    "generated_at": "2026-04-21T14:32:00Z",
    "data":         _build_location_breakdown(),
}


# ─────────────────────────────────────────────────────────────────────────────
# EP4: /api/v1/leo/expenses/payment-type-breakdown
# Powers: Q14, Q15
# ─────────────────────────────────────────────────────────────────────────────

# Enum confirmed 2026-04-21: 1=Cash, 2=Check, 3=Card.
# Split 80% Cash / 12% Check / 8% Card — matches dev-DB sample distribution.

_PAYMENT_LABELS = {1: "Cash", 2: "Check", 3: "Card"}
_PAY_SPLIT = {1: 0.80, 2: 0.12, 3: 0.08}


def _build_payment_type_breakdown():
    rows = []
    for period in _PERIODS:
        month_total = _MONTHLY_TOTALS[period]

        split = {code: round(month_total * frac, 2) for code, frac in _PAY_SPLIT.items()}
        # Apply rounding drift to Card (smallest)
        drift = round(month_total - sum(split.values()), 2)
        split[3] = round(split[3] + drift, 2)

        txn_total_by_period = {
            "2025-10-01": 16, "2025-11-01": 17, "2025-12-01": 22,
            "2026-01-01": 15, "2026-02-01": 19, "2026-03-01": 18,
        }
        tt = txn_total_by_period[period]
        cash_txn = round(tt * 0.75)
        check_txn = round(tt * 0.15)
        card_txn = tt - cash_txn - check_txn
        txn_split = {1: cash_txn, 2: check_txn, 3: card_txn}

        for code in (1, 2, 3):
            rows.append({
                "period":              period,
                "payment_type_code":   code,
                "payment_type_label":  _PAYMENT_LABELS[code],
                "type_total":          split[code],
                "transaction_count":   txn_split[code],
                "month_total":         round(month_total, 2),
                "pct_of_month":        round(split[code] / month_total * 100, 2),
            })

    rows.sort(key=lambda r: -r["type_total"])
    rows.sort(key=lambda r: r["period"], reverse=True)
    return rows


PAYMENT_TYPE_BREAKDOWN = {
    "business_id":  42,
    "period_start": "2025-10-01",
    "period_end":   "2026-03-31",
    "generated_at": "2026-04-21T14:32:00Z",
    "data":         _build_payment_type_breakdown(),
}


# ─────────────────────────────────────────────────────────────────────────────
# EP5: /api/v1/leo/expenses/staff-attribution (PII-safe, k≥3)
# Powers: Q26. Q27 ("Tell me about Sarah") is blocked at AI router.
# ─────────────────────────────────────────────────────────────────────────────

# Maria Lopez (staff_id=12) logs most expense entries as the admin — matches
# dev-DB pattern where one person logs 90% of rows. James (15) fills in 4
# entries in Feb 2026 (the stress month).
# Tom (21, deactivated after Jun 2025) never appears in this window.
# total_amount_logged is RETURNED but NOT embedded in AI RAG chunks
# (per API spec — per-individual dollar totals are borderline surveillance).

_STAFF_ATTR = {
    "2025-10-01": [(12, "Maria Lopez", 16, 4100.00)],
    "2025-11-01": [(12, "Maria Lopez", 17, 4280.00)],
    "2025-12-01": [(12, "Maria Lopez", 22, 5420.00)],
    "2026-01-01": [(12, "Maria Lopez", 15, 3890.00)],
    "2026-02-01": [(12, "Maria Lopez", 15, 3760.00),
                   (15, "James Carter", 4,  840.00)],   # fills in during stress
    "2026-03-01": [(12, "Maria Lopez", 18, 4320.00)],
}


def _build_staff_attribution():
    rows = []
    for period, entries in _STAFF_ATTR.items():
        sorted_entries = sorted(entries, key=lambda e: -e[2])
        for rank, (emp_id, name, entries_logged, amount) in enumerate(sorted_entries, start=1):
            # k-anonymity guard
            if entries_logged < 3:
                continue
            rows.append({
                "period":              period,
                "employee_id":         emp_id,
                "employee_name":       name,
                "entries_logged":      entries_logged,
                "total_amount_logged": round(amount, 2),
                "rank_in_month":       rank,
            })

    rows.sort(key=lambda r: r["rank_in_month"])
    rows.sort(key=lambda r: r["period"], reverse=True)
    return rows


STAFF_ATTRIBUTION = {
    "business_id":  42,
    "period_start": "2025-10-01",
    "period_end":   "2026-03-31",
    "generated_at": "2026-04-21T14:32:00Z",
    "data":         _build_staff_attribution(),
}


# ─────────────────────────────────────────────────────────────────────────────
# EP6: /api/v1/leo/expenses/category-location-cross
# Powers: Q19 (category mix per location)
# ─────────────────────────────────────────────────────────────────────────────

# Feb + Mar 2026 only (the window relevant to "last month" / "this month"
# questions). Defaults to 60/40 Main St / Westside per category, with
# targeted overrides for the marketing spike + Mar products quirks.

def _cross_rows_for(period, cat_totals, main_share=0.60):
    overrides = {}
    if period == "2026-02-01":
        # Marketing spike: 70/30 (ads primarily drove Main St)
        overrides[15] = (448.00, 192.00)
        # Equipment increase: 55/45
        overrides[32] = (242.00, 198.00)
    elif period == "2026-03-01":
        # Products: Main St took the near-duplicates + miscategorized row
        #   Normal 5 txns × 60/40 = $624 Main St / $416 Westside
        #   Add 2 $50 dupes (Main St) + $60 miscategorized (Main St)
        #   Main St Products = $784, Westside = $256
        overrides[24] = (784.00, 256.00)

    out = []
    for cat_id, cat_total in cat_totals.items():
        if cat_id in overrides:
            ms, ws = overrides[cat_id]
        else:
            ms = round(cat_total * main_share, 2)
            ws = round(cat_total - ms, 2)
        out.append((cat_id, ms, ws))
    return out


def _build_category_location_cross():
    rows = []
    for period in ["2026-02-01", "2026-03-01"]:
        cats = _CAT_MONTHLY[period]
        cross = _cross_rows_for(period, cats)

        # Compute location totals (sum across cats) for pct_of_location_month
        loc_totals = {1: 0.0, 2: 0.0}
        for _, ms, ws in cross:
            loc_totals[1] += ms
            loc_totals[2] += ws

        for loc_id in (1, 2):
            cat_amounts = [(cat_id, ms if loc_id == 1 else ws) for cat_id, ms, ws in cross]
            cat_amounts.sort(key=lambda x: -x[1])
            rank_map = {c: i + 1 for i, (c, _) in enumerate(cat_amounts)}

            for cat_id, ms, ws in cross:
                amt = ms if loc_id == 1 else ws
                if amt <= 0:
                    continue
                total_cat_txns = _CAT_TXN_COUNT.get(cat_id, {}).get(period, 1)
                total_cat_amt = ms + ws
                if total_cat_amt > 0:
                    tc = max(1, round(total_cat_txns * amt / total_cat_amt))
                else:
                    tc = 1

                rows.append({
                    "period":                  period,
                    "location_id":             loc_id,
                    "location_name":           _LOC_NAMES[loc_id],
                    "category_id":             cat_id,
                    "category_name":           _CATEGORY_NAMES[cat_id],
                    "cross_total":             round(amt, 2),
                    "transaction_count":       tc,
                    "pct_of_location_month":   round(amt / loc_totals[loc_id] * 100, 2),
                    "rank_in_location_month":  rank_map[cat_id],
                })

    rows.sort(key=lambda r: (r["location_id"], r["rank_in_location_month"]))
    rows.sort(key=lambda r: r["period"], reverse=True)
    return rows


CATEGORY_LOCATION_CROSS = {
    "business_id":  42,
    "period_start": "2026-02-01",
    "period_end":   "2026-03-31",
    "generated_at": "2026-04-21T14:32:00Z",
    "data":         _build_category_location_cross(),
}


# ─────────────────────────────────────────────────────────────────────────────
# Fixture lookup — endpoint path → response
# ─────────────────────────────────────────────────────────────────────────────

FIXTURES: dict[str, dict] = {
    "/api/v1/leo/expenses/monthly-summary":         MONTHLY_SUMMARY,
    "/api/v1/leo/expenses/category-breakdown":      CATEGORY_BREAKDOWN,
    "/api/v1/leo/expenses/location-breakdown":      LOCATION_BREAKDOWN,
    "/api/v1/leo/expenses/payment-type-breakdown":  PAYMENT_TYPE_BREAKDOWN,
    "/api/v1/leo/expenses/staff-attribution":       STAFF_ATTRIBUTION,
    "/api/v1/leo/expenses/category-location-cross": CATEGORY_LOCATION_CROSS,
}


__all__ = [
    "MONTHLY_SUMMARY",
    "CATEGORY_BREAKDOWN",
    "LOCATION_BREAKDOWN",
    "PAYMENT_TYPE_BREAKDOWN",
    "STAFF_ATTRIBUTION",
    "CATEGORY_LOCATION_CROSS",
    "FIXTURES",
]


# ─────────────────────────────────────────────────────────────────────────────
# Self-test — run this file directly to verify fixture internal consistency
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 78)
    print("FIXTURE SELF-TEST")
    print("=" * 78)

    ms_by_period = {r["period"]: r["total_expenses"] for r in MONTHLY_SUMMARY["data"]}

    cat_sum = {}
    for r in CATEGORY_BREAKDOWN["data"]:
        cat_sum.setdefault(r["period"], 0)
        cat_sum[r["period"]] += r["category_total"]

    print("\n1. Monthly ↔ Category sum cross-check:")
    for p in _PERIODS:
        ok = "OK" if abs(ms_by_period[p] - round(cat_sum[p], 2)) < 0.01 else "MISMATCH"
        print(f"   {p}: ${ms_by_period[p]:>8}  =?  ${round(cat_sum[p], 2):>8}  {ok}")

    loc_sum = {}
    for r in LOCATION_BREAKDOWN["data"]:
        loc_sum.setdefault(r["period"], 0)
        loc_sum[r["period"]] += r["location_total"]

    print("\n2. Monthly ↔ Location sum cross-check:")
    for p in _PERIODS:
        ok = "OK" if abs(ms_by_period[p] - round(loc_sum[p], 2)) < 0.01 else "MISMATCH"
        print(f"   {p}: ${ms_by_period[p]:>8}  =?  ${round(loc_sum[p], 2):>8}  {ok}")

    pay_sum = {}
    for r in PAYMENT_TYPE_BREAKDOWN["data"]:
        pay_sum.setdefault(r["period"], 0)
        pay_sum[r["period"]] += r["type_total"]

    print("\n3. Monthly ↔ Payment_type sum cross-check:")
    for p in _PERIODS:
        ok = "OK" if abs(ms_by_period[p] - round(pay_sum[p], 2)) < 0.50 else "MISMATCH"
        print(f"   {p}: ${ms_by_period[p]:>8}  =?  ${round(pay_sum[p], 2):>8}  {ok}")

    print("\n4. Key story verifications:")

    feb_marketing = next(r for r in CATEGORY_BREAKDOWN["data"]
                         if r["period"] == "2026-02-01" and r["category_id"] == 15)
    print(f"   Q22/Q24 Marketing Feb spike: flag={feb_marketing['anomaly_flag']}, "
          f"pct_vs_baseline={feb_marketing['pct_vs_baseline']}%")

    dec_equip = next(r for r in CATEGORY_BREAKDOWN["data"]
                     if r["period"] == "2025-12-01" and r["category_id"] == 32)
    print(f"   Q20 Equipment Dec spike: flag={dec_equip['anomaly_flag']}, "
          f"pct_vs_baseline={dec_equip['pct_vs_baseline']}%")

    admin_after_dec = [r for r in CATEGORY_BREAKDOWN["data"]
                       if r["category_id"] == 18 and r["period"] >= "2026-01-01"]
    print(f"   Q28 Office/Admin rows in 2026: {len(admin_after_dec)} "
          f"(expected 0 → dormant detection fires)")

    mar = next(r for r in MONTHLY_SUMMARY["data"] if r["period"] == "2026-03-01")
    print(f"   Q8/Q21 QoQ: Q1 2026=${mar['current_quarter_total']}, "
          f"Q4 2025=${mar['prev_quarter_total']}, change={mar['qoq_change_pct']}%")
    print(f"   Q2 YTD in Mar 2026: ${mar['ytd_total']} (expected $12,810)")

    feb_staff = [r for r in STAFF_ATTRIBUTION["data"] if r["period"] == "2026-02-01"]
    print(f"   Q26 Feb 2026 staff rows: {len(feb_staff)} (expected 2 — Maria + James)")

    other_counts = {p: len([r for r in STAFF_ATTRIBUTION["data"] if r["period"] == p])
                    for p in _PERIODS if p != "2026-02-01"}
    print(f"   Q26 Other months — rows per period: {other_counts} (expected 1 each)")

    mar_rent = next(r for r in CATEGORY_BREAKDOWN["data"]
                    if r["period"] == "2026-03-01" and r["category_id"] == 14)
    has_subcats = "subcategory_breakdown" in mar_rent
    print(f"   Q13 Subcategories present on Mar Rent & Utilities: {has_subcats}")

    print("\n5. Endpoint row counts:")
    for path, fixture in FIXTURES.items():
        print(f"   {path}: {len(fixture['data'])} rows")

    print("\n" + "=" * 78)
    print("SELF-TEST COMPLETE")
    print("=" * 78)