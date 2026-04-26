"""
tests/mocks/giftcards_fixtures.py
=================================

Mock response data for the 8 Gift Cards endpoints (Domain 9, Sprint 9):
  EP1: /api/v1/leo/giftcards/monthly                 → MONTHLY_SUMMARY
  EP2: /api/v1/leo/giftcards/liability-snapshot      → LIABILITY_SNAPSHOT
  EP3: /api/v1/leo/giftcards/by-staff                → BY_STAFF
  EP4: /api/v1/leo/giftcards/by-location             → BY_LOCATION
  EP5: /api/v1/leo/giftcards/aging-snapshot          → AGING_SNAPSHOT
  EP6: /api/v1/leo/giftcards/anomalies-snapshot      → ANOMALIES_SNAPSHOT
  EP7: /api/v1/leo/giftcards/denomination-snapshot   → DENOMINATION_SNAPSHOT
  EP8: /api/v1/leo/giftcards/health-snapshot         → HEALTH_SNAPSHOT

Single source of truth = the 10 cards + the 14 redemption visits below.
All 8 endpoint responses derive from these via the _build_* helpers, so the
math is internally consistent. _validate_anchors() asserts on import.

── Consistency anchors with existing fixtures ────────────────────────────────
• business_id=42, locations 1=Main St, 2=Westside (matches revenue/appts)
• Staff: Maria Lopez (12), James Carter (15), Aisha Nwosu (9), Tom Rivera (21)
• Tom Rivera: was active in 2025, deactivated late 2025 (matches revenue)
• Mar 2026 = recovery month — gift card redemptions spike with appointments
• Feb 2026 = weak month (only one $10 GC redemption)
• Main St dominates (matches WELCOME10 distribution + general traffic)

── Reference dates ───────────────────────────────────────────────────────────
ref_date     = 2026-04-01   (resolves "last month" → 2026-03)
snapshot_date = 2026-03-31   (latest month-end snapshot anchor)

── Card roster (10 for biz 42) ───────────────────────────────────────────────
ID  Number   Activated     Face   Balance   Active  Pattern
1   GC-001   2025-01-15    $100   $0.00     Y      Drained 2025 (1 visit)
2   GC-002   2025-02-20    $50    $0.00     Y      Drained 2025 (3 visits)
3   GC-003   2025-03-10    $200   $160.00   Y      Partial — $40 redeemed Mar 2026
4   GC-004   2025-05-05    $250   $250.00   Y      DORMANT — never redeemed (330 days)
5   GC-005   2025-08-12    $100   $60.50    Y      Multi-visit ($4 Sep 25 + $35.50 Mar 26)
6   GC-006   2025-11-20    $500   $500.00   Y      DORMANT — never redeemed (131 days)
7   GC-007   2026-01-08    $75    $75.00    Y      DORMANT — never redeemed (82 days)
8   GC-008   2026-02-14    $150   $0.00     Y      ANOMALY — drained, Active=1 (drained Feb-Mar)
9   GC-009   2025-06-01    $300   $50.00    N      DEACTIVATED mid-life (3 visits then off)
10  GC-010   2026-03-22    $100   $80.00    Y      Most recent activation

── Anchor numbers (Step 6 acceptance must see these exact values) ────────────
• Total redemption Mar 2026: $235.50
• Active card count: 9
• Outstanding liability (Active=1): $1,125.50
• Drained-but-active anomaly count: 3 (cards GC-001, GC-002, GC-008)
• Never-redeemed (active): 3 cards / $825 (GC-004, GC-006, GC-007)
• Longest dormant: GC-004, 330 days
• Top staff Mar 2026: Maria Lopez, $135 across 3 visits (rank 1 of 3)
• Top location Mar 2026: Main St, $180 (76.43% of org)
• Most common denomination bucket: $51-$100 (4 cards, 40%)
• Refunded redemptions in period: 0 (always-emit zero — Q31 acceptance)
• Avg days to first redemption: 80.0 (active redeemed only)
• Redemption rate (lifetime): 70% (7 of 10 cards)
• Single-visit (1 visit) vs multi-visit (2+) cards: 3 single / 4 multi

── Q31 zero-emission ─────────────────────────────────────────────────────────
EP6 anomalies_snapshot ALWAYS returns a single object (not array).
refunded_redemption_count = 0 in fixture, must still produce a chunk in
doc generator that says "no refunds" — this is the Q31 acceptance criteria.
"""

from __future__ import annotations
from collections import defaultdict
from datetime import date

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_BUSINESS_ID   = 42
_REF_DATE      = date(2026, 4, 1)
_SNAPSHOT_DATE = date(2026, 3, 31)
_GENERATED_AT  = "2026-04-25T15:00:00Z"

# Staff lookups (match revenue_fixtures.py)
_STAFF_NAMES  = {12: "Maria Lopez", 15: "James Carter",
                 9:  "Aisha Nwosu", 21: "Tom Rivera"}
_STAFF_ACTIVE = {12: 1, 15: 1, 9: 1, 21: 0}   # Tom no longer active

# Location lookups (match revenue_fixtures.py)
_LOC_NAMES = {1: "Main St", 2: "Westside"}


# ─────────────────────────────────────────────────────────────────────────────
# Card master (single source of truth)
# ─────────────────────────────────────────────────────────────────────────────

_CARDS = [
    {"id": 1,  "number": "GC-001", "activated": date(2025, 1, 15),
     "face": 100.00, "balance":   0.00, "active": True},
    {"id": 2,  "number": "GC-002", "activated": date(2025, 2, 20),
     "face":  50.00, "balance":   0.00, "active": True},
    {"id": 3,  "number": "GC-003", "activated": date(2025, 3, 10),
     "face": 200.00, "balance": 160.00, "active": True},
    {"id": 4,  "number": "GC-004", "activated": date(2025, 5, 5),
     "face": 250.00, "balance": 250.00, "active": True},
    {"id": 5,  "number": "GC-005", "activated": date(2025, 8, 12),
     "face": 100.00, "balance":  60.50, "active": True},
    {"id": 6,  "number": "GC-006", "activated": date(2025, 11, 20),
     "face": 500.00, "balance": 500.00, "active": True},
    {"id": 7,  "number": "GC-007", "activated": date(2026, 1, 8),
     "face":  75.00, "balance":  75.00, "active": True},
    {"id": 8,  "number": "GC-008", "activated": date(2026, 2, 14),
     "face": 150.00, "balance":   0.00, "active": True},
    {"id": 9,  "number": "GC-009", "activated": date(2025, 6, 1),
     "face": 300.00, "balance":  50.00, "active": False},
    {"id": 10, "number": "GC-010", "activated": date(2026, 3, 22),
     "face": 100.00, "balance":  80.00, "active": True},
]


# ─────────────────────────────────────────────────────────────────────────────
# Visit master (successful redemption visits — PaymentStatus = 1)
# ─────────────────────────────────────────────────────────────────────────────
# Schema reminder:
#   tbl_visit.GCAmount     = amount applied from gift card to this visit
#   tbl_visit.Payment      = total ticket amount (GC + cash + card)
#   uplift_per_visit       = max(Payment - GCAmount, 0)
#   PaymentStatus = 1      = Successful (the only value emitted here)

_VISITS = [
    # ── 2025: GC-001 drained in 1 visit, GC-002 drained over 3 visits ──
    {"date": date(2025, 2, 15),  "card_id": 1, "amount": 100.00, "payment": 130.00,
     "emp_id": 12, "loc_id": 1, "cust_id": 5001},
    {"date": date(2025, 3, 22),  "card_id": 2, "amount":  20.00, "payment":  35.00,
     "emp_id": 12, "loc_id": 1, "cust_id": 5002},
    {"date": date(2025, 4, 19),  "card_id": 2, "amount":  20.00, "payment":  30.00,
     "emp_id": 15, "loc_id": 1, "cust_id": 5002},
    {"date": date(2025, 5, 15),  "card_id": 2, "amount":  10.00, "payment":  25.00,
     "emp_id": 12, "loc_id": 1, "cust_id": 5002},

    # ── 2025 mid: GC-009 redeemed multiple times by Tom (then deactivated) ──
    {"date": date(2025, 8, 15),  "card_id": 9, "amount":  80.00, "payment": 100.00,
     "emp_id": 21, "loc_id": 2, "cust_id": 5003},
    {"date": date(2025, 9, 20),  "card_id": 9, "amount":  90.00, "payment": 110.00,
     "emp_id": 21, "loc_id": 2, "cust_id": 5003},

    # GC-005 small add-on (its first redemption, 45 days after activation)
    {"date": date(2025, 9, 26),  "card_id": 5, "amount":   4.00, "payment":  20.00,
     "emp_id": 9,  "loc_id": 2, "cust_id": 5005},

    {"date": date(2025, 10, 25), "card_id": 9, "amount":  80.00, "payment":  95.00,
     "emp_id": 9,  "loc_id": 2, "cust_id": 5003},
    # GC-009 deactivated end of October 2025 (Active=0, balance $50 stays put)

    # ── 2026: GC-008 first redemption (Feb), drained over 3 Mar visits ──
    {"date": date(2026, 2, 25),  "card_id": 8, "amount":  10.00, "payment":  30.00,
     "emp_id": 12, "loc_id": 1, "cust_id": 5004},

    # ── March 2026: 6 visits totalling $235.50 (the headline month) ──
    {"date": date(2026, 3, 5),   "card_id": 3, "amount":  40.00, "payment":  60.00,
     "emp_id": 12, "loc_id": 1, "cust_id": 5005},
    {"date": date(2026, 3, 12),  "card_id": 8, "amount":  40.00, "payment":  70.00,
     "emp_id": 12, "loc_id": 1, "cust_id": 5004},
    {"date": date(2026, 3, 18),  "card_id": 8, "amount":  55.00, "payment":  90.00,
     "emp_id": 12, "loc_id": 1, "cust_id": 5004},
    {"date": date(2026, 3, 21),  "card_id": 8, "amount":  45.00, "payment":  80.00,
     "emp_id": 15, "loc_id": 1, "cust_id": 5006},
    {"date": date(2026, 3, 25),  "card_id": 10, "amount": 20.00, "payment":  70.00,
     "emp_id": 9,  "loc_id": 2, "cust_id": 5007},
    {"date": date(2026, 3, 28),  "card_id": 5,  "amount": 35.50, "payment":  80.50,
     "emp_id": 9,  "loc_id": 2, "cust_id": 5005},
]


# ─────────────────────────────────────────────────────────────────────────────
# Anchor validator — fail fast on import if numbers drift
# ─────────────────────────────────────────────────────────────────────────────

def _validate_anchors() -> None:
    """Assert all key Step-1/Step-2 anchor numbers. Raises on drift."""
    # Active outstanding liability
    active_balance_sum = sum(c["balance"] for c in _CARDS if c["active"])
    assert abs(active_balance_sum - 1125.50) < 0.01, (
        f"Outstanding liability anchor: expected $1,125.50, got ${active_balance_sum:.2f}"
    )

    # Active card count
    active_count = sum(1 for c in _CARDS if c["active"])
    assert active_count == 9, f"Active card count anchor: expected 9, got {active_count}"

    # Per-card lifetime redemption == face - balance (visit accounting)
    redemption_by_card: dict[int, float] = defaultdict(float)
    for v in _VISITS:
        redemption_by_card[v["card_id"]] += v["amount"]
    for c in _CARDS:
        expected = c["face"] - c["balance"]
        actual = redemption_by_card[c["id"]]
        assert abs(actual - expected) < 0.01, (
            f"Card {c['number']} redemption math: face ${c['face']:.2f} - "
            f"balance ${c['balance']:.2f} = ${expected:.2f}, but visits sum to ${actual:.2f}"
        )

    # March 2026 redemption total
    mar_2026_total = sum(v["amount"] for v in _VISITS
                          if v["date"].year == 2026 and v["date"].month == 3)
    assert abs(mar_2026_total - 235.50) < 0.01, (
        f"March 2026 total anchor: expected $235.50, got ${mar_2026_total:.2f}"
    )

    # Drained-but-active count
    drained_active = sum(1 for c in _CARDS if c["active"] and c["balance"] == 0)
    assert drained_active == 3, (
        f"Drained-active anomaly anchor: expected 3, got {drained_active}"
    )

    # Never-redeemed (active) — build set directly from _VISITS to avoid
    # defaultdict-access side effects from the earlier per-card loop.
    redeemed_ids = {v["card_id"] for v in _VISITS}
    never_red_active = [c for c in _CARDS
                         if c["active"] and c["id"] not in redeemed_ids]
    assert len(never_red_active) == 3, (
        f"Never-redeemed active anchor: expected 3, got {len(never_red_active)}"
    )
    never_liab = sum(c["balance"] for c in never_red_active)
    assert abs(never_liab - 825.00) < 0.01, (
        f"Never-redeemed liability anchor: expected $825.00, got ${never_liab:.2f}"
    )


_validate_anchors()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _period(d: date) -> str:
    """Return YYYY-MM-01 for the month of date d."""
    return f"{d.year:04d}-{d.month:02d}-01"


def _is_weekend(d: date) -> bool:
    """Sat (5) or Sun (6) is weekend."""
    return d.weekday() >= 5


def _days_between(later: date, earlier: date) -> int:
    return (later - earlier).days


def _mom_pct(curr: float, prev: float | None) -> float | None:
    """MoM/YoY % with NULLIF semantics: NULL when prev is None or 0."""
    if prev is None or prev == 0:
        return None
    return round((curr - prev) / prev * 100, 2)


def _denom_bucket(face: float) -> str:
    """Map face value to denomination bucket label."""
    if face <= 25:    return "$25 or less"
    if face <= 50:    return "$26-$50"
    if face <= 100:   return "$51-$100"
    if face <= 200:   return "$101-$200"
    if face <= 500:   return "$201-$500"
    return "$500+"


def _age_bucket(days: int) -> str:
    """Map days-since-activation to aging bucket label."""
    if days <= 30:    return "0-30"
    if days <= 90:    return "31-90"
    if days <= 180:   return "91-180"
    return "181+"


# ─────────────────────────────────────────────────────────────────────────────
# EP1 — Monthly redemption + activation summary
# ─────────────────────────────────────────────────────────────────────────────

def _build_monthly_summary() -> dict:
    # Aggregate redemptions by month
    red_by_period: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "total": 0.0, "cards": set(),
                  "weekend": 0, "weekday": 0, "uplift_total": 0.0}
    )
    for v in _VISITS:
        p = _period(v["date"])
        b = red_by_period[p]
        b["count"]   += 1
        b["total"]   += v["amount"]
        b["cards"].add(v["card_id"])
        if _is_weekend(v["date"]):
            b["weekend"] += 1
        else:
            b["weekday"] += 1
        b["uplift_total"] += max(v["payment"] - v["amount"], 0)

    # Aggregate activations by month
    act_by_period: dict[str, int] = defaultdict(int)
    for c in _CARDS:
        act_by_period[_period(c["activated"])] += 1

    # Union of months, sorted ascending
    all_periods = sorted(set(red_by_period.keys()) | set(act_by_period.keys()))

    # Skip months with zero redemption AND zero activation
    rows = []
    for p in all_periods:
        r = red_by_period.get(p)
        a = act_by_period.get(p, 0)
        if r is None and a == 0:
            continue
        if r is None:
            r = {"count": 0, "total": 0.0, "cards": set(),
                 "weekend": 0, "weekday": 0, "uplift_total": 0.0}
        rows.append({
            "period_start":              p,
            "redemption_count":          r["count"],
            "redemption_amount_total":   round(r["total"], 2),
            "distinct_cards_redeemed":   len(r["cards"]),
            "activation_count":          a,
            "weekend_redemption_count":  r["weekend"],
            "weekday_redemption_count":  r["weekday"],
            "avg_uplift_per_visit":      round(r["uplift_total"] / r["count"], 2)
                                          if r["count"] else 0.00,
            "uplift_total":              round(r["uplift_total"], 2),
        })

    # MoM (LAG over result set — gaps allowed) and YoY (12-month look-back)
    by_period = {row["period_start"]: row for row in rows}
    for i, row in enumerate(rows):
        prev_row = rows[i - 1] if i > 0 else None
        row["mom_redemption_pct"] = _mom_pct(
            row["redemption_amount_total"],
            prev_row["redemption_amount_total"] if prev_row else None,
        )
        row["mom_activation_pct"] = _mom_pct(
            row["activation_count"],
            prev_row["activation_count"] if prev_row else None,
        )
        # YoY: same calendar month last year
        cur_year  = int(row["period_start"][:4])
        cur_month = int(row["period_start"][5:7])
        py_period = f"{cur_year - 1:04d}-{cur_month:02d}-01"
        py_row = by_period.get(py_period)
        row["yoy_redemption_pct"] = _mom_pct(
            row["redemption_amount_total"],
            py_row["redemption_amount_total"] if py_row else None,
        )

    return {
        "business_id": _BUSINESS_ID,
        "data":        rows,
        "meta": {
            "start_date":   rows[0]["period_start"][:10] if rows else None,
            "end_date":     "2026-03-31",
            "period_count": len(rows),
            "generated_at": _GENERATED_AT,
        },
    }


MONTHLY_SUMMARY = _build_monthly_summary()


# ─────────────────────────────────────────────────────────────────────────────
# EP2 — Liability snapshot (single object)
# ─────────────────────────────────────────────────────────────────────────────

def _build_liability_snapshot() -> dict:
    active  = [c for c in _CARDS if c["active"]]
    nonzero = [c for c in active if c["balance"] > 0]
    drained = [c for c in active if c["balance"] == 0]

    total       = sum(c["balance"] for c in active)
    avg_excl    = (sum(c["balance"] for c in nonzero) / len(nonzero)) if nonzero else 0
    avg_incl    = (total / len(active)) if active else 0

    # Median per Step 2 SQL: ORDER BY balance ASC, LIMIT 1 OFFSET (count // 2)
    sorted_balances = sorted(c["balance"] for c in nonzero)
    median = sorted_balances[len(sorted_balances) // 2] if sorted_balances else 0

    return {
        "business_id": _BUSINESS_ID,
        "data": {
            "snapshot_date":                       _SNAPSHOT_DATE.isoformat(),
            "active_card_count":                    len(active),
            "outstanding_liability_total":          round(total, 2),
            "avg_remaining_balance_excl_drained":   round(avg_excl, 2),
            "avg_remaining_balance_incl_drained":   round(avg_incl, 2),
            "drained_active_count":                 len(drained),
            "median_remaining_balance":             round(median, 2),
        },
        "meta": {
            "generated_at": _GENERATED_AT,
        },
    }


LIABILITY_SNAPSHOT = _build_liability_snapshot()


# ─────────────────────────────────────────────────────────────────────────────
# EP3 — Per-staff redemption (one row per (staff, period) with at least 1 visit)
# ─────────────────────────────────────────────────────────────────────────────

def _build_by_staff() -> dict:
    grouped: dict[tuple, dict] = defaultdict(
        lambda: {"count": 0, "total": 0.0, "cards": set()}
    )
    for v in _VISITS:
        key = (v["emp_id"], _period(v["date"]))
        b = grouped[key]
        b["count"] += 1
        b["total"] += v["amount"]
        b["cards"].add(v["card_id"])

    rows = []
    for (emp_id, period), b in grouped.items():
        rows.append({
            "staff_id":                 emp_id,
            "staff_name":               _STAFF_NAMES.get(emp_id, "Unknown"),
            "is_active":                _STAFF_ACTIVE.get(emp_id, 1),
            "period_start":             period,
            "redemption_count":         b["count"],
            "redemption_amount_total":  round(b["total"], 2),
            "distinct_cards_redeemed":  len(b["cards"]),
        })

    # rank_in_period: RANK() OVER (PARTITION BY period ORDER BY total DESC)
    period_groups: dict[str, list] = defaultdict(list)
    for r in rows:
        period_groups[r["period_start"]].append(r)
    for plist in period_groups.values():
        plist.sort(key=lambda r: -r["redemption_amount_total"])
        for i, r in enumerate(plist, start=1):
            r["rank_in_period"] = i

    rows.sort(key=lambda r: (r["period_start"], r["rank_in_period"]))

    return {
        "business_id": _BUSINESS_ID,
        "data":        rows,
        "meta": {
            "start_date":   rows[0]["period_start"][:10] if rows else None,
            "end_date":     "2026-03-31",
            "generated_at": _GENERATED_AT,
        },
    }


BY_STAFF = _build_by_staff()


# ─────────────────────────────────────────────────────────────────────────────
# EP4 — Per-location redemption with MoM and within-org share
# ─────────────────────────────────────────────────────────────────────────────

def _build_by_location() -> dict:
    grouped: dict[tuple, dict] = defaultdict(
        lambda: {"count": 0, "total": 0.0, "cards": set()}
    )
    for v in _VISITS:
        key = (v["loc_id"], _period(v["date"]))
        b = grouped[key]
        b["count"] += 1
        b["total"] += v["amount"]
        b["cards"].add(v["card_id"])

    # Org-period totals for pct_of_org_redemption
    period_totals: dict[str, float] = defaultdict(float)
    for (loc_id, period), b in grouped.items():
        period_totals[period] += b["total"]

    rows = []
    for (loc_id, period), b in grouped.items():
        org_total = period_totals[period]
        rows.append({
            "location_id":              loc_id,
            "location_name":            _LOC_NAMES.get(loc_id, "Unknown"),
            "period_start":             period,
            "redemption_count":         b["count"],
            "redemption_amount_total":  round(b["total"], 2),
            "distinct_cards_redeemed":  len(b["cards"]),
            "pct_of_org_redemption":    round(b["total"] / org_total * 100, 2)
                                         if org_total else None,
        })

    # MoM per location (LAG within location's own periods, gaps allowed)
    loc_groups: dict[int, list] = defaultdict(list)
    for r in rows:
        loc_groups[r["location_id"]].append(r)
    for lst in loc_groups.values():
        lst.sort(key=lambda r: r["period_start"])
        prev = None
        for r in lst:
            r["mom_redemption_pct"] = _mom_pct(
                r["redemption_amount_total"],
                prev["redemption_amount_total"] if prev else None,
            )
            prev = r

    # Final order: chronological, then by amount within period
    rows.sort(key=lambda r: (r["period_start"], -r["redemption_amount_total"]))

    return {
        "business_id": _BUSINESS_ID,
        "data":        rows,
        "meta": {
            "start_date":   rows[0]["period_start"][:10] if rows else None,
            "end_date":     "2026-03-31",
            "generated_at": _GENERATED_AT,
        },
    }


BY_LOCATION = _build_by_location()


# ─────────────────────────────────────────────────────────────────────────────
# EP5 — Aging buckets + dormancy summary (4 bucket rows + 1 summary row)
# ─────────────────────────────────────────────────────────────────────────────

def _build_aging_snapshot() -> dict:
    # First-redemption date per card
    first_red: dict[int, date] = {}
    for v in _VISITS:
        if v["card_id"] not in first_red or v["date"] < first_red[v["card_id"]]:
            first_red[v["card_id"]] = v["date"]

    # Active cards only (Step 2: WHERE Active = 1)
    active = [c for c in _CARDS if c["active"]]

    # Per-card derived metadata
    cards_meta = []
    for c in active:
        days_old = _days_between(_SNAPSHOT_DATE, c["activated"])
        cards_meta.append({
            "id":               c["id"],
            "balance":          c["balance"],
            "days_old":         days_old,
            "bucket":           _age_bucket(days_old),
            "never_redeemed":   c["id"] not in first_red,
            "days_to_first":    _days_between(first_red[c["id"]], c["activated"])
                                  if c["id"] in first_red else None,
        })

    total_liab = sum(c["balance"] for c in active)

    # 4 aging bucket rows
    rows = []
    for bucket in ["0-30", "31-90", "91-180", "181+"]:
        in_bucket = [c for c in cards_meta if c["bucket"] == bucket]
        liab = sum(c["balance"] for c in in_bucket)
        never = sum(1 for c in in_bucket if c["never_redeemed"])
        rows.append({
            "row_type":                       "aging_bucket",
            "age_bucket":                     bucket,
            "card_count":                     len(in_bucket),
            "liability_amount":               round(liab, 2),
            "pct_of_total_liability":         round(liab / total_liab * 100, 2)
                                                if total_liab else None,
            "never_redeemed_in_bucket":       never,
            "avg_days_to_first_redemption":   None,
            "longest_dormant_card_id":        None,
            "longest_dormant_days":           None,
        })

    # Dormancy summary row
    never_cards    = [c for c in cards_meta if c["never_redeemed"]]
    redeemed_cards = [c for c in cards_meta if not c["never_redeemed"]]
    longest = max(never_cards, key=lambda c: c["days_old"]) if never_cards else None
    avg_dtf = (sum(c["days_to_first"] for c in redeemed_cards) / len(redeemed_cards)
                if redeemed_cards else None)

    rows.append({
        "row_type":                       "dormancy_summary",
        "age_bucket":                     "all",
        "card_count":                     len(never_cards),
        "liability_amount":               round(sum(c["balance"] for c in never_cards), 2),
        "pct_of_total_liability":         None,
        "never_redeemed_in_bucket":       len(never_cards),
        "avg_days_to_first_redemption":   round(avg_dtf, 1) if avg_dtf is not None else None,
        "longest_dormant_card_id":        longest["id"] if longest else None,
        "longest_dormant_days":           longest["days_old"] if longest else None,
    })

    return {
        "business_id": _BUSINESS_ID,
        "data":        rows,
        "meta": {
            "snapshot_date": _SNAPSHOT_DATE.isoformat(),
            "generated_at":  _GENERATED_AT,
        },
    }


AGING_SNAPSHOT = _build_aging_snapshot()


# ─────────────────────────────────────────────────────────────────────────────
# EP6 — Anomalies snapshot (ALWAYS-EMIT — Q31 acceptance)
# ─────────────────────────────────────────────────────────────────────────────
# Spec contract: returns one data object even when all counts are zero.
# Refunded redemptions are counted within [start_date, end_date].
# Our _VISITS list contains only PaymentStatus=1 (successful), so refund count
# is genuinely zero — we still emit the row with zeros.

_ANOM_PERIOD_START = date(2026, 1, 1)
_ANOM_PERIOD_END   = date(2026, 3, 31)


def _build_anomalies_snapshot() -> dict:
    drained_active = [c for c in _CARDS if c["active"] and c["balance"] == 0]
    deactivated    = [c for c in _CARDS if not c["active"]]
    # Derived face value for deactivated cards = balance + sum of successful redemptions.
    # Equals tbl_giftcard.face for our fixture (no refund-restored balance scenarios).
    deactivated_value = sum(c["face"] for c in deactivated)

    # Refunded redemptions in [period_start, period_end]: PaymentStatus IN (0,3,4,5)
    # Our fixture has none — emit zero per Q31 always-emit contract.
    refunded_count  = 0
    refunded_amount = 0.0

    return {
        "business_id": _BUSINESS_ID,
        "data": {
            "snapshot_date":                    _SNAPSHOT_DATE.isoformat(),
            "drained_active_count":             len(drained_active),
            "drained_active_card_ids":          [c["id"] for c in drained_active][:100],
            "deactivated_count":                len(deactivated),
            "deactivated_value_total_derived":  round(deactivated_value, 2),
            "refunded_redemption_count":        refunded_count,
            "refunded_redemption_amount":       round(refunded_amount, 2),
        },
        "meta": {
            "start_date":   _ANOM_PERIOD_START.isoformat(),
            "end_date":     _ANOM_PERIOD_END.isoformat(),
            "generated_at": _GENERATED_AT,
            "caveat": (
                "deactivated_value_total_derived = current balance + sum(successful "
                "redemptions). May undercount if any card was refunded with balance restored."
            ),
        },
    }


ANOMALIES_SNAPSHOT = _build_anomalies_snapshot()


# ─────────────────────────────────────────────────────────────────────────────
# EP7 — Denomination distribution (all 10 cards, derived face value)
# ─────────────────────────────────────────────────────────────────────────────

def _build_denomination_snapshot() -> dict:
    # All cards regardless of Active flag (questions about issuance history)
    cards = _CARDS

    by_bucket: dict[str, list] = defaultdict(list)
    for c in cards:
        by_bucket[_denom_bucket(c["face"])].append(c)

    total_cards = len(cards)
    rows = []
    for bucket in ["$25 or less", "$26-$50", "$51-$100",
                   "$101-$200", "$201-$500", "$500+"]:
        in_bucket = by_bucket.get(bucket, [])
        total_value = sum(c["face"] for c in in_bucket)
        rows.append({
            "denomination_bucket":   bucket,
            "card_count":            len(in_bucket),
            "total_value_issued":    round(total_value, 2),
            "avg_face_value":        round(total_value / len(in_bucket), 2)
                                       if in_bucket else 0.00,
            "pct_of_cards":          round(len(in_bucket) / total_cards * 100, 2)
                                       if total_cards else 0.00,
        })

    return {
        "business_id": _BUSINESS_ID,
        "data":        rows,
        "meta": {
            "snapshot_date": _SNAPSHOT_DATE.isoformat(),
            "total_cards":   total_cards,
            "generated_at":  _GENERATED_AT,
            "caveat": (
                "Face value is derived: current balance + sum(successful redemptions). "
                "May undercount on refund-restored cards."
            ),
        },
    }


DENOMINATION_SNAPSHOT = _build_denomination_snapshot()


# ─────────────────────────────────────────────────────────────────────────────
# EP8 — Card population health (single object, lifetime metrics)
# ─────────────────────────────────────────────────────────────────────────────

def _build_health_snapshot() -> dict:
    # Visit count per card (lifetime — all PaymentStatus=1 visits)
    visits_per_card: dict[int, int] = defaultdict(int)
    for v in _VISITS:
        visits_per_card[v["card_id"]] += 1

    total        = len(_CARDS)
    redeemed     = [c for c in _CARDS if visits_per_card[c["id"]] >= 1]
    single_visit = [c for c in _CARDS if visits_per_card[c["id"]] == 1]
    multi_visit  = [c for c in _CARDS if visits_per_card[c["id"]] >= 2]

    distinct_redeemers = len({v["cust_id"] for v in _VISITS})

    return {
        "business_id": _BUSINESS_ID,
        "data": {
            "snapshot_date":                          _SNAPSHOT_DATE.isoformat(),
            "total_cards_issued":                     total,
            "cards_with_redemption":                  len(redeemed),
            "redemption_rate_pct":                    round(len(redeemed) / total * 100, 2)
                                                       if total else None,
            "single_visit_drained_count":             len(single_visit),
            "multi_visit_redeemed_count":             len(multi_visit),
            "single_visit_drained_pct_of_redeemed":   round(len(single_visit) / len(redeemed) * 100, 2)
                                                       if redeemed else None,
            "multi_visit_redeemed_pct_of_redeemed":   round(len(multi_visit) / len(redeemed) * 100, 2)
                                                       if redeemed else None,
            "distinct_customer_redeemers":            distinct_redeemers,
        },
        "meta": {
            "generated_at": _GENERATED_AT,
        },
    }


HEALTH_SNAPSHOT = _build_health_snapshot()


# ─────────────────────────────────────────────────────────────────────────────
# Fixture lookup — endpoint path → response
# ─────────────────────────────────────────────────────────────────────────────

FIXTURES: dict[str, dict] = {
    "/api/v1/leo/giftcards/monthly":                MONTHLY_SUMMARY,
    "/api/v1/leo/giftcards/liability-snapshot":     LIABILITY_SNAPSHOT,
    "/api/v1/leo/giftcards/by-staff":               BY_STAFF,
    "/api/v1/leo/giftcards/by-location":            BY_LOCATION,
    "/api/v1/leo/giftcards/aging-snapshot":         AGING_SNAPSHOT,
    "/api/v1/leo/giftcards/anomalies-snapshot":     ANOMALIES_SNAPSHOT,
    "/api/v1/leo/giftcards/denomination-snapshot":  DENOMINATION_SNAPSHOT,
    "/api/v1/leo/giftcards/health-snapshot":        HEALTH_SNAPSHOT,
}


__all__ = [
    "MONTHLY_SUMMARY",
    "LIABILITY_SNAPSHOT",
    "BY_STAFF",
    "BY_LOCATION",
    "AGING_SNAPSHOT",
    "ANOMALIES_SNAPSHOT",
    "DENOMINATION_SNAPSHOT",
    "HEALTH_SNAPSHOT",
    "FIXTURES",
]