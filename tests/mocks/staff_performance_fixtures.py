"""
tests/mocks/staff_performance_fixtures.py

Realistic mock response data for the 2 staff performance endpoints:
  - /api/v1/leo/staff-performance   (mode=monthly)  → MONTHLY_PERFORMANCE
  - /api/v1/leo/staff-performance   (mode=summary)  → SUMMARY_PERFORMANCE

Based on the same salon business (business_id=42) with 2 locations,
4 staff members (Tom Rivera left after Jun 2025), covering:
  - Jan 2025 – Mar 2026 (15 months of monthly data)
  - Jan 2026 – Mar 2026 added to support "last month" and "last 3 months" questions

WHY NO STAFF APPOINTMENTS DATA HERE:
  No-shows (Q40), cancellations per staff (Q39), and completion rates (Q38)
  are already covered by the Appointments domain via:
    /api/v1/leo/appointments/by-staff  (appointments_fixtures.py)
    appointments_fixtures_2026.py (Feb + Mar 2026)
  Those fixtures have: completed_count, cancelled_count, no_show_count,
  no_show_rate_pct, completion_rate_pct, mom_growth_pct per staff per period.
  Duplicating that data here would create two conflicting sources for the same facts.

Data is internally consistent with appointments_fixtures.py:
  completed_visit_count MUST match appointments by-staff completed_count
  for the same staff_id + period. Verified in cross-checks below.

Cross-checks — completed_visit_count vs appointments by-staff completed_count:
  ── 2025 ───────────────────────────────────────────────────────────────────
  Maria  Jan: 52 ✓  Feb: 47 ✓  Mar: 58 ✓  Apr: 56 ✓  May: 62 ✓  Jun: 68 ✓
  James  Jan: 48 ✓  Feb: 43 ✓  Mar: 52 ✓  Apr: 50 ✓  May: 54 ✓  Jun: 58 ✓
  Aisha  Jan: 46 ✓  Feb: 42 ✓  Mar: 54 ✓  Apr: 52 ✓  May: 57 ✓  Jun: 61 ✓
  Tom    Jan: 32 ✓  Feb: 31 ✓  Mar: 37 ✓  Apr: 37 ✓  May: 37 ✓  Jun: 36 ✓
  ── 2026 (from appointments_fixtures_2026.py) ──────────────────────────────
  Maria  Jan: 71 (est) Feb: 83 ✓  Mar: 87 ✓
  James  Jan: 60 (est) Feb: 74 ✓  Mar: 79 ✓
  Aisha  Jan: 63 (est) Feb: 77 ✓  Mar: 81 ✓
  Tom:   no 2026 rows — left after Jun 2025 ✓

Org-level revenue by period:
  2025-01: $11,910.40  2025-02: $10,860.20  2025-03: $13,422.70
  2025-04: $12,991.70  2025-05: $14,062.90  2025-06: $15,017.00
  2026-01: $13,900.10  2026-02: $16,780.10  2026-03: $17,716.90

Reference data:
  Locations:  1=Main St, 2=Westside
  Staff:      12=Maria Lopez, 15=James Carter, 9=Aisha Nwosu, 21=Tom Rivera (inactive)
  hire_dates: Maria=2022-03-15, James=2021-08-01, Aisha=2023-01-10, Tom=2020-06-20
  Avg ticket: Maria=$68.50, James=$74.80, Aisha=$72.20, Tom=$44.90
  Tip rate:   Maria=12%, James=10%, Aisha=11%, Tom=8%
  Commission: Maria=15%, James=13%, Aisha=14%, Tom=12%
"""


def _row(staff_id, full_name, first, last, active, hire, loc_id, loc_name,
         year, month, visits, rev, tips, total_pay, avg_rev, comm,
         cancelled, refunded, revoked, review_count, avg_rating):
    """Helper to build a monthly performance row without repetition."""
    return {
        "business_id":              42,
        "staff_id":                 staff_id,
        "staff_full_name":          full_name,
        "staff_first_name":         first,
        "staff_last_name":          last,
        "is_active":                active,
        "hire_date":                hire,
        "location_id":              loc_id,
        "location_name":            loc_name,
        "year":                     year,
        "month":                    month,
        "period_label":             f"{year}-{month:02d}",
        "completed_visit_count":    visits,
        "unique_customer_count":    round(visits * 0.84),  # ~84% unique
        "revenue":                  rev,
        "tips":                     tips,
        "total_pay":                total_pay,
        "avg_revenue_per_visit":    avg_rev,
        "commission_earned":        comm,
        "cancelled_payment_count":  cancelled,
        "refunded_payment_count":   refunded,
        "revoked_payment_count":    revoked,
        "review_count":             review_count,
        "avg_rating":               avg_rating,
    }


# ─────────────────────────────────────────────────────────────────────────────
# /api/v1/leo/staff-performance  (mode=monthly)
# Grain: one row per (staff_id × location_id × period_label)
# Powers: Q1–Q8, Q11–Q22, Q25–Q32, Q34–Q35, Q37–Q38
# ─────────────────────────────────────────────────────────────────────────────

MONTHLY_PERFORMANCE = {
    "business_id": 42,
    "mode": "monthly",
    "data": [

        # ══════════════════════════════════════════════════════════════════════
        # MARIA LOPEZ — staff_id=12, Main St (location_id=1)
        # Top performer. Facial Treatment + Manicure. Growing every month.
        # Highest avg_rating (4.8). 15% commission rate.
        # ══════════════════════════════════════════════════════════════════════

        # ── 2025 ──────────────────────────────────────────────────────────────
        _row(12,"Maria Lopez","Maria","Lopez",True,"2022-03-15",1,"Main St",
             2025,1, 52, 3562.00, 427.44, 3989.44, 68.50, 534.30, 4,1,0, 18,4.8),
        _row(12,"Maria Lopez","Maria","Lopez",True,"2022-03-15",1,"Main St",
             2025,2, 47, 3219.50, 386.34, 3605.84, 68.50, 482.92, 4,2,0, 16,4.8),
        _row(12,"Maria Lopez","Maria","Lopez",True,"2022-03-15",1,"Main St",
             2025,3, 58, 3973.00, 476.76, 4449.76, 68.50, 595.95, 5,1,0, 20,4.9),
        _row(12,"Maria Lopez","Maria","Lopez",True,"2022-03-15",1,"Main St",
             2025,4, 56, 3836.00, 460.32, 4296.32, 68.50, 575.40, 4,1,0, 19,4.8),
        _row(12,"Maria Lopez","Maria","Lopez",True,"2022-03-15",1,"Main St",
             2025,5, 62, 4247.00, 509.64, 4756.64, 68.50, 637.05, 5,1,0, 22,4.8),
        _row(12,"Maria Lopez","Maria","Lopez",True,"2022-03-15",1,"Main St",
             2025,6, 68, 4658.00, 558.96, 5216.96, 68.50, 698.70, 5,1,0, 24,4.8),

        # ── 2026 — Jan estimated, Feb/Mar match appointments_fixtures_2026 ───
        _row(12,"Maria Lopez","Maria","Lopez",True,"2022-03-15",1,"Main St",
             2026,1, 71, 4863.50, 583.62, 5447.12, 68.50, 729.52, 5,1,0, 24,4.8),
        _row(12,"Maria Lopez","Maria","Lopez",True,"2022-03-15",1,"Main St",
             2026,2, 83, 5685.50, 682.26, 6367.76, 68.50, 852.82, 6,1,0, 28,4.9),  # ✓ appt 2026-02
        _row(12,"Maria Lopez","Maria","Lopez",True,"2022-03-15",1,"Main St",
             2026,3, 87, 5959.50, 715.14, 6674.64, 68.50, 893.92, 6,1,0, 30,4.9),  # ✓ appt 2026-03

        # ══════════════════════════════════════════════════════════════════════
        # JAMES CARTER — staff_id=15, Main St (location_id=1)
        # #2 by revenue. Swedish Massage + Hair Color = highest avg ticket ($74.80).
        # 4.6 rating. Steady consistent growth.
        # ══════════════════════════════════════════════════════════════════════

        # ── 2025 ──────────────────────────────────────────────────────────────
        _row(15,"James Carter","James","Carter",True,"2021-08-01",1,"Main St",
             2025,1, 48, 3590.40, 359.04, 3949.44, 74.80, 466.75, 4,1,0, 15,4.6),
        _row(15,"James Carter","James","Carter",True,"2021-08-01",1,"Main St",
             2025,2, 43, 3216.40, 321.64, 3538.04, 74.80, 418.13, 4,2,0, 13,4.6),
        _row(15,"James Carter","James","Carter",True,"2021-08-01",1,"Main St",
             2025,3, 52, 3889.60, 388.96, 4278.56, 74.80, 505.65, 5,1,0, 16,4.7),
        _row(15,"James Carter","James","Carter",True,"2021-08-01",1,"Main St",
             2025,4, 50, 3740.00, 374.00, 4114.00, 74.80, 486.20, 4,1,0, 15,4.6),
        _row(15,"James Carter","James","Carter",True,"2021-08-01",1,"Main St",
             2025,5, 54, 4039.20, 403.92, 4443.12, 74.80, 525.10, 5,1,0, 17,4.6),
        _row(15,"James Carter","James","Carter",True,"2021-08-01",1,"Main St",
             2025,6, 58, 4338.40, 433.84, 4772.24, 74.80, 563.99, 5,1,0, 19,4.6),

        # ── 2026 ──────────────────────────────────────────────────────────────
        _row(15,"James Carter","James","Carter",True,"2021-08-01",1,"Main St",
             2026,1, 60, 4488.00, 448.80, 4936.80, 74.80, 583.44, 5,1,0, 19,4.6),
        _row(15,"James Carter","James","Carter",True,"2021-08-01",1,"Main St",
             2026,2, 74, 5535.20, 553.52, 6088.72, 74.80, 719.58, 6,1,0, 24,4.7),  # ✓ appt 2026-02
        _row(15,"James Carter","James","Carter",True,"2021-08-01",1,"Main St",
             2026,3, 79, 5909.20, 590.92, 6500.12, 74.80, 768.20, 6,1,0, 26,4.7),  # ✓ appt 2026-03

        # ══════════════════════════════════════════════════════════════════════
        # AISHA NWOSU — staff_id=9, Westside (location_id=2)
        # #3 by revenue. Evening specialist. Reliable 4.7 rating.
        # ══════════════════════════════════════════════════════════════════════

        # ── 2025 ──────────────────────────────────────────────────────────────
        _row(9,"Aisha Nwosu","Aisha","Nwosu",True,"2023-01-10",2,"Westside",
             2025,1, 46, 3321.20, 365.33, 3686.53, 72.20, 464.97, 4,1,0, 14,4.7),
        _row(9,"Aisha Nwosu","Aisha","Nwosu",True,"2023-01-10",2,"Westside",
             2025,2, 42, 3032.40, 333.56, 3365.96, 72.20, 424.54, 4,2,0, 12,4.7),
        _row(9,"Aisha Nwosu","Aisha","Nwosu",True,"2023-01-10",2,"Westside",
             2025,3, 54, 3898.80, 428.87, 4327.67, 72.20, 545.83, 5,1,0, 17,4.7),
        _row(9,"Aisha Nwosu","Aisha","Nwosu",True,"2023-01-10",2,"Westside",
             2025,4, 52, 3754.40, 412.98, 4167.38, 72.20, 525.62, 5,1,0, 15,4.7),
        _row(9,"Aisha Nwosu","Aisha","Nwosu",True,"2023-01-10",2,"Westside",
             2025,5, 57, 4115.40, 452.69, 4568.09, 72.20, 576.16, 5,1,0, 18,4.8),
        _row(9,"Aisha Nwosu","Aisha","Nwosu",True,"2023-01-10",2,"Westside",
             2025,6, 61, 4404.20, 484.46, 4888.66, 72.20, 616.59, 5,1,0, 20,4.7),

        # ── 2026 ──────────────────────────────────────────────────────────────
        _row(9,"Aisha Nwosu","Aisha","Nwosu",True,"2023-01-10",2,"Westside",
             2026,1, 63, 4548.60, 500.35, 5048.95, 72.20, 636.80, 5,1,0, 20,4.7),
        _row(9,"Aisha Nwosu","Aisha","Nwosu",True,"2023-01-10",2,"Westside",
             2026,2, 77, 5559.40, 611.53, 6170.93, 72.20, 778.32, 6,1,0, 25,4.8),  # ✓ appt 2026-02
        _row(9,"Aisha Nwosu","Aisha","Nwosu",True,"2023-01-10",2,"Westside",
             2026,3, 81, 5848.20, 643.30, 6491.50, 72.20, 818.75, 6,1,0, 27,4.8),  # ✓ appt 2026-03

        # ══════════════════════════════════════════════════════════════════════
        # TOM RIVERA — staff_id=21, Westside (location_id=2)
        # INACTIVE — left after Jun 2025. NO 2026 rows (matches appointments_fixtures_2026).
        # is_active=False tests Q5 (is Jake active?) and Q21 (deactivated mid-month).
        # Lowest avg_rating (4.2) — tests Q10 (lowest rating).
        # ══════════════════════════════════════════════════════════════════════

        _row(21,"Tom Rivera","Tom","Rivera",False,"2020-06-20",2,"Westside",
             2025,1, 32, 1436.80, 114.94, 1551.74, 44.90, 172.42, 5,1,1,  8,4.2),
        _row(21,"Tom Rivera","Tom","Rivera",False,"2020-06-20",2,"Westside",
             2025,2, 31, 1391.90, 111.35, 1503.25, 44.90, 167.03, 5,2,1,  7,4.1),
        _row(21,"Tom Rivera","Tom","Rivera",False,"2020-06-20",2,"Westside",
             2025,3, 37, 1661.30, 132.90, 1794.20, 44.90, 199.36, 6,1,0,  9,4.2),
        _row(21,"Tom Rivera","Tom","Rivera",False,"2020-06-20",2,"Westside",
             2025,4, 37, 1661.30, 132.90, 1794.20, 44.90, 199.36, 6,1,0,  8,4.2),
        _row(21,"Tom Rivera","Tom","Rivera",False,"2020-06-20",2,"Westside",
             2025,5, 37, 1661.30, 132.90, 1794.20, 44.90, 199.36, 6,1,0,  7,4.2),
        _row(21,"Tom Rivera","Tom","Rivera",False,"2020-06-20",2,"Westside",
             2025,6, 36, 1616.40, 129.31, 1745.71, 44.90, 193.97, 5,1,0,  7,4.2),
        # No 2026 rows for Tom ✓
    ],
    "meta": {
        # 2025 totals only (Jan–Jun 2025)
        "total_completed_visits_2025":  1170,  # matches appointments + revenue ✓
        "total_revenue_2025":           67272.90,
        "total_tips_2025":              7131.46,
        "total_commission_2025":        9273.93,
        "avg_rating_org":               4.58,
        "best_staff_by_revenue":        "Maria Lopez",
        "best_period_2025":             "2025-06",
        "worst_period_2025":            "2025-02",
        "active_staff_count":           3,   # Tom excluded
        "total_staff_count":            4,   # all incl. inactive
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# /api/v1/leo/staff-performance  (mode=summary)
# Grain: one row per staff_id — YTD or all-time aggregation
# Powers: Q9 (rank all staff by revenue), Q10 (lowest rating), Q29, Q31
# ─────────────────────────────────────────────────────────────────────────────

SUMMARY_PERFORMANCE = {
    "business_id": 42,
    "mode": "summary",
    "data": [
        {
            "business_id":                    42,
            "staff_id":                       12,
            "staff_full_name":                "Maria Lopez",
            "staff_first_name":               "Maria",
            "staff_last_name":                "Lopez",
            "is_active":                      True,
            "hire_date":                      "2022-03-15",
            # YTD 2025 (Jan–Jun) — 6 months only, for apples-to-apples ranking
            "total_visits_ytd":               343,   # 52+47+58+56+62+68 ✓
            "total_revenue_ytd":              23495.50,
            "total_tips_ytd":                 2819.46,
            "total_commission_ytd":           3524.32,
            "total_customers_served":         289,
            "total_cancelled_ytd":            27,
            "total_refunded_ytd":             7,
            "overall_avg_rating":             4.82,
            "total_review_count":             119,
            "lifetime_avg_revenue_per_visit": 68.50,
            "first_active_period":            "2022-04",
            "last_active_period":             "2026-03",
            "revenue_pct_of_org_latest":      33.62,  # 5959.50 / 17716.90 × 100 (Mar 2026)
        },
        {
            "business_id": 42, "staff_id": 15,
            "staff_full_name": "James Carter",
            "staff_first_name": "James", "staff_last_name": "Carter",
            "is_active": True, "hire_date": "2021-08-01",
            "total_visits_ytd":               305,   # 48+43+52+50+54+58 ✓
            "total_revenue_ytd":              22814.00,
            "total_tips_ytd":                 2281.40,
            "total_commission_ytd":           2965.82,
            "total_customers_served":         244,
            "total_cancelled_ytd":            27,
            "total_refunded_ytd":             7,
            "overall_avg_rating":             4.63,
            "total_review_count":             95,
            "lifetime_avg_revenue_per_visit": 74.80,
            "first_active_period":            "2021-09",
            "last_active_period":             "2026-03",
            "revenue_pct_of_org_latest":      33.36,  # 5909.20 / 17716.90 × 100
        },
        {
            "business_id": 42, "staff_id": 9,
            "staff_full_name": "Aisha Nwosu",
            "staff_first_name": "Aisha", "staff_last_name": "Nwosu",
            "is_active": True, "hire_date": "2023-01-10",
            "total_visits_ytd":               312,   # 46+42+54+52+57+61 ✓
            "total_revenue_ytd":              22526.40,
            "total_tips_ytd":                 2477.89,
            "total_commission_ytd":           3153.71,
            "total_customers_served":         254,
            "total_cancelled_ytd":            28,
            "total_refunded_ytd":             7,
            "overall_avg_rating":             4.73,
            "total_review_count":             96,
            "lifetime_avg_revenue_per_visit": 72.20,
            "first_active_period":            "2023-02",
            "last_active_period":             "2026-03",
            "revenue_pct_of_org_latest":      33.01,  # 5848.20 / 17716.90 × 100
        },
        {
            "business_id": 42, "staff_id": 21,
            "staff_full_name": "Tom Rivera",
            "staff_first_name": "Tom", "staff_last_name": "Rivera",
            "is_active":    False,          # ← deactivated — still in summary ✓
            "hire_date":    "2020-06-20",
            "total_visits_ytd":               210,   # 32+31+37+37+37+36 ✓
            "total_revenue_ytd":              9429.00,
            "total_tips_ytd":                 754.34,
            "total_commission_ytd":           1131.50,
            "total_customers_served":         172,
            "total_cancelled_ytd":            33,    # highest — Q39 pointer to appt domain
            "total_refunded_ytd":             8,
            "overall_avg_rating":             4.18,  # lowest — Q10 trigger ✓
            "total_review_count":             46,
            "lifetime_avg_revenue_per_visit": 44.90,
            "first_active_period":            "2020-07",
            "last_active_period":             "2025-06",  # last period before leaving
            "revenue_pct_of_org_latest":      None,       # not active in latest period
        },
    ],
    "meta": {
        "org_total_revenue_ytd":     68265.00,
        "org_total_visits_ytd":      1170,
        "org_avg_rating":            4.58,
        "top_earner_staff_id":       12,
        "top_earner_staff_name":     "Maria Lopez",
        "lowest_rating_staff_id":    21,
        "lowest_rating_staff_name":  "Tom Rivera",
        "most_visits_staff_id":      12,
        "period_from":               "2025-01",
        "period_to":                 "2025-06",  # YTD basis for summary ranking
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Fixture lookup
# ─────────────────────────────────────────────────────────────────────────────

FIXTURES: dict[str, dict] = {
    "/api/v1/leo/staff-performance":         MONTHLY_PERFORMANCE,
    "/api/v1/leo/staff-performance-summary": SUMMARY_PERFORMANCE,
}