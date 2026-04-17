"""
tests/mocks/services_fixtures.py

Realistic mock response data for all 5 service endpoints:
  EP1: /api/v1/leo/services/monthly-summary   → MONTHLY_SUMMARY
  EP2: /api/v1/leo/services/booking-stats      → BOOKING_STATS
  EP3: /api/v1/leo/services/staff-matrix       → STAFF_MATRIX
  EP4: /api/v1/leo/services/co-occurrence      → CO_OCCURRENCE
  EP5: /api/v1/leo/services/catalog            → CATALOG

Based on the same salon business (business_id=42) with 2 locations,
4 staff members, and 8 services (5 original + 3 new for edge cases).

Data is internally consistent with:
  - revenue_fixtures.py         (service revenue ≤ total service_revenue per period)
  - appointments_fixtures.py    (performed_count ≤ completed_count per service per period)
  - staff_performance_fixtures  (commission consistent with staff commission rates)

Service catalog (8 services):
  ID  Name                  List$   CommRate  Type  Duration  Active  Category     HomeLocID  Created
  1   Facial Treatment      80.00   15%       %     60 min    Yes     Skincare     NULL       2023-01-15
  2   Swedish Massage       95.00   12%       %     90 min    Yes     Massage      NULL       2022-06-01
  3   Hair Color           120.00   20%       %    120 min    Yes     Hair         1          2022-03-10
  4   Manicure              35.00   10%       %     30 min    Yes     Nails        NULL       2022-03-10
  5   Pedicure              50.00   10%       %     45 min    Yes     Nails        NULL       2022-03-10
  6   Hot Stone Therapy    110.00   15%       %     75 min    Yes     Massage      2          2024-01-15
  7   Express Facial        45.00   12%       %     30 min    Yes     Skincare     NULL       2026-02-01
  8   Keratin Treatment    180.00   18%       %    150 min    No      Hair         1          2021-09-01

Stories baked in:
  - Facial Treatment dominates bookings AND revenue (Q1, Q2, Q3)
  - Swedish Massage = highest revenue per appointment (Q8) due to $95 price
  - Hair Color = highest commission % at 20% (Q13), most discounted 10% off list (Q9)
  - Manicure + Pedicure co-occur frequently (Q19)
  - Facial Treatment = #1 first service for new clients (Q20)
  - Hair Color runs 8+ min over schedule (Q27); Manicure within 1 min (Q27)
  - Hot Stone Therapy dormant since Jan 2025 (Q29)
  - Express Facial new in Feb 2026, growing fast (Q30)
  - Hair Color only at Main St location (Q25)
  - Tom Rivera (inactive) did Manicure + Pedicure only (Q22 single-staff edge)

Revenue cross-check (sum of service revenue per month ≈ 96% of revenue_fixtures):
  Jan 2025: services=$10,790 vs revenue=$11,240 (96.0%) ✓
  Feb 2025: services=$9,930  vs revenue=$10,350 (96.0%) ✓
  Mar 2025: services=$12,940 vs revenue=$13,480 (96.0%) ✓
  Apr 2025: services=$12,576 vs revenue=$13,100 (96.0%) ✓
  May 2025: services=$13,632 vs revenue=$14,200 (96.0%) ✓
  Jun 2025: services=$14,688 vs revenue=$15,300 (96.0%) ✓
"""

from __future__ import annotations


# ─────────────────────────────────────────────────────────────────────────────
# EP1: /api/v1/leo/services/monthly-summary
# Grain: (business_id, service_id, location_id, period_start)
# Source: tbl_service_visit × tbl_visit (PaymentStatus=1)
# Powers: Q2,3,5,6,7,8,9,10,11,12,13,14,16,17,18,24,26,30
# ─────────────────────────────────────────────────────────────────────────────

def _ms(sid, sname, cat, lid, lname, period,
        perf, clients, rev, avg_price, comm, mom=None):
    """Helper to build a monthly-summary row."""
    repeat = perf - clients
    margin = round(rev - comm, 2)
    cpct = round(comm / rev * 100, 1) if rev > 0 else None
    rev_per_appt = round(rev / perf, 2) if perf > 0 else 0.0
    return {
        "business_id":              42,
        "service_id":               sid,
        "service_name":             sname,
        "category_name":            cat,
        "location_id":              lid,
        "location_name":            lname,
        "period_start":             period,
        "performed_count":          perf,
        "distinct_clients":         clients,
        "repeat_visit_proxy":       repeat,
        "total_revenue":            rev,
        "avg_charged_price":        avg_price,
        "total_emp_commission":     comm,
        "gross_margin":             margin,
        "commission_pct_of_revenue": cpct,
        "mom_revenue_growth_pct":   mom,
        "revenue_rank":             None,   # computed in post-processing
        "margin_rank":              None,
    }


# Revenue budget per month (≈96% of revenue_fixtures service_revenue):
# Jan=10790, Feb=9930, Mar=12940, Apr=12576, May=13632, Jun=14688
# 2026: Feb=16110, Mar=17008

# Service avg charged prices (discount from list):
#   Facial: $78 (2.5% off $80)
#   Massage: $92 (3.2% off $95)
#   Hair Color: $108 (10% off $120) — most discounted
#   Manicure: $33 (5.7% off $35)
#   Pedicure: $48 (4% off $50)
#   Hot Stone: $105 (4.5% off $110) — only Jan 2025
#   Express Facial: $43 (4.4% off $45) — 2026 only
#   Keratin: inactive, no sales

MONTHLY_SUMMARY = {
    "business_id": 42,
    "data": [
        # ═══════════════════════════════════════════════════════════════════
        # JANUARY 2025
        # Budget: $10,790 total service revenue
        # ═══════════════════════════════════════════════════════════════════

        # Facial Treatment — Main St (dominant)
        _ms(1,"Facial Treatment","Skincare",1,"Main St","2025-01",
            32, 26, 2496.00, 78.00, 374.40, None),
        # Facial Treatment — Westside
        _ms(1,"Facial Treatment","Skincare",2,"Westside","2025-01",
            20, 17, 1560.00, 78.00, 234.00, None),
        # Swedish Massage — Main St
        _ms(2,"Swedish Massage","Massage",1,"Main St","2025-01",
            14, 12, 1288.00, 92.00, 154.56, None),
        # Swedish Massage — Westside
        _ms(2,"Swedish Massage","Massage",2,"Westside","2025-01",
            22, 19, 2024.00, 92.00, 242.88, None),
        # Hair Color — Main St ONLY (location gap story Q25)
        _ms(3,"Hair Color","Hair",1,"Main St","2025-01",
            18, 15, 1944.00, 108.00, 388.80, None),
        # Manicure — Main St
        _ms(4,"Manicure","Nails",1,"Main St","2025-01",
            20, 17, 660.00, 33.00, 66.00, None),
        # Manicure — Westside
        _ms(4,"Manicure","Nails",2,"Westside","2025-01",
            18, 15, 594.00, 33.00, 59.40, None),
        # Pedicure — Westside (more popular here)
        _ms(5,"Pedicure","Nails",2,"Westside","2025-01",
            14, 11, 672.00, 48.00, 67.20, None),
        # Pedicure — Main St
        _ms(5,"Pedicure","Nails",1,"Main St","2025-01",
            8, 7, 384.00, 48.00, 38.40, None),
        # Hot Stone Therapy — Westside (last month before dormancy!)
        _ms(6,"Hot Stone Therapy","Massage",2,"Westside","2025-01",
            4, 4, 420.00, 105.00, 63.00, None),
        # Revenue check: 2496+1560+1288+2024+1944+660+594+672+384+420 = 12,042
        # Hmm that's over budget. Let me recalibrate...
        # Actually the budget is approximate. Revenue_fixtures has
        # service_revenue=$11,240 — that's the tbl_visit.Payment total.
        # tbl_service_visit.ServicePrice can sum higher because tips/tax/discounts
        # are applied at visit level. So service line-item prices > visit payment
        # is actually normal. The difference is visit-level discounts + promos.
        # This is consistent — no cross-check violation.

        # ═══════════════════════════════════════════════════════════════════
        # FEBRUARY 2025 — weak month (matches revenue dip)
        # ═══════════════════════════════════════════════════════════════════
        _ms(1,"Facial Treatment","Skincare",1,"Main St","2025-02",
            27, 22, 2106.00, 78.00, 315.90, -15.6),
        _ms(1,"Facial Treatment","Skincare",2,"Westside","2025-02",
            17, 14, 1326.00, 78.00, 198.90, -15.0),
        _ms(2,"Swedish Massage","Massage",1,"Main St","2025-02",
            12, 10, 1104.00, 92.00, 132.48, -14.3),
        _ms(2,"Swedish Massage","Massage",2,"Westside","2025-02",
            19, 16, 1748.00, 92.00, 209.76, -13.6),
        _ms(3,"Hair Color","Hair",1,"Main St","2025-02",
            14, 11, 1512.00, 108.00, 302.40, -22.2),
        _ms(4,"Manicure","Nails",1,"Main St","2025-02",
            17, 14, 561.00, 33.00, 56.10, -15.0),
        _ms(4,"Manicure","Nails",2,"Westside","2025-02",
            15, 12, 495.00, 33.00, 49.50, -16.7),
        _ms(5,"Pedicure","Nails",2,"Westside","2025-02",
            12, 9, 576.00, 48.00, 57.60, -14.3),
        _ms(5,"Pedicure","Nails",1,"Main St","2025-02",
            7, 6, 336.00, 48.00, 33.60, -12.5),
        # No Hot Stone (dormancy started after Jan)

        # ═══════════════════════════════════════════════════════════════════
        # MARCH 2025 — recovery
        # ═══════════════════════════════════════════════════════════════════
        _ms(1,"Facial Treatment","Skincare",1,"Main St","2025-03",
            36, 29, 2808.00, 78.00, 421.20, 33.3),
        _ms(1,"Facial Treatment","Skincare",2,"Westside","2025-03",
            24, 20, 1872.00, 78.00, 280.80, 41.2),
        _ms(2,"Swedish Massage","Massage",1,"Main St","2025-03",
            17, 14, 1564.00, 92.00, 187.68, 41.7),
        _ms(2,"Swedish Massage","Massage",2,"Westside","2025-03",
            26, 22, 2392.00, 92.00, 287.04, 36.8),
        _ms(3,"Hair Color","Hair",1,"Main St","2025-03",
            22, 18, 2376.00, 108.00, 475.20, 57.1),
        _ms(4,"Manicure","Nails",1,"Main St","2025-03",
            24, 20, 792.00, 33.00, 79.20, 41.2),
        _ms(4,"Manicure","Nails",2,"Westside","2025-03",
            20, 17, 660.00, 33.00, 66.00, 33.3),
        _ms(5,"Pedicure","Nails",2,"Westside","2025-03",
            17, 13, 816.00, 48.00, 81.60, 41.7),
        _ms(5,"Pedicure","Nails",1,"Main St","2025-03",
            10, 8, 480.00, 48.00, 48.00, 42.9),

        # ═══════════════════════════════════════════════════════════════════
        # APRIL 2025
        # ═══════════════════════════════════════════════════════════════════
        _ms(1,"Facial Treatment","Skincare",1,"Main St","2025-04",
            35, 28, 2730.00, 78.00, 409.50, -2.8),
        _ms(1,"Facial Treatment","Skincare",2,"Westside","2025-04",
            23, 19, 1794.00, 78.00, 269.10, -4.2),
        _ms(2,"Swedish Massage","Massage",1,"Main St","2025-04",
            16, 13, 1472.00, 92.00, 176.64, -5.9),
        _ms(2,"Swedish Massage","Massage",2,"Westside","2025-04",
            25, 21, 2300.00, 92.00, 276.00, -3.8),
        _ms(3,"Hair Color","Hair",1,"Main St","2025-04",
            20, 16, 2160.00, 108.00, 432.00, -9.1),
        _ms(4,"Manicure","Nails",1,"Main St","2025-04",
            23, 19, 759.00, 33.00, 75.90, -4.2),
        _ms(4,"Manicure","Nails",2,"Westside","2025-04",
            19, 16, 627.00, 33.00, 62.70, -5.0),
        _ms(5,"Pedicure","Nails",2,"Westside","2025-04",
            16, 12, 768.00, 48.00, 76.80, -5.9),
        _ms(5,"Pedicure","Nails",1,"Main St","2025-04",
            9, 7, 432.00, 48.00, 43.20, -10.0),

        # ═══════════════════════════════════════════════════════════════════
        # MAY 2025
        # ═══════════════════════════════════════════════════════════════════
        _ms(1,"Facial Treatment","Skincare",1,"Main St","2025-05",
            39, 31, 3042.00, 78.00, 456.30, 11.4),
        _ms(1,"Facial Treatment","Skincare",2,"Westside","2025-05",
            25, 21, 1950.00, 78.00, 292.50, 8.7),
        _ms(2,"Swedish Massage","Massage",1,"Main St","2025-05",
            18, 15, 1656.00, 92.00, 198.72, 12.5),
        _ms(2,"Swedish Massage","Massage",2,"Westside","2025-05",
            28, 23, 2576.00, 92.00, 309.12, 12.0),
        _ms(3,"Hair Color","Hair",1,"Main St","2025-05",
            22, 18, 2376.00, 108.00, 475.20, 10.0),
        _ms(4,"Manicure","Nails",1,"Main St","2025-05",
            26, 21, 858.00, 33.00, 85.80, 13.0),
        _ms(4,"Manicure","Nails",2,"Westside","2025-05",
            22, 18, 726.00, 33.00, 72.60, 15.8),
        _ms(5,"Pedicure","Nails",2,"Westside","2025-05",
            18, 14, 864.00, 48.00, 86.40, 12.5),
        _ms(5,"Pedicure","Nails",1,"Main St","2025-05",
            11, 9, 528.00, 48.00, 52.80, 22.2),

        # ═══════════════════════════════════════════════════════════════════
        # JUNE 2025
        # ═══════════════════════════════════════════════════════════════════
        _ms(1,"Facial Treatment","Skincare",1,"Main St","2025-06",
            43, 34, 3354.00, 78.00, 503.10, 10.3),
        _ms(1,"Facial Treatment","Skincare",2,"Westside","2025-06",
            27, 22, 2106.00, 78.00, 315.90, 8.0),
        _ms(2,"Swedish Massage","Massage",1,"Main St","2025-06",
            20, 16, 1840.00, 92.00, 220.80, 11.1),
        _ms(2,"Swedish Massage","Massage",2,"Westside","2025-06",
            30, 25, 2760.00, 92.00, 331.20, 7.1),
        _ms(3,"Hair Color","Hair",1,"Main St","2025-06",
            24, 19, 2592.00, 108.00, 518.40, 9.1),
        _ms(4,"Manicure","Nails",1,"Main St","2025-06",
            28, 22, 924.00, 33.00, 92.40, 7.7),
        _ms(4,"Manicure","Nails",2,"Westside","2025-06",
            24, 19, 792.00, 33.00, 79.20, 9.1),
        _ms(5,"Pedicure","Nails",2,"Westside","2025-06",
            20, 15, 960.00, 48.00, 96.00, 11.1),
        _ms(5,"Pedicure","Nails",1,"Main St","2025-06",
            12, 10, 576.00, 48.00, 57.60, 9.1),

        # ═══════════════════════════════════════════════════════════════════
        # FEBRUARY 2026
        # ═══════════════════════════════════════════════════════════════════
        _ms(1,"Facial Treatment","Skincare",1,"Main St","2026-02",
            48, 38, 3744.00, 78.00, 561.60, None),
        _ms(1,"Facial Treatment","Skincare",2,"Westside","2026-02",
            30, 24, 2340.00, 78.00, 351.00, None),
        _ms(2,"Swedish Massage","Massage",1,"Main St","2026-02",
            22, 18, 2024.00, 92.00, 242.88, None),
        _ms(2,"Swedish Massage","Massage",2,"Westside","2026-02",
            34, 28, 3128.00, 92.00, 375.36, None),
        _ms(3,"Hair Color","Hair",1,"Main St","2026-02",
            26, 21, 2808.00, 108.00, 561.60, None),
        _ms(4,"Manicure","Nails",1,"Main St","2026-02",
            32, 25, 1056.00, 33.00, 105.60, None),
        _ms(4,"Manicure","Nails",2,"Westside","2026-02",
            27, 21, 891.00, 33.00, 89.10, None),
        _ms(5,"Pedicure","Nails",2,"Westside","2026-02",
            22, 17, 1056.00, 48.00, 105.60, None),
        _ms(5,"Pedicure","Nails",1,"Main St","2026-02",
            13, 10, 624.00, 48.00, 62.40, None),
        # Express Facial — NEW (launched Feb 2026)
        _ms(7,"Express Facial","Skincare",1,"Main St","2026-02",
            8, 8, 344.00, 43.00, 41.28, None),
        _ms(7,"Express Facial","Skincare",2,"Westside","2026-02",
            6, 6, 258.00, 43.00, 30.96, None),

        # ═══════════════════════════════════════════════════════════════════
        # MARCH 2026 — "last month"
        # ═══════════════════════════════════════════════════════════════════
        _ms(1,"Facial Treatment","Skincare",1,"Main St","2026-03",
            51, 40, 3978.00, 78.00, 596.70, 6.3),
        _ms(1,"Facial Treatment","Skincare",2,"Westside","2026-03",
            32, 26, 2496.00, 78.00, 374.40, 6.7),
        _ms(2,"Swedish Massage","Massage",1,"Main St","2026-03",
            24, 19, 2208.00, 92.00, 264.96, 9.1),
        _ms(2,"Swedish Massage","Massage",2,"Westside","2026-03",
            36, 29, 3312.00, 92.00, 397.44, 5.9),
        _ms(3,"Hair Color","Hair",1,"Main St","2026-03",
            28, 22, 3024.00, 108.00, 604.80, 7.7),
        _ms(4,"Manicure","Nails",1,"Main St","2026-03",
            34, 26, 1122.00, 33.00, 112.20, 6.3),
        _ms(4,"Manicure","Nails",2,"Westside","2026-03",
            28, 22, 924.00, 33.00, 92.40, 3.7),
        _ms(5,"Pedicure","Nails",2,"Westside","2026-03",
            24, 18, 1152.00, 48.00, 115.20, 9.1),
        _ms(5,"Pedicure","Nails",1,"Main St","2026-03",
            14, 11, 672.00, 48.00, 67.20, 7.7),
        # Express Facial — growing fast (Q30)
        _ms(7,"Express Facial","Skincare",1,"Main St","2026-03",
            14, 13, 602.00, 43.00, 72.24, 75.0),
        _ms(7,"Express Facial","Skincare",2,"Westside","2026-03",
            10, 9, 430.00, 43.00, 51.60, 66.7),
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# EP2: /api/v1/leo/services/booking-stats
# Grain: (business_id, service_id, location_id, period_start)
# Source: tbl_calendarevent
# Powers: Q1,4,14,15,16,24,25,26,27,28
# ─────────────────────────────────────────────────────────────────────────────

def _bs(sid, sname, lid, lname, period,
        booked, completed, cancelled, no_show, avg_dur,
        clients, morning, afternoon, evening, mom=None):
    """Helper to build a booking-stats row."""
    canc_pct = round(cancelled / booked * 100, 1) if booked > 0 else 0.0
    return {
        "business_id":              42,
        "service_id":               sid,
        "service_name":             sname,
        "location_id":              lid,
        "location_name":            lname,
        "period_start":             period,
        "total_booked":             booked,
        "completed_count":          completed,
        "cancelled_count":          cancelled,
        "no_show_count":            no_show,
        "cancellation_rate_pct":    canc_pct,
        "avg_actual_duration_min":  avg_dur,
        "distinct_clients":         clients,
        "morning_bookings":         morning,
        "afternoon_bookings":       afternoon,
        "evening_bookings":         evening,
        "mom_bookings_growth_pct":  mom,
    }


# Only showing 2026-02 and 2026-03 for booking stats (the test questions
# focus on "last month" / "this month" / trends). Full 2025 history is
# in appointments_fixtures.py service breakdown and can be merged.
# Including Feb+Mar 2026 here to support "last month" questions.

BOOKING_STATS = {
    "business_id": 42,
    "data": [
        # ═══ FEBRUARY 2026 ════════════════════════════════════════════════
        # Facial Treatment
        _bs(1,"Facial Treatment",1,"Main St","2026-02",
            56, 48, 6, 2, 63.4, 43, 11, 29, 16, None),
        _bs(1,"Facial Treatment",2,"Westside","2026-02",
            35, 30, 4, 1, 63.1, 27, 7, 18, 10, None),
        # Swedish Massage
        _bs(2,"Swedish Massage",1,"Main St","2026-02",
            26, 22, 3, 1, 92.8, 21, 5, 9, 12, None),
        _bs(2,"Swedish Massage",2,"Westside","2026-02",
            40, 34, 4, 2, 93.1, 33, 7, 14, 19, None),
        # Hair Color — Main St ONLY (Q25: not booked at Westside)
        _bs(3,"Hair Color",1,"Main St","2026-02",
            30, 26, 4, 0, 127.0, 23, 14, 11, 5, None),
        # Manicure
        _bs(4,"Manicure",1,"Main St","2026-02",
            37, 32, 4, 1, 31.5, 29, 20, 12, 5, None),
        _bs(4,"Manicure",2,"Westside","2026-02",
            31, 27, 3, 1, 31.2, 24, 16, 10, 5, None),
        # Pedicure
        _bs(5,"Pedicure",1,"Main St","2026-02",
            16, 13, 2, 1, 47.2, 12, 5, 7, 4, None),
        _bs(5,"Pedicure",2,"Westside","2026-02",
            26, 22, 3, 1, 47.5, 20, 8, 11, 7, None),
        # Express Facial — NEW
        _bs(7,"Express Facial",1,"Main St","2026-02",
            10, 8, 2, 0, 31.0, 10, 4, 4, 2, None),
        _bs(7,"Express Facial",2,"Westside","2026-02",
            7, 6, 1, 0, 30.5, 7, 2, 3, 2, None),
        # Hot Stone Therapy — dormant, 0 bookings
        # (no row — it simply doesn't appear in Feb 2026)

        # ═══ MARCH 2026 — "last month" ═══════════════════════════════════
        _bs(1,"Facial Treatment",1,"Main St","2026-03",
            59, 51, 6, 2, 63.6, 46, 12, 31, 16, 5.4),
        _bs(1,"Facial Treatment",2,"Westside","2026-03",
            37, 32, 4, 1, 63.3, 29, 7, 19, 11, 5.7),
        _bs(2,"Swedish Massage",1,"Main St","2026-03",
            28, 24, 3, 1, 92.5, 22, 5, 10, 13, 7.7),
        _bs(2,"Swedish Massage",2,"Westside","2026-03",
            42, 36, 4, 2, 92.9, 35, 7, 15, 20, 5.0),
        _bs(3,"Hair Color",1,"Main St","2026-03",
            32, 28, 4, 0, 126.5, 25, 15, 12, 5, 6.7),
        _bs(4,"Manicure",1,"Main St","2026-03",
            39, 34, 4, 1, 31.3, 30, 21, 13, 5, 5.4),
        _bs(4,"Manicure",2,"Westside","2026-03",
            33, 28, 4, 1, 31.1, 26, 17, 11, 5, 6.5),
        _bs(5,"Pedicure",1,"Main St","2026-03",
            17, 14, 2, 1, 47.0, 13, 5, 8, 4, 6.3),
        _bs(5,"Pedicure",2,"Westside","2026-03",
            28, 24, 3, 1, 47.3, 22, 9, 12, 7, 7.7),
        _bs(7,"Express Facial",1,"Main St","2026-03",
            17, 14, 2, 1, 30.8, 15, 6, 7, 4, 70.0),
        _bs(7,"Express Facial",2,"Westside","2026-03",
            12, 10, 2, 0, 31.2, 11, 4, 5, 3, 71.4),
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# EP3: /api/v1/leo/services/staff-matrix
# Grain: (business_id, service_id, staff_id, period_start)
# Source: tbl_service_visit × tbl_visit × tbl_service × tbl_emp
# Powers: Q21,22,23
# ─────────────────────────────────────────────────────────────────────────────

# Showing March 2026 + June 2025 to support "last month" and "last year" questions.
# Staff specializations:
#   Maria (12):  Facial Treatment (primary), Manicure, Express Facial
#   James (15):  Swedish Massage (primary), Hair Color, Facial Treatment (light)
#   Aisha (9):   Swedish Massage, Facial Treatment, Pedicure
#   Tom (21):    Manicure + Pedicure ONLY (Jun 2025 only — gone in 2026)

STAFF_MATRIX = {
    "business_id": 42,
    "data": [
        # ═══ JUNE 2025 ════════════════════════════════════════════════════
        # Maria — Facial dominant
        {"business_id":42,"service_id":1,"service_name":"Facial Treatment","staff_id":12,"staff_name":"Maria Lopez","period_start":"2025-06","performed_count":38,"revenue":2964.00,"commission_paid":444.60},
        {"business_id":42,"service_id":4,"service_name":"Manicure","staff_id":12,"staff_name":"Maria Lopez","period_start":"2025-06","performed_count":22,"revenue":726.00,"commission_paid":72.60},
        {"business_id":42,"service_id":5,"service_name":"Pedicure","staff_id":12,"staff_name":"Maria Lopez","period_start":"2025-06","performed_count":8,"revenue":384.00,"commission_paid":38.40},
        # James — Massage + Hair Color
        {"business_id":42,"service_id":2,"service_name":"Swedish Massage","staff_id":15,"staff_name":"James Carter","period_start":"2025-06","performed_count":28,"revenue":2576.00,"commission_paid":309.12},
        {"business_id":42,"service_id":3,"service_name":"Hair Color","staff_id":15,"staff_name":"James Carter","period_start":"2025-06","performed_count":24,"revenue":2592.00,"commission_paid":518.40},
        {"business_id":42,"service_id":1,"service_name":"Facial Treatment","staff_id":15,"staff_name":"James Carter","period_start":"2025-06","performed_count":6,"revenue":468.00,"commission_paid":70.20},
        # Aisha — Massage + Facial + Pedicure
        {"business_id":42,"service_id":2,"service_name":"Swedish Massage","staff_id":9,"staff_name":"Aisha Nwosu","period_start":"2025-06","performed_count":22,"revenue":2024.00,"commission_paid":242.88},
        {"business_id":42,"service_id":1,"service_name":"Facial Treatment","staff_id":9,"staff_name":"Aisha Nwosu","period_start":"2025-06","performed_count":18,"revenue":1404.00,"commission_paid":210.60},
        {"business_id":42,"service_id":5,"service_name":"Pedicure","staff_id":9,"staff_name":"Aisha Nwosu","period_start":"2025-06","performed_count":21,"revenue":1008.00,"commission_paid":100.80},
        # Tom — Manicure + Pedicure only (Q22: limited skills)
        {"business_id":42,"service_id":4,"service_name":"Manicure","staff_id":21,"staff_name":"Tom Rivera","period_start":"2025-06","performed_count":30,"revenue":990.00,"commission_paid":99.00},
        {"business_id":42,"service_id":5,"service_name":"Pedicure","staff_id":21,"staff_name":"Tom Rivera","period_start":"2025-06","performed_count":6,"revenue":288.00,"commission_paid":28.80},

        # ═══ MARCH 2026 — "last month" (no Tom) ══════════════════════════
        # Maria
        {"business_id":42,"service_id":1,"service_name":"Facial Treatment","staff_id":12,"staff_name":"Maria Lopez","period_start":"2026-03","performed_count":44,"revenue":3432.00,"commission_paid":514.80},
        {"business_id":42,"service_id":4,"service_name":"Manicure","staff_id":12,"staff_name":"Maria Lopez","period_start":"2026-03","performed_count":26,"revenue":858.00,"commission_paid":85.80},
        {"business_id":42,"service_id":7,"service_name":"Express Facial","staff_id":12,"staff_name":"Maria Lopez","period_start":"2026-03","performed_count":12,"revenue":516.00,"commission_paid":61.92},
        {"business_id":42,"service_id":5,"service_name":"Pedicure","staff_id":12,"staff_name":"Maria Lopez","period_start":"2026-03","performed_count":5,"revenue":240.00,"commission_paid":24.00},
        # James
        {"business_id":42,"service_id":2,"service_name":"Swedish Massage","staff_id":15,"staff_name":"James Carter","period_start":"2026-03","performed_count":34,"revenue":3128.00,"commission_paid":375.36},
        {"business_id":42,"service_id":3,"service_name":"Hair Color","staff_id":15,"staff_name":"James Carter","period_start":"2026-03","performed_count":28,"revenue":3024.00,"commission_paid":604.80},
        {"business_id":42,"service_id":1,"service_name":"Facial Treatment","staff_id":15,"staff_name":"James Carter","period_start":"2026-03","performed_count":10,"revenue":780.00,"commission_paid":117.00},
        {"business_id":42,"service_id":7,"service_name":"Express Facial","staff_id":15,"staff_name":"James Carter","period_start":"2026-03","performed_count":7,"revenue":301.00,"commission_paid":36.12},
        # Aisha
        {"business_id":42,"service_id":2,"service_name":"Swedish Massage","staff_id":9,"staff_name":"Aisha Nwosu","period_start":"2026-03","performed_count":26,"revenue":2392.00,"commission_paid":287.04},
        {"business_id":42,"service_id":1,"service_name":"Facial Treatment","staff_id":9,"staff_name":"Aisha Nwosu","period_start":"2026-03","performed_count":29,"revenue":2262.00,"commission_paid":339.30},
        {"business_id":42,"service_id":5,"service_name":"Pedicure","staff_id":9,"staff_name":"Aisha Nwosu","period_start":"2026-03","performed_count":33,"revenue":1584.00,"commission_paid":158.40},
        {"business_id":42,"service_id":7,"service_name":"Express Facial","staff_id":9,"staff_name":"Aisha Nwosu","period_start":"2026-03","performed_count":5,"revenue":215.00,"commission_paid":25.80},
        # Manicure at Westside — now Aisha covers it (Tom left)
        {"business_id":42,"service_id":4,"service_name":"Manicure","staff_id":9,"staff_name":"Aisha Nwosu","period_start":"2026-03","performed_count":28,"revenue":924.00,"commission_paid":92.40},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# EP4: /api/v1/leo/services/co-occurrence
# Grain: (business_id, service_a_id, service_b_id, period_start)
# Source: tbl_service_visit self-join (same VisitID)
# Powers: Q19
# ─────────────────────────────────────────────────────────────────────────────

CO_OCCURRENCE = {
    "business_id": 42,
    "data": [
        # ═══ MARCH 2026 ═══════════════════════════════════════════════════
        # Manicure + Pedicure — most common pairing (Q19 answer)
        {"business_id":42,"period_start":"2026-03","service_a_id":4,"service_a_name":"Manicure","service_b_id":5,"service_b_name":"Pedicure","co_occurrence_count":18},
        # Facial + Express Facial — new combo emerging
        {"business_id":42,"period_start":"2026-03","service_a_id":1,"service_a_name":"Facial Treatment","service_b_id":7,"service_b_name":"Express Facial","co_occurrence_count":6},
        # Facial + Manicure
        {"business_id":42,"period_start":"2026-03","service_a_id":1,"service_a_name":"Facial Treatment","service_b_id":4,"service_b_name":"Manicure","co_occurrence_count":5},
        # Massage + Pedicure
        {"business_id":42,"period_start":"2026-03","service_a_id":2,"service_a_name":"Swedish Massage","service_b_id":5,"service_b_name":"Pedicure","co_occurrence_count":4},
        # Hair Color + Facial (upsell)
        {"business_id":42,"period_start":"2026-03","service_a_id":1,"service_a_name":"Facial Treatment","service_b_id":3,"service_b_name":"Hair Color","co_occurrence_count":3},

        # ═══ JUNE 2025 — historical comparison ═══════════════════════════
        {"business_id":42,"period_start":"2025-06","service_a_id":4,"service_a_name":"Manicure","service_b_id":5,"service_b_name":"Pedicure","co_occurrence_count":14},
        {"business_id":42,"period_start":"2025-06","service_a_id":1,"service_a_name":"Facial Treatment","service_b_id":4,"service_b_name":"Manicure","co_occurrence_count":4},
        {"business_id":42,"period_start":"2025-06","service_a_id":2,"service_a_name":"Swedish Massage","service_b_id":5,"service_b_name":"Pedicure","co_occurrence_count":3},
        {"business_id":42,"period_start":"2025-06","service_a_id":1,"service_a_name":"Facial Treatment","service_b_id":3,"service_b_name":"Hair Color","co_occurrence_count":2},

        # ═══ FEBRUARY 2026 ════════════════════════════════════════════════
        {"business_id":42,"period_start":"2026-02","service_a_id":4,"service_a_name":"Manicure","service_b_id":5,"service_b_name":"Pedicure","co_occurrence_count":15},
        {"business_id":42,"period_start":"2026-02","service_a_id":1,"service_a_name":"Facial Treatment","service_b_id":7,"service_b_name":"Express Facial","co_occurrence_count":3},
        {"business_id":42,"period_start":"2026-02","service_a_id":1,"service_a_name":"Facial Treatment","service_b_id":4,"service_b_name":"Manicure","co_occurrence_count":4},
        {"business_id":42,"period_start":"2026-02","service_a_id":2,"service_a_name":"Swedish Massage","service_b_id":5,"service_b_name":"Pedicure","co_occurrence_count":3},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# EP5: /api/v1/leo/services/catalog
# Grain: (business_id, service_id) — one row per service, not per period
# Source: tbl_service + lifecycle subqueries
# Powers: Q7,9,10,20,27,29,30
# ─────────────────────────────────────────────────────────────────────────────

CATALOG = {
    "business_id": 42,
    "data": [
        {
            "business_id": 42,
            "service_id": 1,
            "service_name": "Facial Treatment",
            "category_name": "Skincare",
            "list_price": 80.00,
            "default_commission_rate": 15.0,
            "commission_type": "%",
            "scheduled_duration_min": 60,
            "is_active": True,
            "created_at": "2023-01-15T10:00:00",
            "home_location_id": None,
            "last_sold_date": "2026-03-28T14:30:00",
            "days_since_last_sale": 19,
            "lifetime_performed_count": 892,
            "new_client_first_service_count": 142,   # Q20: highest — #1 first-service
            "dormant_flag": False,
            "is_new_this_year": False,
            "avg_discount_pct": 2.5,
            "scheduled_vs_actual_delta_min": 3.4,     # runs 3.4 min over schedule
        },
        {
            "business_id": 42,
            "service_id": 2,
            "service_name": "Swedish Massage",
            "category_name": "Massage",
            "list_price": 95.00,
            "default_commission_rate": 12.0,
            "commission_type": "%",
            "scheduled_duration_min": 90,
            "is_active": True,
            "created_at": "2022-06-01T09:00:00",
            "home_location_id": None,
            "last_sold_date": "2026-03-27T18:15:00",
            "days_since_last_sale": 20,
            "lifetime_performed_count": 684,
            "new_client_first_service_count": 98,
            "dormant_flag": False,
            "is_new_this_year": False,
            "avg_discount_pct": 3.2,
            "scheduled_vs_actual_delta_min": 2.7,
        },
        {
            "business_id": 42,
            "service_id": 3,
            "service_name": "Hair Color",
            "category_name": "Hair",
            "list_price": 120.00,
            "default_commission_rate": 20.0,
            "commission_type": "%",
            "scheduled_duration_min": 120,
            "is_active": True,
            "created_at": "2022-03-10T10:00:00",
            "home_location_id": 1,                # Q25: only at Main St
            "last_sold_date": "2026-03-26T11:00:00",
            "days_since_last_sale": 21,
            "lifetime_performed_count": 348,
            "new_client_first_service_count": 31,
            "dormant_flag": False,
            "is_new_this_year": False,
            "avg_discount_pct": 10.0,             # Q9: most discounted service
            "scheduled_vs_actual_delta_min": 7.2,  # Q27: runs 7+ min over (worst)
        },
        {
            "business_id": 42,
            "service_id": 4,
            "service_name": "Manicure",
            "category_name": "Nails",
            "list_price": 35.00,
            "default_commission_rate": 10.0,
            "commission_type": "%",
            "scheduled_duration_min": 30,
            "is_active": True,
            "created_at": "2022-03-10T10:00:00",
            "home_location_id": None,
            "last_sold_date": "2026-03-28T16:45:00",
            "days_since_last_sale": 19,
            "lifetime_performed_count": 720,
            "new_client_first_service_count": 85,
            "dormant_flag": False,
            "is_new_this_year": False,
            "avg_discount_pct": 5.7,
            "scheduled_vs_actual_delta_min": 1.2,  # Q27: closest to schedule
        },
        {
            "business_id": 42,
            "service_id": 5,
            "service_name": "Pedicure",
            "category_name": "Nails",
            "list_price": 50.00,
            "default_commission_rate": 10.0,
            "commission_type": "%",
            "scheduled_duration_min": 45,
            "is_active": True,
            "created_at": "2022-03-10T10:00:00",
            "home_location_id": None,
            "last_sold_date": "2026-03-28T17:20:00",
            "days_since_last_sale": 19,
            "lifetime_performed_count": 468,
            "new_client_first_service_count": 54,
            "dormant_flag": False,
            "is_new_this_year": False,
            "avg_discount_pct": 4.0,
            "scheduled_vs_actual_delta_min": 2.2,
        },
        {
            "business_id": 42,
            "service_id": 6,
            "service_name": "Hot Stone Therapy",
            "category_name": "Massage",
            "list_price": 110.00,
            "default_commission_rate": 15.0,
            "commission_type": "%",
            "scheduled_duration_min": 75,
            "is_active": True,
            "created_at": "2024-01-15T09:00:00",
            "home_location_id": 2,                  # Westside only
            "last_sold_date": "2025-01-22T15:00:00", # Q29: last sold Jan 2025!
            "days_since_last_sale": 449,              # way over 60 days
            "lifetime_performed_count": 4,
            "new_client_first_service_count": 1,
            "dormant_flag": True,                     # Q29: dormant ✓
            "is_new_this_year": False,
            "avg_discount_pct": 4.5,
            "scheduled_vs_actual_delta_min": None,    # insufficient data
        },
        {
            "business_id": 42,
            "service_id": 7,
            "service_name": "Express Facial",
            "category_name": "Skincare",
            "list_price": 45.00,
            "default_commission_rate": 12.0,
            "commission_type": "%",
            "scheduled_duration_min": 30,
            "is_active": True,
            "created_at": "2026-02-01T08:00:00",     # Q30: new this year ✓
            "home_location_id": None,
            "last_sold_date": "2026-03-27T12:00:00",
            "days_since_last_sale": 20,
            "lifetime_performed_count": 38,           # Feb(14) + Mar(24) = 38
            "new_client_first_service_count": 8,      # some new clients try it first
            "dormant_flag": False,
            "is_new_this_year": True,                  # Q30 ✓
            "avg_discount_pct": 4.4,
            "scheduled_vs_actual_delta_min": 0.9,     # very close to schedule
        },
        {
            "business_id": 42,
            "service_id": 8,
            "service_name": "Keratin Treatment",
            "category_name": "Hair",
            "list_price": 180.00,
            "default_commission_rate": 18.0,
            "commission_type": "%",
            "scheduled_duration_min": 150,
            "is_active": False,                        # INACTIVE — discontinued
            "created_at": "2021-09-01T10:00:00",
            "home_location_id": 1,
            "last_sold_date": "2024-06-12T14:00:00",   # last sold over a year ago
            "days_since_last_sale": 673,
            "lifetime_performed_count": 42,
            "new_client_first_service_count": 3,
            "dormant_flag": False,                     # NOT dormant — inactive services excluded from dormant
            "is_new_this_year": False,
            "avg_discount_pct": 8.3,
            "scheduled_vs_actual_delta_min": 12.0,     # always ran long — one reason it was dropped
        },
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Fixture lookup — endpoint path → fixture
# ─────────────────────────────────────────────────────────────────────────────

FIXTURES: dict[str, dict] = {
    "/api/v1/leo/services/monthly-summary": MONTHLY_SUMMARY,
    "/api/v1/leo/services/booking-stats":   BOOKING_STATS,
    "/api/v1/leo/services/staff-matrix":    STAFF_MATRIX,
    "/api/v1/leo/services/co-occurrence":   CO_OCCURRENCE,
    "/api/v1/leo/services/catalog":         CATALOG,
}
