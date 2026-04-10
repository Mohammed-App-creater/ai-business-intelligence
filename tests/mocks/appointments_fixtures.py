"""
tests/mocks/appointments_fixtures.py

Realistic mock response data for all 4 appointment endpoints.
Based on the same salon business (business_id=42) with 2 locations,
4 staff members, and 6 months of history (Jan–Jun 2025).

Data is internally consistent with revenue_fixtures.py:
  - visit_count in revenue monthly-summary aligns with completed_count here
  - Feb dip in revenue = higher cancellation rate in Feb appointments
  - Maria Lopez (emp_id=12) is top revenue staff → top completed appointments
  - Facial Treatment is the top service by both bookings and revenue contribution
  - Tom Rivera (emp_id=21, now inactive) has declining appointment trend

Cross-checks (all numbers verify against revenue_fixtures):
  Revenue Jan visit_count=178 → appt Jan completed_count(loc1+loc2)=178 ✓
  Revenue Feb visit_count=163 → appt Feb completed_count(loc1+loc2)=163 ✓
  Revenue Mar visit_count=201 → appt Mar completed_count(loc1+loc2)=201 ✓
  Revenue Apr visit_count=195 → appt Apr completed_count(loc1+loc2)=195 ✓
  Revenue May visit_count=210 → appt May completed_count(loc1+loc2)=210 ✓
  Revenue Jun visit_count=223 → appt Jun completed_count(loc1+loc2)=223 ✓
"""

# ─────────────────────────────────────────────────────────────────────────────
# Reference data (shared across fixtures)
# ─────────────────────────────────────────────────────────────────────────────
#
# Locations:   1=Main St, 2=Westside
# Staff:       12=Maria Lopez, 15=James Carter, 9=Aisha Nwosu, 21=Tom Rivera
# Services:    1=Facial Treatment, 2=Swedish Massage, 3=Hair Color,
#              4=Manicure, 5=Pedicure
#
# Story baked into the data:
#   - Feb is weak: cancellation rate spikes to 18% (from ~10% in Jan)
#     → aligns with revenue dip (-7.9% MoM in revenue fixtures)
#   - Tom Rivera (emp 21) is declining: bookings fall each month
#     → he left the business after June
#   - Facial Treatment dominates: always #1 service by bookings
#   - Evening slots are busiest at Westside; mornings rule at Main St
#   - Weekend bookings spike in Mar–Jun (summer season starts)
# ─────────────────────────────────────────────────────────────────────────────


# ── /api/v1/leo/appointments/monthly-summary ─────────────────────────────────
# Per-location rows + org rollup (location_id=0, location_name="__ALL__")
# total_booked = completed + cancelled + no_show + upcoming(~0 for past months)
# completed_count must match revenue visit_count for same period
#
# Verification table:
#   Jan: L1=98 completed + L2=80 completed = 178 ✓
#   Feb: L1=91 + L2=72 = 163 ✓
#   Mar: L1=112 + L2=89 = 201 ✓
#   Apr: L1=108 + L2=87 = 195 ✓
#   May: L1=116 + L2=94 = 210 ✓
#   Jun: L1=123 + L2=100 = 223 ✓

MONTHLY_SUMMARY = {
    "business_id": 42,
    "data": [

        # ── January 2025 ──────────────────────────────────────────────────────
        {
            "period":                   "2025-01",
            "location_id":              1,
            "location_name":            "Main St",
            "location_city":            "Miami",
            "total_booked":             110,
            "confirmed_count":          105,
            "completed_count":          98,
            "cancelled_count":          10,
            "no_show_count":            2,
            "morning_count":            48,   # Main St skews morning
            "afternoon_count":          40,
            "evening_count":            22,
            "weekend_count":            31,
            "weekday_count":            79,
            "avg_actual_duration_min":  54.2,
            "cancellation_rate_pct":    9.09,
            "no_show_rate_pct":         1.82,
            "mom_growth_pct":           None,
            "walkin_count":             18,
            "app_booking_count":        92,
        },
        {
            "period":                   "2025-01",
            "location_id":              2,
            "location_name":            "Westside",
            "location_city":            "Miami",
            "total_booked":             91,
            "confirmed_count":          87,
            "completed_count":          80,
            "cancelled_count":          9,
            "no_show_count":            2,
            "morning_count":            24,   # Westside skews evening
            "afternoon_count":          33,
            "evening_count":            34,
            "weekend_count":            28,
            "weekday_count":            63,
            "avg_actual_duration_min":  51.8,
            "cancellation_rate_pct":    9.89,
            "no_show_rate_pct":         2.20,
            "mom_growth_pct":           None,
            "walkin_count":             22,
            "app_booking_count":        69,
        },
        {
            "period":                   "2025-01",
            "location_id":              0,
            "location_name":            "__ALL__",
            "location_city":            "",
            "total_booked":             201,
            "confirmed_count":          192,
            "completed_count":          178,
            "cancelled_count":          19,
            "no_show_count":            4,
            "morning_count":            72,
            "afternoon_count":          73,
            "evening_count":            56,
            "weekend_count":            59,
            "weekday_count":            142,
            "avg_actual_duration_min":  53.2,
            "cancellation_rate_pct":    9.45,
            "no_show_rate_pct":         1.99,
            "mom_growth_pct":           None,
            "walkin_count":             40,
            "app_booking_count":        161,
        },

        # ── February 2025 — cancellation spike (revenue dip month) ───────────
        {
            "period":                   "2025-02",
            "location_id":              1,
            "location_name":            "Main St",
            "location_city":            "Miami",
            "total_booked":             112,
            "confirmed_count":          106,
            "completed_count":          91,
            "cancelled_count":          18,   # spike: 16.1%
            "no_show_count":            3,
            "morning_count":            44,
            "afternoon_count":          38,
            "evening_count":            30,
            "weekend_count":            28,
            "weekday_count":            84,
            "avg_actual_duration_min":  53.8,
            "cancellation_rate_pct":    16.07,
            "no_show_rate_pct":         2.68,
            "mom_growth_pct":           1.82,  # total_booked slightly up but completions down
            "walkin_count":             14,
            "app_booking_count":        98,
        },
        {
            "period":                   "2025-02",
            "location_id":              2,
            "location_name":            "Westside",
            "location_city":            "Miami",
            "total_booked":             92,
            "confirmed_count":          86,
            "completed_count":          72,
            "cancelled_count":          17,   # spike: 18.5%
            "no_show_count":            3,
            "morning_count":            22,
            "afternoon_count":          31,
            "evening_count":            39,
            "weekend_count":            26,
            "weekday_count":            66,
            "avg_actual_duration_min":  50.9,
            "cancellation_rate_pct":    18.48,
            "no_show_rate_pct":         3.26,
            "mom_growth_pct":           1.10,
            "walkin_count":             19,
            "app_booking_count":        73,
        },
        {
            "period":                   "2025-02",
            "location_id":              0,
            "location_name":            "__ALL__",
            "location_city":            "",
            "total_booked":             204,
            "confirmed_count":          192,
            "completed_count":          163,
            "cancelled_count":          35,
            "no_show_count":            6,
            "morning_count":            66,
            "afternoon_count":          69,
            "evening_count":            69,
            "weekend_count":            54,
            "weekday_count":            150,
            "avg_actual_duration_min":  52.5,
            "cancellation_rate_pct":    17.16,
            "no_show_rate_pct":         2.94,
            "mom_growth_pct":           1.49,
            "walkin_count":             33,
            "app_booking_count":        171,
        },

        # ── March 2025 — recovery ─────────────────────────────────────────────
        {
            "period":                   "2025-03",
            "location_id":              1,
            "location_name":            "Main St",
            "location_city":            "Miami",
            "total_booked":             126,
            "confirmed_count":          122,
            "completed_count":          112,
            "cancelled_count":          11,
            "no_show_count":            3,
            "morning_count":            52,
            "afternoon_count":          46,
            "evening_count":            28,
            "weekend_count":            38,
            "weekday_count":            88,
            "avg_actual_duration_min":  55.1,
            "cancellation_rate_pct":    8.73,
            "no_show_rate_pct":         2.38,
            "mom_growth_pct":           12.50,
            "walkin_count":             20,
            "app_booking_count":        106,
        },
        {
            "period":                   "2025-03",
            "location_id":              2,
            "location_name":            "Westside",
            "location_city":            "Miami",
            "total_booked":             102,
            "confirmed_count":          98,
            "completed_count":          89,
            "cancelled_count":          10,
            "no_show_count":            3,
            "morning_count":            26,
            "afternoon_count":          36,
            "evening_count":            40,
            "weekend_count":            34,
            "weekday_count":            68,
            "avg_actual_duration_min":  52.4,
            "cancellation_rate_pct":    9.80,
            "no_show_rate_pct":         2.94,
            "mom_growth_pct":           10.87,
            "walkin_count":             24,
            "app_booking_count":        78,
        },
        {
            "period":                   "2025-03",
            "location_id":              0,
            "location_name":            "__ALL__",
            "location_city":            "",
            "total_booked":             228,
            "confirmed_count":          220,
            "completed_count":          201,
            "cancelled_count":          21,
            "no_show_count":            6,
            "morning_count":            78,
            "afternoon_count":          82,
            "evening_count":            68,
            "weekend_count":            72,
            "weekday_count":            156,
            "avg_actual_duration_min":  53.9,
            "cancellation_rate_pct":    9.21,
            "no_show_rate_pct":         2.63,
            "mom_growth_pct":           11.76,
            "walkin_count":             44,
            "app_booking_count":        184,
        },

        # ── April 2025 ────────────────────────────────────────────────────────
        {
            "period":                   "2025-04",
            "location_id":              1,
            "location_name":            "Main St",
            "location_city":            "Miami",
            "total_booked":             121,
            "confirmed_count":          117,
            "completed_count":          108,
            "cancelled_count":          11,
            "no_show_count":            2,
            "morning_count":            50,
            "afternoon_count":          44,
            "evening_count":            27,
            "weekend_count":            36,
            "weekday_count":            85,
            "avg_actual_duration_min":  54.8,
            "cancellation_rate_pct":    9.09,
            "no_show_rate_pct":         1.65,
            "mom_growth_pct":           -3.97,
            "walkin_count":             19,
            "app_booking_count":        102,
        },
        {
            "period":                   "2025-04",
            "location_id":              2,
            "location_name":            "Westside",
            "location_city":            "Miami",
            "total_booked":             99,
            "confirmed_count":          95,
            "completed_count":          87,
            "cancelled_count":          10,
            "no_show_count":            2,
            "morning_count":            25,
            "afternoon_count":          35,
            "evening_count":            39,
            "weekend_count":            32,
            "weekday_count":            67,
            "avg_actual_duration_min":  51.6,
            "cancellation_rate_pct":    10.10,
            "no_show_rate_pct":         2.02,
            "mom_growth_pct":           -2.94,
            "walkin_count":             22,
            "app_booking_count":        77,
        },
        {
            "period":                   "2025-04",
            "location_id":              0,
            "location_name":            "__ALL__",
            "location_city":            "",
            "total_booked":             220,
            "confirmed_count":          212,
            "completed_count":          195,
            "cancelled_count":          21,
            "no_show_count":            4,
            "morning_count":            75,
            "afternoon_count":          79,
            "evening_count":            66,
            "weekend_count":            68,
            "weekday_count":            152,
            "avg_actual_duration_min":  53.3,
            "cancellation_rate_pct":    9.55,
            "no_show_rate_pct":         1.82,
            "mom_growth_pct":           -3.51,
            "walkin_count":             41,
            "app_booking_count":        179,
        },

        # ── May 2025 ──────────────────────────────────────────────────────────
        {
            "period":                   "2025-05",
            "location_id":              1,
            "location_name":            "Main St",
            "location_city":            "Miami",
            "total_booked":             131,
            "confirmed_count":          127,
            "completed_count":          116,
            "cancelled_count":          12,
            "no_show_count":            3,
            "morning_count":            54,
            "afternoon_count":          48,
            "evening_count":            29,
            "weekend_count":            40,
            "weekday_count":            91,
            "avg_actual_duration_min":  55.4,
            "cancellation_rate_pct":    9.16,
            "no_show_rate_pct":         2.29,
            "mom_growth_pct":           8.26,
            "walkin_count":             21,
            "app_booking_count":        110,
        },
        {
            "period":                   "2025-05",
            "location_id":              2,
            "location_name":            "Westside",
            "location_city":            "Miami",
            "total_booked":             107,
            "confirmed_count":          103,
            "completed_count":          94,
            "cancelled_count":          11,
            "no_show_count":            2,
            "morning_count":            27,
            "afternoon_count":          38,
            "evening_count":            42,
            "weekend_count":            36,
            "weekday_count":            71,
            "avg_actual_duration_min":  52.1,
            "cancellation_rate_pct":    10.28,
            "no_show_rate_pct":         1.87,
            "mom_growth_pct":           8.08,
            "walkin_count":             24,
            "app_booking_count":        83,
        },
        {
            "period":                   "2025-05",
            "location_id":              0,
            "location_name":            "__ALL__",
            "location_city":            "",
            "total_booked":             238,
            "confirmed_count":          230,
            "completed_count":          210,
            "cancelled_count":          23,
            "no_show_count":            5,
            "morning_count":            81,
            "afternoon_count":          86,
            "evening_count":            71,
            "weekend_count":            76,
            "weekday_count":            162,
            "avg_actual_duration_min":  53.9,
            "cancellation_rate_pct":    9.66,
            "no_show_rate_pct":         2.10,
            "mom_growth_pct":           8.18,
            "walkin_count":             45,
            "app_booking_count":        193,
        },

        # ── June 2025 ─────────────────────────────────────────────────────────
        {
            "period":                   "2025-06",
            "location_id":              1,
            "location_name":            "Main St",
            "location_city":            "Miami",
            "total_booked":             139,
            "confirmed_count":          135,
            "completed_count":          123,
            "cancelled_count":          13,
            "no_show_count":            3,
            "morning_count":            58,
            "afternoon_count":          51,
            "evening_count":            30,
            "weekend_count":            44,
            "weekday_count":            95,
            "avg_actual_duration_min":  55.9,
            "cancellation_rate_pct":    9.35,
            "no_show_rate_pct":         2.16,
            "mom_growth_pct":           6.11,
            "walkin_count":             22,
            "app_booking_count":        117,
        },
        {
            "period":                   "2025-06",
            "location_id":              2,
            "location_name":            "Westside",
            "location_city":            "Miami",
            "total_booked":             115,
            "confirmed_count":          111,
            "completed_count":          100,
            "cancelled_count":          12,
            "no_show_count":            3,
            "morning_count":            29,
            "afternoon_count":          41,
            "evening_count":            45,
            "weekend_count":            39,
            "weekday_count":            76,
            "avg_actual_duration_min":  52.6,
            "cancellation_rate_pct":    10.43,
            "no_show_rate_pct":         2.61,
            "mom_growth_pct":           7.48,
            "walkin_count":             25,
            "app_booking_count":        90,
        },
        {
            "period":                   "2025-06",
            "location_id":              0,
            "location_name":            "__ALL__",
            "location_city":            "",
            "total_booked":             254,
            "confirmed_count":          246,
            "completed_count":          223,
            "cancelled_count":          25,
            "no_show_count":            6,
            "morning_count":            87,
            "afternoon_count":          92,
            "evening_count":            75,
            "weekend_count":            83,
            "weekday_count":            171,
            "avg_actual_duration_min":  54.4,
            "cancellation_rate_pct":    9.84,
            "no_show_rate_pct":         2.36,
            "mom_growth_pct":           6.72,
            "walkin_count":             47,
            "app_booking_count":        207,
        },
    ],
    "meta": {
        "total_booked":         1345,   # 6 months, org-level sum
        "total_completed":      1170,   # matches revenue total_visits=1170 ✓
        "total_cancelled":      144,
        "total_no_shows":        31,
        "avg_cancellation_rate_pct":  10.71,
        "avg_no_show_rate_pct":        2.30,
        "best_period":          "2025-06",   # most completed
        "worst_period":         "2025-02",   # highest cancellation rate
        "trend_slope":           8.4,        # positive = bookings growing
    },
}


# ── /api/v1/leo/appointments/by-staff ────────────────────────────────────────
# Staff story:
#   Maria Lopez (12):   consistent top performer, growing appointments
#   James Carter (15):  steady, slight growth
#   Aisha Nwosu (9):    solid mid-tier, most evening slots
#   Tom Rivera (21):    declining each month — left after Jun (now inactive)
#
# completed_count by staff per month must sum to revenue visit_count for that period:
#   Jan: 52+48+46+32 = 178 ✓
#   Feb: 47+43+42+31 = 163 ✓
#   Mar: 58+52+54+37 = 201 ✓
#   Apr: 56+50+52+37 = 195 ✓
#   May: 62+54+57+37 = 210 ✓
#   Jun: 68+58+61+36 = 223 ✓

STAFF_BREAKDOWN = {
    "business_id": 42,
    "data": [
        # ── Maria Lopez — top performer, growing ─────────────────────────────
        {"staff_id": 12, "staff_name": "Maria Lopez",  "location_id": 1, "location_name": "Main St",  "period": "2025-01", "total_booked": 58, "completed_count": 52, "cancelled_count": 5, "no_show_count": 1, "no_show_rate_pct": 1.72, "distinct_services_handled": 4, "mom_growth_pct": None},
        {"staff_id": 12, "staff_name": "Maria Lopez",  "location_id": 1, "location_name": "Main St",  "period": "2025-02", "total_booked": 54, "completed_count": 47, "cancelled_count": 6, "no_show_count": 1, "no_show_rate_pct": 1.85, "distinct_services_handled": 4, "mom_growth_pct": -6.90},
        {"staff_id": 12, "staff_name": "Maria Lopez",  "location_id": 1, "location_name": "Main St",  "period": "2025-03", "total_booked": 65, "completed_count": 58, "cancelled_count": 6, "no_show_count": 1, "no_show_rate_pct": 1.54, "distinct_services_handled": 4, "mom_growth_pct": 20.37},
        {"staff_id": 12, "staff_name": "Maria Lopez",  "location_id": 1, "location_name": "Main St",  "period": "2025-04", "total_booked": 63, "completed_count": 56, "cancelled_count": 6, "no_show_count": 1, "no_show_rate_pct": 1.59, "distinct_services_handled": 4, "mom_growth_pct": -3.08},
        {"staff_id": 12, "staff_name": "Maria Lopez",  "location_id": 1, "location_name": "Main St",  "period": "2025-05", "total_booked": 70, "completed_count": 62, "cancelled_count": 7, "no_show_count": 1, "no_show_rate_pct": 1.43, "distinct_services_handled": 4, "mom_growth_pct": 11.11},
        {"staff_id": 12, "staff_name": "Maria Lopez",  "location_id": 1, "location_name": "Main St",  "period": "2025-06", "total_booked": 77, "completed_count": 68, "cancelled_count": 7, "no_show_count": 2, "no_show_rate_pct": 2.60, "distinct_services_handled": 4, "mom_growth_pct": 10.00},

        # ── James Carter — steady growth ──────────────────────────────────────
        {"staff_id": 15, "staff_name": "James Carter", "location_id": 1, "location_name": "Main St",  "period": "2025-01", "total_booked": 54, "completed_count": 48, "cancelled_count": 5, "no_show_count": 1, "no_show_rate_pct": 1.85, "distinct_services_handled": 3, "mom_growth_pct": None},
        {"staff_id": 15, "staff_name": "James Carter", "location_id": 1, "location_name": "Main St",  "period": "2025-02", "total_booked": 51, "completed_count": 43, "cancelled_count": 7, "no_show_count": 1, "no_show_rate_pct": 1.96, "distinct_services_handled": 3, "mom_growth_pct": -5.56},
        {"staff_id": 15, "staff_name": "James Carter", "location_id": 1, "location_name": "Main St",  "period": "2025-03", "total_booked": 60, "completed_count": 52, "cancelled_count": 7, "no_show_count": 1, "no_show_rate_pct": 1.67, "distinct_services_handled": 3, "mom_growth_pct": 17.65},
        {"staff_id": 15, "staff_name": "James Carter", "location_id": 1, "location_name": "Main St",  "period": "2025-04", "total_booked": 58, "completed_count": 50, "cancelled_count": 7, "no_show_count": 1, "no_show_rate_pct": 1.72, "distinct_services_handled": 3, "mom_growth_pct": -3.33},
        {"staff_id": 15, "staff_name": "James Carter", "location_id": 1, "location_name": "Main St",  "period": "2025-05", "total_booked": 63, "completed_count": 54, "cancelled_count": 8, "no_show_count": 1, "no_show_rate_pct": 1.59, "distinct_services_handled": 3, "mom_growth_pct": 8.62},
        {"staff_id": 15, "staff_name": "James Carter", "location_id": 1, "location_name": "Main St",  "period": "2025-06", "total_booked": 68, "completed_count": 58, "cancelled_count": 8, "no_show_count": 2, "no_show_rate_pct": 2.94, "distinct_services_handled": 3, "mom_growth_pct": 7.94},

        # ── Aisha Nwosu — most evening bookings, works Westside ───────────────
        {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "location_id": 2, "location_name": "Westside", "period": "2025-01", "total_booked": 52, "completed_count": 46, "cancelled_count": 5, "no_show_count": 1, "no_show_rate_pct": 1.92, "distinct_services_handled": 3, "mom_growth_pct": None},
        {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "location_id": 2, "location_name": "Westside", "period": "2025-02", "total_booked": 51, "completed_count": 42, "cancelled_count": 8, "no_show_count": 1, "no_show_rate_pct": 1.96, "distinct_services_handled": 3, "mom_growth_pct": -1.92},
        {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "location_id": 2, "location_name": "Westside", "period": "2025-03", "total_booked": 62, "completed_count": 54, "cancelled_count": 7, "no_show_count": 1, "no_show_rate_pct": 1.61, "distinct_services_handled": 3, "mom_growth_pct": 21.57},
        {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "location_id": 2, "location_name": "Westside", "period": "2025-04", "total_booked": 60, "completed_count": 52, "cancelled_count": 7, "no_show_count": 1, "no_show_rate_pct": 1.67, "distinct_services_handled": 3, "mom_growth_pct": -3.23},
        {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "location_id": 2, "location_name": "Westside", "period": "2025-05", "total_booked": 66, "completed_count": 57, "cancelled_count": 8, "no_show_count": 1, "no_show_rate_pct": 1.52, "distinct_services_handled": 3, "mom_growth_pct": 10.00},
        {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "location_id": 2, "location_name": "Westside", "period": "2025-06", "total_booked": 72, "completed_count": 61, "cancelled_count": 9, "no_show_count": 2, "no_show_rate_pct": 2.78, "distinct_services_handled": 3, "mom_growth_pct": 9.09},

        # ── Tom Rivera — declining (now inactive, left after Jun) ─────────────
        {"staff_id": 21, "staff_name": "Tom Rivera",   "location_id": 2, "location_name": "Westside", "period": "2025-01", "total_booked": 40, "completed_count": 32, "cancelled_count": 7, "no_show_count": 1, "no_show_rate_pct": 2.50, "distinct_services_handled": 2, "mom_growth_pct": None},
        {"staff_id": 21, "staff_name": "Tom Rivera",   "location_id": 2, "location_name": "Westside", "period": "2025-02", "total_booked": 42, "completed_count": 31, "cancelled_count": 10, "no_show_count": 1, "no_show_rate_pct": 2.38, "distinct_services_handled": 2, "mom_growth_pct": 5.00},   # booked more but completed fewer
        {"staff_id": 21, "staff_name": "Tom Rivera",   "location_id": 2, "location_name": "Westside", "period": "2025-03", "total_booked": 44, "completed_count": 37, "cancelled_count": 6, "no_show_count": 1, "no_show_rate_pct": 2.27, "distinct_services_handled": 2, "mom_growth_pct": 4.76},
        {"staff_id": 21, "staff_name": "Tom Rivera",   "location_id": 2, "location_name": "Westside", "period": "2025-04", "total_booked": 42, "completed_count": 37, "cancelled_count": 4, "no_show_count": 1, "no_show_rate_pct": 2.38, "distinct_services_handled": 2, "mom_growth_pct": -4.55},
        {"staff_id": 21, "staff_name": "Tom Rivera",   "location_id": 2, "location_name": "Westside", "period": "2025-05", "total_booked": 41, "completed_count": 37, "cancelled_count": 3, "no_show_count": 1, "no_show_rate_pct": 2.44, "distinct_services_handled": 2, "mom_growth_pct": -2.38},
        {"staff_id": 21, "staff_name": "Tom Rivera",   "location_id": 2, "location_name": "Westside", "period": "2025-06", "total_booked": 40, "completed_count": 36, "cancelled_count": 3, "no_show_count": 1, "no_show_rate_pct": 2.50, "distinct_services_handled": 2, "mom_growth_pct": -2.44},
    ],
}


# ── /api/v1/leo/appointments/by-service ──────────────────────────────────────
# Services story:
#   Facial Treatment (1): #1 by bookings, highest repeat rate, skews afternoon
#   Swedish Massage (2):  #2 by bookings, long duration, evening-heavy
#   Hair Color (3):       fewest bookings but highest cancellation rate (takes long)
#   Manicure (4):         high volume, short duration, morning-heavy
#   Pedicure (5):         mid-volume, pairs with Manicure, growing
#
# total_booked per month across all services sums to org total_booked in monthly-summary
# (within rounding — some appointments have ServiceId=0 walk-ins, excluded)

SERVICE_BREAKDOWN = {
    "business_id": 42,
    "data": [
        # ── Facial Treatment — dominant, growing ─────────────────────────────
        {"service_id": 1, "service_name": "Facial Treatment", "period": "2025-01", "total_booked": 58, "completed_count": 52, "cancelled_count": 4,  "distinct_clients": 44, "repeat_visit_count": 14, "avg_scheduled_duration_min": 60.0, "avg_actual_duration_min": 63.2, "cancellation_rate_pct": 6.90,  "morning_count": 12, "afternoon_count": 30, "evening_count": 16},
        {"service_id": 1, "service_name": "Facial Treatment", "period": "2025-02", "total_booked": 52, "completed_count": 44, "cancelled_count": 7,  "distinct_clients": 40, "repeat_visit_count": 12, "avg_scheduled_duration_min": 60.0, "avg_actual_duration_min": 63.8, "cancellation_rate_pct": 13.46, "morning_count": 10, "afternoon_count": 27, "evening_count": 15},
        {"service_id": 1, "service_name": "Facial Treatment", "period": "2025-03", "total_booked": 68, "completed_count": 61, "cancelled_count": 5,  "distinct_clients": 50, "repeat_visit_count": 18, "avg_scheduled_duration_min": 60.0, "avg_actual_duration_min": 62.9, "cancellation_rate_pct": 7.35,  "morning_count": 14, "afternoon_count": 36, "evening_count": 18},
        {"service_id": 1, "service_name": "Facial Treatment", "period": "2025-04", "total_booked": 65, "completed_count": 58, "cancelled_count": 5,  "distinct_clients": 48, "repeat_visit_count": 17, "avg_scheduled_duration_min": 60.0, "avg_actual_duration_min": 63.1, "cancellation_rate_pct": 7.69,  "morning_count": 13, "afternoon_count": 34, "evening_count": 18},
        {"service_id": 1, "service_name": "Facial Treatment", "period": "2025-05", "total_booked": 72, "completed_count": 64, "cancelled_count": 6,  "distinct_clients": 53, "repeat_visit_count": 19, "avg_scheduled_duration_min": 60.0, "avg_actual_duration_min": 63.5, "cancellation_rate_pct": 8.33,  "morning_count": 15, "afternoon_count": 38, "evening_count": 19},
        {"service_id": 1, "service_name": "Facial Treatment", "period": "2025-06", "total_booked": 78, "completed_count": 70, "cancelled_count": 6,  "distinct_clients": 57, "repeat_visit_count": 21, "avg_scheduled_duration_min": 60.0, "avg_actual_duration_min": 63.7, "cancellation_rate_pct": 7.69,  "morning_count": 16, "afternoon_count": 41, "evening_count": 21},

        # ── Swedish Massage — long duration, evening-heavy ───────────────────
        {"service_id": 2, "service_name": "Swedish Massage",  "period": "2025-01", "total_booked": 44, "completed_count": 40, "cancelled_count": 3,  "distinct_clients": 36, "repeat_visit_count": 8,  "avg_scheduled_duration_min": 90.0, "avg_actual_duration_min": 92.4, "cancellation_rate_pct": 6.82,  "morning_count": 8,  "afternoon_count": 16, "evening_count": 20},
        {"service_id": 2, "service_name": "Swedish Massage",  "period": "2025-02", "total_booked": 41, "completed_count": 34, "cancelled_count": 6,  "distinct_clients": 33, "repeat_visit_count": 8,  "avg_scheduled_duration_min": 90.0, "avg_actual_duration_min": 93.1, "cancellation_rate_pct": 14.63, "morning_count": 7,  "afternoon_count": 14, "evening_count": 20},
        {"service_id": 2, "service_name": "Swedish Massage",  "period": "2025-03", "total_booked": 52, "completed_count": 46, "cancelled_count": 4,  "distinct_clients": 41, "repeat_visit_count": 11, "avg_scheduled_duration_min": 90.0, "avg_actual_duration_min": 91.8, "cancellation_rate_pct": 7.69,  "morning_count": 9,  "afternoon_count": 19, "evening_count": 24},
        {"service_id": 2, "service_name": "Swedish Massage",  "period": "2025-04", "total_booked": 50, "completed_count": 44, "cancelled_count": 4,  "distinct_clients": 39, "repeat_visit_count": 11, "avg_scheduled_duration_min": 90.0, "avg_actual_duration_min": 92.0, "cancellation_rate_pct": 8.00,  "morning_count": 9,  "afternoon_count": 18, "evening_count": 23},
        {"service_id": 2, "service_name": "Swedish Massage",  "period": "2025-05", "total_booked": 55, "completed_count": 49, "cancelled_count": 5,  "distinct_clients": 43, "repeat_visit_count": 12, "avg_scheduled_duration_min": 90.0, "avg_actual_duration_min": 92.6, "cancellation_rate_pct": 9.09,  "morning_count": 10, "afternoon_count": 20, "evening_count": 25},
        {"service_id": 2, "service_name": "Swedish Massage",  "period": "2025-06", "total_booked": 60, "completed_count": 53, "cancelled_count": 5,  "distinct_clients": 46, "repeat_visit_count": 14, "avg_scheduled_duration_min": 90.0, "avg_actual_duration_min": 92.2, "cancellation_rate_pct": 8.33,  "morning_count": 10, "afternoon_count": 22, "evening_count": 28},

        # ── Hair Color — fewest bookings, highest cancellation rate ───────────
        {"service_id": 3, "service_name": "Hair Color",       "period": "2025-01", "total_booked": 22, "completed_count": 18, "cancelled_count": 4,  "distinct_clients": 17, "repeat_visit_count": 5,  "avg_scheduled_duration_min": 120.0, "avg_actual_duration_min": 128.3, "cancellation_rate_pct": 18.18, "morning_count": 10, "afternoon_count": 8,  "evening_count": 4},
        {"service_id": 3, "service_name": "Hair Color",       "period": "2025-02", "total_booked": 20, "completed_count": 14, "cancelled_count": 6,  "distinct_clients": 13, "repeat_visit_count": 7,  "avg_scheduled_duration_min": 120.0, "avg_actual_duration_min": 130.1, "cancellation_rate_pct": 30.00, "morning_count": 9,  "afternoon_count": 7,  "evening_count": 4},  # worst cancellation month
        {"service_id": 3, "service_name": "Hair Color",       "period": "2025-03", "total_booked": 26, "completed_count": 22, "cancelled_count": 4,  "distinct_clients": 20, "repeat_visit_count": 6,  "avg_scheduled_duration_min": 120.0, "avg_actual_duration_min": 127.5, "cancellation_rate_pct": 15.38, "morning_count": 12, "afternoon_count": 10, "evening_count": 4},
        {"service_id": 3, "service_name": "Hair Color",       "period": "2025-04", "total_booked": 24, "completed_count": 20, "cancelled_count": 4,  "distinct_clients": 18, "repeat_visit_count": 6,  "avg_scheduled_duration_min": 120.0, "avg_actual_duration_min": 128.8, "cancellation_rate_pct": 16.67, "morning_count": 11, "afternoon_count": 9,  "evening_count": 4},
        {"service_id": 3, "service_name": "Hair Color",       "period": "2025-05", "total_booked": 27, "completed_count": 22, "cancelled_count": 5,  "distinct_clients": 20, "repeat_visit_count": 7,  "avg_scheduled_duration_min": 120.0, "avg_actual_duration_min": 127.9, "cancellation_rate_pct": 18.52, "morning_count": 12, "afternoon_count": 10, "evening_count": 5},
        {"service_id": 3, "service_name": "Hair Color",       "period": "2025-06", "total_booked": 28, "completed_count": 24, "cancelled_count": 4,  "distinct_clients": 21, "repeat_visit_count": 7,  "avg_scheduled_duration_min": 120.0, "avg_actual_duration_min": 128.0, "cancellation_rate_pct": 14.29, "morning_count": 13, "afternoon_count": 10, "evening_count": 5},

        # ── Manicure — high volume, short, morning-heavy ─────────────────────
        {"service_id": 4, "service_name": "Manicure",         "period": "2025-01", "total_booked": 48, "completed_count": 44, "cancelled_count": 3,  "distinct_clients": 38, "repeat_visit_count": 10, "avg_scheduled_duration_min": 30.0, "avg_actual_duration_min": 31.4, "cancellation_rate_pct": 6.25,  "morning_count": 26, "afternoon_count": 16, "evening_count": 6},
        {"service_id": 4, "service_name": "Manicure",         "period": "2025-02", "total_booked": 46, "completed_count": 38, "cancelled_count": 7,  "distinct_clients": 35, "repeat_visit_count": 11, "avg_scheduled_duration_min": 30.0, "avg_actual_duration_min": 31.9, "cancellation_rate_pct": 15.22, "morning_count": 24, "afternoon_count": 15, "evening_count": 7},
        {"service_id": 4, "service_name": "Manicure",         "period": "2025-03", "total_booked": 54, "completed_count": 48, "cancelled_count": 4,  "distinct_clients": 42, "repeat_visit_count": 12, "avg_scheduled_duration_min": 30.0, "avg_actual_duration_min": 31.2, "cancellation_rate_pct": 7.41,  "morning_count": 29, "afternoon_count": 18, "evening_count": 7},
        {"service_id": 4, "service_name": "Manicure",         "period": "2025-04", "total_booked": 52, "completed_count": 46, "cancelled_count": 5,  "distinct_clients": 40, "repeat_visit_count": 12, "avg_scheduled_duration_min": 30.0, "avg_actual_duration_min": 31.5, "cancellation_rate_pct": 9.62,  "morning_count": 28, "afternoon_count": 17, "evening_count": 7},
        {"service_id": 4, "service_name": "Manicure",         "period": "2025-05", "total_booked": 57, "completed_count": 51, "cancelled_count": 5,  "distinct_clients": 44, "repeat_visit_count": 13, "avg_scheduled_duration_min": 30.0, "avg_actual_duration_min": 31.3, "cancellation_rate_pct": 8.77,  "morning_count": 31, "afternoon_count": 19, "evening_count": 7},
        {"service_id": 4, "service_name": "Manicure",         "period": "2025-06", "total_booked": 61, "completed_count": 54, "cancelled_count": 6,  "distinct_clients": 46, "repeat_visit_count": 15, "avg_scheduled_duration_min": 30.0, "avg_actual_duration_min": 31.6, "cancellation_rate_pct": 9.84,  "morning_count": 33, "afternoon_count": 20, "evening_count": 8},

        # ── Pedicure — growing, pairs with manicure ───────────────────────────
        {"service_id": 5, "service_name": "Pedicure",         "period": "2025-01", "total_booked": 29, "completed_count": 24, "cancelled_count": 5,  "distinct_clients": 21, "repeat_visit_count": 8,  "avg_scheduled_duration_min": 45.0, "avg_actual_duration_min": 47.6, "cancellation_rate_pct": 17.24, "morning_count": 9,  "afternoon_count": 13, "evening_count": 7},
        {"service_id": 5, "service_name": "Pedicure",         "period": "2025-02", "total_booked": 27, "completed_count": 22, "cancelled_count": 4,  "distinct_clients": 19, "repeat_visit_count": 8,  "avg_scheduled_duration_min": 45.0, "avg_actual_duration_min": 48.2, "cancellation_rate_pct": 14.81, "morning_count": 8,  "afternoon_count": 12, "evening_count": 7},
        {"service_id": 5, "service_name": "Pedicure",         "period": "2025-03", "total_booked": 34, "completed_count": 28, "cancelled_count": 5,  "distinct_clients": 24, "repeat_visit_count": 10, "avg_scheduled_duration_min": 45.0, "avg_actual_duration_min": 47.3, "cancellation_rate_pct": 14.71, "morning_count": 10, "afternoon_count": 15, "evening_count": 9},
        {"service_id": 5, "service_name": "Pedicure",         "period": "2025-04", "total_booked": 33, "completed_count": 27, "cancelled_count": 5,  "distinct_clients": 23, "repeat_visit_count": 10, "avg_scheduled_duration_min": 45.0, "avg_actual_duration_min": 47.9, "cancellation_rate_pct": 15.15, "morning_count": 10, "afternoon_count": 14, "evening_count": 9},
        {"service_id": 5, "service_name": "Pedicure",         "period": "2025-05", "total_booked": 36, "completed_count": 30, "cancelled_count": 5,  "distinct_clients": 26, "repeat_visit_count": 10, "avg_scheduled_duration_min": 45.0, "avg_actual_duration_min": 47.7, "cancellation_rate_pct": 13.89, "morning_count": 11, "afternoon_count": 16, "evening_count": 9},
        {"service_id": 5, "service_name": "Pedicure",         "period": "2025-06", "total_booked": 38, "completed_count": 32, "cancelled_count": 5,  "distinct_clients": 27, "repeat_visit_count": 11, "avg_scheduled_duration_min": 45.0, "avg_actual_duration_min": 47.4, "cancellation_rate_pct": 13.16, "morning_count": 11, "afternoon_count": 17, "evening_count": 10},
    ],
}


# ── /api/v1/leo/appointments/staff-service-cross ─────────────────────────────
# Shows which staff handles which services — reflects specialisation:
#   Maria (12):  Facial Treatment + Manicure specialist
#   James (15):  Swedish Massage + Hair Color
#   Aisha (9):   Swedish Massage + Facial Treatment + Pedicure
#   Tom (21):    Manicure + Pedicure only (limited skills)

STAFF_SERVICE_CROSS = {
    "business_id": 42,
    "data": [
        # Maria Lopez — Facial Treatment (dominant) + Manicure
        {"staff_id": 12, "staff_name": "Maria Lopez",  "service_id": 1, "service_name": "Facial Treatment", "period": "2025-06", "total_booked": 46, "completed_count": 41},
        {"staff_id": 12, "staff_name": "Maria Lopez",  "service_id": 4, "service_name": "Manicure",         "period": "2025-06", "total_booked": 24, "completed_count": 22},
        {"staff_id": 12, "staff_name": "Maria Lopez",  "service_id": 5, "service_name": "Pedicure",         "period": "2025-06", "total_booked": 7,  "completed_count": 5},

        # James Carter — Swedish Massage + Hair Color
        {"staff_id": 15, "staff_name": "James Carter", "service_id": 2, "service_name": "Swedish Massage",  "period": "2025-06", "total_booked": 36, "completed_count": 32},
        {"staff_id": 15, "staff_name": "James Carter", "service_id": 3, "service_name": "Hair Color",       "period": "2025-06", "total_booked": 22, "completed_count": 19},
        {"staff_id": 15, "staff_name": "James Carter", "service_id": 1, "service_name": "Facial Treatment", "period": "2025-06", "total_booked": 10, "completed_count": 7},

        # Aisha Nwosu — Swedish Massage + Facial Treatment + Pedicure
        {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "service_id": 2, "service_name": "Swedish Massage",  "period": "2025-06", "total_booked": 30, "completed_count": 26},
        {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "service_id": 1, "service_name": "Facial Treatment", "period": "2025-06", "total_booked": 22, "completed_count": 19},
        {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "service_id": 5, "service_name": "Pedicure",         "period": "2025-06", "total_booked": 20, "completed_count": 16},

        # Tom Rivera — Manicure + Pedicure only (limited service range)
        {"staff_id": 21, "staff_name": "Tom Rivera",   "service_id": 4, "service_name": "Manicure",         "period": "2025-06", "total_booked": 25, "completed_count": 22},
        {"staff_id": 21, "staff_name": "Tom Rivera",   "service_id": 5, "service_name": "Pedicure",         "period": "2025-06", "total_booked": 15, "completed_count": 14},
    ],
}


# ── Lookup: endpoint path → fixture ──────────────────────────────────────────
FIXTURES: dict[str, dict] = {
    "/api/v1/leo/appointments/monthly-summary":  MONTHLY_SUMMARY,
    "/api/v1/leo/appointments/by-staff":         STAFF_BREAKDOWN,
    "/api/v1/leo/appointments/by-service":       SERVICE_BREAKDOWN,
    "/api/v1/leo/appointments/staff-service-cross": STAFF_SERVICE_CROSS,
}
