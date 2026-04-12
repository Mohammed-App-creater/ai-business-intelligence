"""
tests/mocks/appointments_fixtures_2026.py
==========================================
Extension to appointments_fixtures.py — adds Feb and Mar 2026 data
so "last month" (Mar 2026) and "this month" (Apr 2026) questions
have real data to retrieve.

Story for 2026 data:
  - Tom Rivera left (no rows for 2026)
  - Maria Lopez continues growing
  - James Carter and Aisha Nwosu stable
  - Hair Color cancellation rate improved (new stylist James getting better)
  - Main St still mornings, Westside still evenings
  - Overall volume growing ~5% MoM continuing the 2025 trend

HOW TO USE:
  Merge MONTHLY_SUMMARY_2026, STAFF_BREAKDOWN_2026, SERVICE_BREAKDOWN_2026
  into the corresponding 2025 fixtures by extending their "data" lists.

  In appointments_fixtures.py, update MONTHLY_SUMMARY, STAFF_BREAKDOWN,
  SERVICE_BREAKDOWN to include these additional rows.

  OR update mock_analytics_server.py to merge both datasets.
"""

# ── Feb 2026 + Mar 2026 monthly summary rows ──────────────────────────────────
# Feb 2026: slight dip (seasonal — post-holiday)
# Mar 2026: recovery, growth

MONTHLY_SUMMARY_2026_ROWS = [

    # ── February 2026 ─────────────────────────────────────────────────────────
    {
        "period":                   "2026-02",
        "location_id":              1,
        "location_name":            "Main St",
        "location_city":            "Miami",
        "total_booked":             145,
        "confirmed_count":          140,
        "completed_count":          128,
        "cancelled_count":          14,
        "no_show_count":            3,
        "morning_count":            60,
        "afternoon_count":          52,
        "evening_count":            33,
        "weekend_count":            46,
        "weekday_count":            99,
        "avg_actual_duration_min":  56.1,
        "cancellation_rate_pct":    9.66,
        "no_show_rate_pct":         2.07,
        "mom_growth_pct":           None,   # first 2026 period in fixture window
        "walkin_count":             24,
        "app_booking_count":        121,
        "peak_slot":                "morning",
    },
    {
        "period":                   "2026-02",
        "location_id":              2,
        "location_name":            "Westside",
        "location_city":            "Miami",
        "total_booked":             121,
        "confirmed_count":          116,
        "completed_count":          106,
        "cancelled_count":          12,
        "no_show_count":            3,
        "morning_count":            30,
        "afternoon_count":          43,
        "evening_count":            48,
        "weekend_count":            41,
        "weekday_count":            80,
        "avg_actual_duration_min":  53.0,
        "cancellation_rate_pct":    9.92,
        "no_show_rate_pct":         2.48,
        "mom_growth_pct":           None,
        "walkin_count":             26,
        "app_booking_count":        95,
        "peak_slot":                "evening",
    },
    {
        "period":                   "2026-02",
        "location_id":              0,
        "location_name":            "__ALL__",
        "location_city":            "",
        "total_booked":             266,
        "confirmed_count":          256,
        "completed_count":          234,
        "cancelled_count":          26,
        "no_show_count":            6,
        "morning_count":            90,
        "afternoon_count":          95,
        "evening_count":            81,
        "weekend_count":            87,
        "weekday_count":            179,
        "avg_actual_duration_min":  54.7,
        "cancellation_rate_pct":    9.77,
        "no_show_rate_pct":         2.26,
        "mom_growth_pct":           None,
        "walkin_count":             50,
        "app_booking_count":        216,
        "peak_slot":                "afternoon",
    },

    # ── March 2026 — "last month" relative to Apr 2026 ────────────────────────
    {
        "period":                   "2026-03",
        "location_id":              1,
        "location_name":            "Main St",
        "location_city":            "Miami",
        "total_booked":             152,
        "confirmed_count":          147,
        "completed_count":          135,
        "cancelled_count":          14,
        "no_show_count":            3,
        "morning_count":            64,
        "afternoon_count":          55,
        "evening_count":            33,
        "weekend_count":            49,
        "weekday_count":            103,
        "avg_actual_duration_min":  56.4,
        "cancellation_rate_pct":    9.21,
        "no_show_rate_pct":         1.97,
        "mom_growth_pct":           4.83,
        "walkin_count":             25,
        "app_booking_count":        127,
        "peak_slot":                "morning",
    },
    {
        "period":                   "2026-03",
        "location_id":              2,
        "location_name":            "Westside",
        "location_city":            "Miami",
        "total_booked":             127,
        "confirmed_count":          122,
        "completed_count":          112,
        "cancelled_count":          12,
        "no_show_count":            3,
        "morning_count":            32,
        "afternoon_count":          45,
        "evening_count":            50,
        "weekend_count":            43,
        "weekday_count":            84,
        "avg_actual_duration_min":  53.3,
        "cancellation_rate_pct":    9.45,
        "no_show_rate_pct":         2.36,
        "mom_growth_pct":           4.96,
        "walkin_count":             27,
        "app_booking_count":        100,
        "peak_slot":                "evening",
    },
    {
        "period":                   "2026-03",
        "location_id":              0,
        "location_name":            "__ALL__",
        "location_city":            "",
        "total_booked":             279,
        "confirmed_count":          269,
        "completed_count":          247,
        "cancelled_count":          26,
        "no_show_count":            6,
        "morning_count":            96,
        "afternoon_count":          100,
        "evening_count":            83,
        "weekend_count":            92,
        "weekday_count":            187,
        "avg_actual_duration_min":  55.0,
        "cancellation_rate_pct":    9.32,
        "no_show_rate_pct":         2.15,
        "mom_growth_pct":           4.89,
        "walkin_count":             52,
        "app_booking_count":        227,
        "peak_slot":                "afternoon",
    },
]

# ── Feb + Mar 2026 staff breakdown ────────────────────────────────────────────
# Tom Rivera gone (no rows). 3 active staff.

STAFF_BREAKDOWN_2026_ROWS = [
    # Maria Lopez — absorbing some of Tom's former clients
    {"staff_id": 12, "staff_name": "Maria Lopez",  "location_id": 1, "location_name": "Main St",  "period": "2026-02", "total_booked": 92, "completed_count": 83, "completion_rate_pct": 90.2, "cancelled_count": 8, "no_show_count": 1, "no_show_rate_pct": 1.09, "distinct_services_handled": 4, "mom_growth_pct": None},
    {"staff_id": 12, "staff_name": "Maria Lopez",  "location_id": 1, "location_name": "Main St",  "period": "2026-03", "total_booked": 96, "completed_count": 87, "completion_rate_pct": 90.6, "cancelled_count": 8, "no_show_count": 1, "no_show_rate_pct": 1.04, "distinct_services_handled": 4, "mom_growth_pct": 4.35},
    # James Carter
    {"staff_id": 15, "staff_name": "James Carter", "location_id": 1, "location_name": "Main St",  "period": "2026-02", "total_booked": 84, "completed_count": 74, "completion_rate_pct": 88.1, "cancelled_count": 8, "no_show_count": 2, "no_show_rate_pct": 2.38, "distinct_services_handled": 3, "mom_growth_pct": None},
    {"staff_id": 15, "staff_name": "James Carter", "location_id": 1, "location_name": "Main St",  "period": "2026-03", "total_booked": 87, "completed_count": 79, "completion_rate_pct": 90.8, "cancelled_count": 7, "no_show_count": 1, "no_show_rate_pct": 1.15, "distinct_services_handled": 3, "mom_growth_pct": 3.57},
    # Aisha Nwosu — at Westside, highest completed count there
    {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "location_id": 2, "location_name": "Westside", "period": "2026-02", "total_booked": 87, "completed_count": 77, "completion_rate_pct": 88.5, "cancelled_count": 8, "no_show_count": 2, "no_show_rate_pct": 2.30, "distinct_services_handled": 3, "mom_growth_pct": None},
    {"staff_id": 9,  "staff_name": "Aisha Nwosu",  "location_id": 2, "location_name": "Westside", "period": "2026-03", "total_booked": 91, "completed_count": 81, "completion_rate_pct": 89.0, "cancelled_count": 9, "no_show_count": 1, "no_show_rate_pct": 1.10, "distinct_services_handled": 3, "mom_growth_pct": 4.60},
]

# ── Feb + Mar 2026 service breakdown ─────────────────────────────────────────

SERVICE_BREAKDOWN_2026_ROWS = [
    # Facial Treatment
    {"service_id": 1, "service_name": "Facial Treatment", "period": "2026-02", "total_booked": 84, "completed_count": 75, "cancelled_count": 7,  "distinct_clients": 61, "repeat_visit_count": 23, "avg_scheduled_duration_min": 60.0, "avg_actual_duration_min": 63.4, "cancellation_rate_pct": 8.33,  "morning_count": 17, "afternoon_count": 44, "evening_count": 23},
    {"service_id": 1, "service_name": "Facial Treatment", "period": "2026-03", "total_booked": 88, "completed_count": 79, "cancelled_count": 7,  "distinct_clients": 64, "repeat_visit_count": 24, "avg_scheduled_duration_min": 60.0, "avg_actual_duration_min": 63.6, "cancellation_rate_pct": 7.95,  "morning_count": 18, "afternoon_count": 46, "evening_count": 24},
    # Swedish Massage
    {"service_id": 2, "service_name": "Swedish Massage",  "period": "2026-02", "total_booked": 64, "completed_count": 57, "cancelled_count": 5,  "distinct_clients": 50, "repeat_visit_count": 14, "avg_scheduled_duration_min": 90.0, "avg_actual_duration_min": 92.8, "cancellation_rate_pct": 7.81,  "morning_count": 11, "afternoon_count": 23, "evening_count": 30},
    {"service_id": 2, "service_name": "Swedish Massage",  "period": "2026-03", "total_booked": 67, "completed_count": 60, "cancelled_count": 5,  "distinct_clients": 52, "repeat_visit_count": 15, "avg_scheduled_duration_min": 90.0, "avg_actual_duration_min": 92.5, "cancellation_rate_pct": 7.46,  "morning_count": 11, "afternoon_count": 24, "evening_count": 32},
    # Hair Color
    {"service_id": 3, "service_name": "Hair Color",       "period": "2026-02", "total_booked": 30, "completed_count": 26, "cancelled_count": 4,  "distinct_clients": 23, "repeat_visit_count": 7,  "avg_scheduled_duration_min": 120.0, "avg_actual_duration_min": 127.0, "cancellation_rate_pct": 13.33, "morning_count": 14, "afternoon_count": 11, "evening_count": 5},
    {"service_id": 3, "service_name": "Hair Color",       "period": "2026-03", "total_booked": 32, "completed_count": 28, "cancelled_count": 4,  "distinct_clients": 25, "repeat_visit_count": 7,  "avg_scheduled_duration_min": 120.0, "avg_actual_duration_min": 126.5, "cancellation_rate_pct": 12.50, "morning_count": 15, "afternoon_count": 12, "evening_count": 5},
    # Manicure
    {"service_id": 4, "service_name": "Manicure",         "period": "2026-02", "total_booked": 65, "completed_count": 58, "cancelled_count": 6,  "distinct_clients": 50, "repeat_visit_count": 15, "avg_scheduled_duration_min": 30.0, "avg_actual_duration_min": 31.5, "cancellation_rate_pct": 9.23,  "morning_count": 35, "afternoon_count": 22, "evening_count": 8},
    {"service_id": 4, "service_name": "Manicure",         "period": "2026-03", "total_booked": 68, "completed_count": 61, "cancelled_count": 6,  "distinct_clients": 52, "repeat_visit_count": 16, "avg_scheduled_duration_min": 30.0, "avg_actual_duration_min": 31.3, "cancellation_rate_pct": 8.82,  "morning_count": 37, "afternoon_count": 23, "evening_count": 8},
    # Pedicure
    {"service_id": 5, "service_name": "Pedicure",         "period": "2026-02", "total_booked": 43, "completed_count": 38, "cancelled_count": 4,  "distinct_clients": 31, "repeat_visit_count": 12, "avg_scheduled_duration_min": 45.0, "avg_actual_duration_min": 47.2, "cancellation_rate_pct": 9.30,  "morning_count": 13, "afternoon_count": 19, "evening_count": 11},
    {"service_id": 5, "service_name": "Pedicure",         "period": "2026-03", "total_booked": 45, "completed_count": 40, "cancelled_count": 4,  "distinct_clients": 33, "repeat_visit_count": 12, "avg_scheduled_duration_min": 45.0, "avg_actual_duration_min": 47.0, "cancellation_rate_pct": 8.89,  "morning_count": 14, "afternoon_count": 20, "evening_count": 11},
]
