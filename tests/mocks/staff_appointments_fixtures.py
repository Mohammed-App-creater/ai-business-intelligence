"""
tests/mocks/staff_attendance_fixtures.py

Mock response data for the staff attendance endpoint only.
This is the ONLY staff-specific fixture that doesn't overlap with an existing domain.

WHY THIS EXISTS:
  Hours worked (Q33) comes from tbl_attendance — no other domain touches this.
  No-shows, cancellations, and completion rates are handled by the
  Appointments domain (appointments_fixtures.py / appointments_fixtures_2026.py).

Time format confirmed by team (2026-04-13):
  time_sign_in / time_sign_out stored as '10:44:15 PM' (12-hour with seconds).
  No-time value = '0'. Date anchor = RecTimeDate (datetime column).
  Overnight shifts handled server-side with +1440 min guard.
  Duration capped at 24h/day as sanity check.

Stories baked in:
  - Maria: most hours (8.5h/day) — Q33 trigger ✓
  - Tom: declining hours Jan→Jun 2025, then no 2026 rows (left) ✓
  - Feb: fewer working days (shorter month) ✓
  - days_missing_signout: small realistic counts (forgot to sign out) ✓
  - Feb Tom: 1 day has same in/out time — dev DB artifact documented ✓
"""


# ─────────────────────────────────────────────────────────────────────────────
# /api/v1/leo/staff-attendance
# Grain: one row per (staff_id × location_id × period_label)
# Source: tbl_attendance — confirmed format '10:44:15 PM'
# Powers: Q33 (who clocked the most hours)
# ─────────────────────────────────────────────────────────────────────────────

STAFF_ATTENDANCE = {
    "business_id": 42,
    "data": [

        # ══ MARIA LOPEZ — most hours on team ══════════════════════════════════
        # 8.5h/day × 22 working days = 187h/month — Q33 trigger (highest)

        {"business_id": 42, "staff_id": 12, "staff_full_name": "Maria Lopez",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 1, "period_label": "2025-01",
         "days_with_signin": 22, "days_fully_recorded": 21, "days_missing_signout": 1,
         "total_hours_worked": 178.50, "avg_hours_per_day": 8.50},

        {"business_id": 42, "staff_id": 12, "staff_full_name": "Maria Lopez",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 2, "period_label": "2025-02",
         "days_with_signin": 20, "days_fully_recorded": 20, "days_missing_signout": 0,
         "total_hours_worked": 170.00, "avg_hours_per_day": 8.50},

        {"business_id": 42, "staff_id": 12, "staff_full_name": "Maria Lopez",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 3, "period_label": "2025-03",
         "days_with_signin": 22, "days_fully_recorded": 22, "days_missing_signout": 0,
         "total_hours_worked": 187.00, "avg_hours_per_day": 8.50},

        {"business_id": 42, "staff_id": 12, "staff_full_name": "Maria Lopez",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 4, "period_label": "2025-04",
         "days_with_signin": 22, "days_fully_recorded": 21, "days_missing_signout": 1,
         "total_hours_worked": 178.50, "avg_hours_per_day": 8.50},

        {"business_id": 42, "staff_id": 12, "staff_full_name": "Maria Lopez",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 5, "period_label": "2025-05",
         "days_with_signin": 22, "days_fully_recorded": 22, "days_missing_signout": 0,
         "total_hours_worked": 187.00, "avg_hours_per_day": 8.50},

        {"business_id": 42, "staff_id": 12, "staff_full_name": "Maria Lopez",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 6, "period_label": "2025-06",
         "days_with_signin": 22, "days_fully_recorded": 22, "days_missing_signout": 0,
         "total_hours_worked": 187.00, "avg_hours_per_day": 8.50},

        # ══ JAMES CARTER — solid hours ════════════════════════════════════════

        {"business_id": 42, "staff_id": 15, "staff_full_name": "James Carter",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 1, "period_label": "2025-01",
         "days_with_signin": 22, "days_fully_recorded": 22, "days_missing_signout": 0,
         "total_hours_worked": 180.40, "avg_hours_per_day": 8.20},

        {"business_id": 42, "staff_id": 15, "staff_full_name": "James Carter",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 2, "period_label": "2025-02",
         "days_with_signin": 20, "days_fully_recorded": 20, "days_missing_signout": 0,
         "total_hours_worked": 164.00, "avg_hours_per_day": 8.20},

        {"business_id": 42, "staff_id": 15, "staff_full_name": "James Carter",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 3, "period_label": "2025-03",
         "days_with_signin": 22, "days_fully_recorded": 21, "days_missing_signout": 1,
         "total_hours_worked": 172.20, "avg_hours_per_day": 8.20},

        {"business_id": 42, "staff_id": 15, "staff_full_name": "James Carter",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 4, "period_label": "2025-04",
         "days_with_signin": 22, "days_fully_recorded": 22, "days_missing_signout": 0,
         "total_hours_worked": 180.40, "avg_hours_per_day": 8.20},

        {"business_id": 42, "staff_id": 15, "staff_full_name": "James Carter",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 5, "period_label": "2025-05",
         "days_with_signin": 22, "days_fully_recorded": 22, "days_missing_signout": 0,
         "total_hours_worked": 180.40, "avg_hours_per_day": 8.20},

        {"business_id": 42, "staff_id": 15, "staff_full_name": "James Carter",
         "is_active": True, "location_id": 1, "location_name": "Main St",
         "year": 2025, "month": 6, "period_label": "2025-06",
         "days_with_signin": 22, "days_fully_recorded": 22, "days_missing_signout": 0,
         "total_hours_worked": 180.40, "avg_hours_per_day": 8.20},

        # ══ AISHA NWOSU — evening shifts, Westside ════════════════════════════

        {"business_id": 42, "staff_id": 9, "staff_full_name": "Aisha Nwosu",
         "is_active": True, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 1, "period_label": "2025-01",
         "days_with_signin": 21, "days_fully_recorded": 21, "days_missing_signout": 0,
         "total_hours_worked": 168.00, "avg_hours_per_day": 8.00},

        {"business_id": 42, "staff_id": 9, "staff_full_name": "Aisha Nwosu",
         "is_active": True, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 2, "period_label": "2025-02",
         "days_with_signin": 20, "days_fully_recorded": 19, "days_missing_signout": 1,
         "total_hours_worked": 152.00, "avg_hours_per_day": 8.00},

        {"business_id": 42, "staff_id": 9, "staff_full_name": "Aisha Nwosu",
         "is_active": True, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 3, "period_label": "2025-03",
         "days_with_signin": 21, "days_fully_recorded": 21, "days_missing_signout": 0,
         "total_hours_worked": 168.00, "avg_hours_per_day": 8.00},

        {"business_id": 42, "staff_id": 9, "staff_full_name": "Aisha Nwosu",
         "is_active": True, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 4, "period_label": "2025-04",
         "days_with_signin": 21, "days_fully_recorded": 21, "days_missing_signout": 0,
         "total_hours_worked": 168.00, "avg_hours_per_day": 8.00},

        {"business_id": 42, "staff_id": 9, "staff_full_name": "Aisha Nwosu",
         "is_active": True, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 5, "period_label": "2025-05",
         "days_with_signin": 22, "days_fully_recorded": 22, "days_missing_signout": 0,
         "total_hours_worked": 176.00, "avg_hours_per_day": 8.00},

        {"business_id": 42, "staff_id": 9, "staff_full_name": "Aisha Nwosu",
         "is_active": True, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 6, "period_label": "2025-06",
         "days_with_signin": 22, "days_fully_recorded": 22, "days_missing_signout": 0,
         "total_hours_worked": 176.00, "avg_hours_per_day": 8.00},

        # ══ TOM RIVERA — declining hours as he prepares to leave ══════════════
        # Q33: lowest hours AND clear downward trend Jan→Jun 2025.
        # Feb note: 1 day has same sign_in/sign_out → 0h contributed (dev DB artifact).
        # is_active=False — deactivated staff attendance still returned ✓
        # NO 2026 rows — left after Jun 2025 ✓

        {"business_id": 42, "staff_id": 21, "staff_full_name": "Tom Rivera",
         "is_active": False, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 1, "period_label": "2025-01",
         "days_with_signin": 20, "days_fully_recorded": 20, "days_missing_signout": 0,
         "total_hours_worked": 140.00, "avg_hours_per_day": 7.00},

        {"business_id": 42, "staff_id": 21, "staff_full_name": "Tom Rivera",
         "is_active": False, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 2, "period_label": "2025-02",
         # 1 day: same in/out time (dev DB artifact) → 0h, avg slightly dragged down
         "days_with_signin": 19, "days_fully_recorded": 19, "days_missing_signout": 0,
         "total_hours_worked": 125.30, "avg_hours_per_day": 6.60},

        {"business_id": 42, "staff_id": 21, "staff_full_name": "Tom Rivera",
         "is_active": False, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 3, "period_label": "2025-03",
         "days_with_signin": 20, "days_fully_recorded": 19, "days_missing_signout": 1,
         "total_hours_worked": 127.30, "avg_hours_per_day": 6.70},

        {"business_id": 42, "staff_id": 21, "staff_full_name": "Tom Rivera",
         "is_active": False, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 4, "period_label": "2025-04",
         "days_with_signin": 18, "days_fully_recorded": 18, "days_missing_signout": 0,
         "total_hours_worked": 117.00, "avg_hours_per_day": 6.50},

        {"business_id": 42, "staff_id": 21, "staff_full_name": "Tom Rivera",
         "is_active": False, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 5, "period_label": "2025-05",
         "days_with_signin": 16, "days_fully_recorded": 16, "days_missing_signout": 0,
         "total_hours_worked": 104.00, "avg_hours_per_day": 6.50},

        {"business_id": 42, "staff_id": 21, "staff_full_name": "Tom Rivera",
         "is_active": False, "location_id": 2, "location_name": "Westside",
         "year": 2025, "month": 6, "period_label": "2025-06",
         "days_with_signin": 14, "days_fully_recorded": 14, "days_missing_signout": 0,
         "total_hours_worked": 91.00, "avg_hours_per_day": 6.50},
        # No 2026 rows for Tom ✓
    ],
    "meta": {
        "most_hours_staff_id":    12,
        "most_hours_staff_name":  "Maria Lopez",
        "least_hours_staff_id":   21,
        "least_hours_staff_name": "Tom Rivera",
        "period_from":            "2025-01",
        "period_to":              "2025-06",
        "data_quality_notes": [
            {
                "staff_id":    21,
                "period":      "2025-02",
                "issue":       "same_time_in_out",
                "description": "1 attendance row has identical time_sign_in and time_sign_out — contributes 0h. Dev DB artifact.",
            }
        ],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Fixture lookup
# ─────────────────────────────────────────────────────────────────────────────

FIXTURES: dict[str, dict] = {
    "/api/v1/leo/staff-attendance": STAFF_ATTENDANCE,
}