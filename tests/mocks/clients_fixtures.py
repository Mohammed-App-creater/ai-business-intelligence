"""
tests/mocks/clients_fixtures.py
================================
Realistic mock response data for the 3 Clients endpoints:
  EP1: /api/v1/leo/clients/retention-snapshot    → RETENTION_SNAPSHOT
  EP2: /api/v1/leo/clients/cohort-monthly         → COHORT_MONTHLY
  EP3: /api/v1/leo/clients/per-location-monthly   → PER_LOCATION_MONTHLY

Based on the same salon business (business_id=42) with 2 locations (Main St +
Westside) and 4 staff, weaving into the story from revenue/appointments/staff/
services fixtures.

── Consistency anchors with existing fixtures ────────────────────────────────
• business_id=42, locations 1=Main St, 2=Westside (same as appointments)
• Tom Rivera left after Jun 2025 — his former regulars are in 2026 either:
    - Reassigned to Aisha at Westside (they kept visiting)
    - Became at-risk (haven't been back since 2025)
• Feb 2026 = weak month (cancellation spike) → fewer new clients in Feb
• Mar 2026 = recovery → more new clients, Express Facial (new service) attracts some
• Main St dominates new-client acquisition (matches WELCOME10: 38 Main St vs 27 Westside)
• Facial Treatment is most popular → top LTV clients are Facial regulars
• Express Facial launched Feb 2026 → 2 of the new clients tried it first

── Sizing note ───────────────────────────────────────────────────────────────
Appointment fixtures imply ~80 unique active clients per month. For test
tractability this fixture uses 38 tracked clients total for biz 42 — they cover
every edge case the 23 acceptance questions exercise. Aggregate counts in EP2
are self-consistent with these 38 (not scaled to appointment volumes).

── Client roster (38 for biz 42) ─────────────────────────────────────────────
    IDs 1001-1005: Top 5 LTV — Maria's/Aisha's regulars, 2 members
    IDs 1006-1012: Normal active (7 clients) — 2 members
    IDs 1013-1017: New in Mar 2026 (5 clients, 3 at Main St, 2 at Westside)
    IDs 1018-1022: New in Feb 2026, returning in Mar (5 clients)
    IDs 1023-1027: At-risk (5 clients, last visit Dec 2025 – Jan 2026)
    IDs 1028-1030: Reactivated in Mar after 90+ day gap (3 clients)
    ID 1031:       Jane Smith (PII decline test target, normal active)
    ID 1032:       Unsubscribed email (normal active)
    ID 1033:       Unsubscribed SMS (normal active)
    ID 1034:       Soft-deleted globally (tbl_customers.Active=0)
    ID 1035:       Soft-deleted per-tenant (tbl_custorg.Active=0)
    IDs 1036-1038: New in Feb 2026, did NOT return in Mar (for Q10 drop story)

── Members (is_member=True) ──────────────────────────────────────────────────
8 of 35 clients: 1003, 1004, 1005, 1007, 1010, 1018, 1028, 1031

── Reference date ────────────────────────────────────────────────────────────
ref_date = 2026-04-01 (set via LEO_TEST_REF_DATE env var)
  → "last month" resolves to 2026-03
  → age computed against this date
  → days_since_last_visit computed against this date
  → at_risk_flag: days_since_last_visit > 60 (before 2026-02-01)

── Q23 walk-in vs booking dedup ──────────────────────────────────────────────
All unique-visit counts here assume the schema-level dedup from tbl_visit
(single source of truth for served visits). unique_visitors_in_period is the
count of DISTINCT clients who had ≥1 tbl_visit row in the period — already
deduplicated across booking/signin/walk-in channels.
"""

from __future__ import annotations


# ─────────────────────────────────────────────────────────────────────────────
# Per-client row helper
# ─────────────────────────────────────────────────────────────────────────────

def _client(
    client_id, first, last, email, mobile, age, dob, points,
    first_visit_ever, last_visit, total_visits_ever, visits_mar_2026,
    lifetime_revenue, lifetime_tips, revenue_mar_2026,
    home_loc_id, first_visit_loc_id,
    is_not_deleted=True, is_reachable_email=True, is_reachable_sms=True,
    is_member=False,
    is_new_in_period=False, is_returning_in_period=False,
    is_reactivated_in_period=False,
):
    """Build one per-client row for EP1 response."""
    # Age bracket
    if age is None:
        bracket = "unknown"
    elif age < 25:
        bracket = "under_25"
    elif age < 40:
        bracket = "25_to_40"
    elif age < 55:
        bracket = "40_to_55"
    else:
        bracket = "55_plus"

    # Derived fields
    ref_date = "2026-04-01"
    if last_visit:
        from datetime import date
        ld = date.fromisoformat(last_visit)
        days_since = (date.fromisoformat(ref_date) - ld).days
    else:
        days_since = None

    avg_ticket = round(lifetime_revenue / total_visits_ever, 2) if total_visits_ever else None
    lifetime_total_paid = round(lifetime_revenue + lifetime_tips, 2)

    # at_risk: days_since > 60 AND is_not_deleted
    at_risk_flag = bool(days_since is not None and days_since > 60 and is_not_deleted)

    location_names = {1: "Main St", 2: "Westside", None: None}

    return {
        "client_id":                 client_id,
        "business_id":               42,
        "first_name":                first,
        "last_name":                 last,
        "age":                       age,
        "age_bracket":               bracket,
        "points":                    points,
        "first_visit_ever_date":     first_visit_ever,
        "last_visit_date":           last_visit,
        "days_since_last_visit":     days_since,
        "total_visits_ever":         total_visits_ever,
        "visits_in_period":          visits_mar_2026,
        "lifetime_revenue":          lifetime_revenue,
        "lifetime_tips":             lifetime_tips,
        "lifetime_total_paid":       lifetime_total_paid,
        "revenue_in_period":         revenue_mar_2026,
        "avg_ticket":                avg_ticket,
        "home_location_id":          home_loc_id,
        "home_location_name":        location_names.get(home_loc_id),
        "first_visit_location_id":   first_visit_loc_id,
        "first_visit_location_name": location_names.get(first_visit_loc_id),
        "is_not_deleted":            is_not_deleted,
        "is_reachable_email":        is_reachable_email,
        "is_reachable_sms":          is_reachable_sms,
        "is_member":                 is_member,
        "is_new_in_period":          is_new_in_period,
        "is_returning_in_period":    is_returning_in_period,
        "is_reactivated_in_period":  is_reactivated_in_period,
        "at_risk_flag":              at_risk_flag,
        # Ranks populated after list built (see _assign_ranks below)
        "ltv_rank":                  None,
        "frequency_rank":            None,
        "points_rank":               None,
        "ltv_percentile_decile":     None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# The 35 clients for business_id=42
# ─────────────────────────────────────────────────────────────────────────────

_CLIENTS_42 = [

    # ══ TOP 5 LTV (1001-1005) — Maria's / Aisha's regulars ══════════════════
    # 1001: Maria Garcia — top spender, facial regular at Main St, not a member
    _client(1001, "Maria", "Garcia", "maria.garcia@example.com", "305-555-0101",
            42, "1983-08-15", 820.5,
            first_visit_ever="2024-03-12", last_visit="2026-03-28",
            total_visits_ever=47, visits_mar_2026=3,
            lifetime_revenue=4820.00, lifetime_tips=720.00, revenue_mar_2026=330.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_returning_in_period=True),

    # 1002: John Lee — massage regular at Westside (Aisha's), not a member
    _client(1002, "John", "Lee", "john.lee@example.com", "305-555-0102",
            51, "1975-02-20", 760.0,
            first_visit_ever="2024-05-04", last_visit="2026-03-30",
            total_visits_ever=45, visits_mar_2026=2,
            lifetime_revenue=4510.00, lifetime_tips=451.00, revenue_mar_2026=184.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_returning_in_period=True),

    # 1003: Sarah Chen — hair color at Main St, MEMBER
    _client(1003, "Sarah", "Chen", "sarah.chen@example.com", "305-555-0103",
            36, "1990-06-11", 680.0,
            first_visit_ever="2024-07-18", last_visit="2026-03-22",
            total_visits_ever=32, visits_mar_2026=1,
            lifetime_revenue=3890.00, lifetime_tips=389.00, revenue_mar_2026=108.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_member=True, is_returning_in_period=True),

    # 1004: Robert Wilson — multi-service at Westside, MEMBER
    _client(1004, "Robert", "Wilson", "robert.wilson@example.com", "305-555-0104",
            48, "1978-11-30", 615.0,
            first_visit_ever="2024-04-22", last_visit="2026-03-26",
            total_visits_ever=44, visits_mar_2026=3,
            lifetime_revenue=3650.00, lifetime_tips=438.00, revenue_mar_2026=275.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_member=True, is_returning_in_period=True),

    # 1005: Emily Brown — facial + manicure, Main St, MEMBER
    _client(1005, "Emily", "Brown", "emily.brown@example.com", "305-555-0105",
            29, "1997-01-08", 580.0,
            first_visit_ever="2024-08-14", last_visit="2026-03-25",
            total_visits_ever=40, visits_mar_2026=2,
            lifetime_revenue=3420.00, lifetime_tips=410.00, revenue_mar_2026=196.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_member=True, is_returning_in_period=True),

    # ══ NORMAL ACTIVE (1006-1012) — 7 clients, mid-LTV ═════════════════════
    # 1006: Michael Davis — Westside massage regular
    _client(1006, "Michael", "Davis", "michael.davis@example.com", "305-555-0106",
            44, "1982-04-17", 410.0,
            first_visit_ever="2024-10-02", last_visit="2026-03-20",
            total_visits_ever=28, visits_mar_2026=2,
            lifetime_revenue=2640.00, lifetime_tips=290.00, revenue_mar_2026=184.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_returning_in_period=True),

    # 1007: Jessica Martinez — Main St, MEMBER
    _client(1007, "Jessica", "Martinez", "jessica.martinez@example.com", "305-555-0107",
            33, "1993-09-22", 380.0,
            first_visit_ever="2025-01-15", last_visit="2026-03-18",
            total_visits_ever=25, visits_mar_2026=2,
            lifetime_revenue=2100.00, lifetime_tips=231.00, revenue_mar_2026=156.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_member=True, is_returning_in_period=True),

    # 1008: David Kim — Westside, facial regular
    _client(1008, "David", "Kim", "david.kim@example.com", "305-555-0108",
            38, "1988-12-05", 340.0,
            first_visit_ever="2025-02-08", last_visit="2026-03-15",
            total_visits_ever=22, visits_mar_2026=2,
            lifetime_revenue=1850.00, lifetime_tips=185.00, revenue_mar_2026=156.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_returning_in_period=True),

    # 1009: Linda Taylor — Main St, nails
    _client(1009, "Linda", "Taylor", "linda.taylor@example.com", "305-555-0109",
            58, "1968-05-14", 295.0,
            first_visit_ever="2025-03-10", last_visit="2026-03-10",
            total_visits_ever=20, visits_mar_2026=1,
            lifetime_revenue=1520.00, lifetime_tips=152.00, revenue_mar_2026=66.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_returning_in_period=True),

    # 1010: Christopher Johnson — Westside, MEMBER
    _client(1010, "Christopher", "Johnson", "chris.johnson@example.com", "305-555-0110",
            47, "1979-10-01", 270.0,
            first_visit_ever="2025-04-20", last_visit="2026-03-12",
            total_visits_ever=18, visits_mar_2026=1,
            lifetime_revenue=1380.00, lifetime_tips=138.00, revenue_mar_2026=92.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_member=True, is_returning_in_period=True),

    # 1011: Angela White — Main St
    _client(1011, "Angela", "White", "angela.white@example.com", "305-555-0111",
            26, "1999-07-23", 210.0,
            first_visit_ever="2025-05-12", last_visit="2026-03-24",
            total_visits_ever=16, visits_mar_2026=2,
            lifetime_revenue=1180.00, lifetime_tips=130.00, revenue_mar_2026=144.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_returning_in_period=True),

    # 1012: Brian Anderson — Westside
    _client(1012, "Brian", "Anderson", "brian.anderson@example.com", "305-555-0112",
            52, "1974-03-02", 195.0,
            first_visit_ever="2025-06-08", last_visit="2026-03-28",
            total_visits_ever=14, visits_mar_2026=1,
            lifetime_revenue=1060.00, lifetime_tips=106.00, revenue_mar_2026=92.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_returning_in_period=True),

    # ══ NEW IN MARCH 2026 (1013-1017) — Main St wins (3 vs 2) ═══════════════
    # Age spread: under_25, 25_to_40, 40_to_55, 55_plus, unknown
    # 1013: under_25 — first visit Main St Mar 5 (Express Facial — new service)
    _client(1013, "Sophia", "Rodriguez", "sophia.rodriguez@example.com", "305-555-0113",
            22, "2003-11-18", 15.0,
            first_visit_ever="2026-03-05", last_visit="2026-03-05",
            total_visits_ever=1, visits_mar_2026=1,
            lifetime_revenue=43.00, lifetime_tips=5.00, revenue_mar_2026=43.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_new_in_period=True),

    # 1014: 25_to_40 — Main St Mar 10 (used WELCOME10)
    _client(1014, "Daniel", "Park", "daniel.park@example.com", "305-555-0114",
            32, "1993-12-03", 20.0,
            first_visit_ever="2026-03-10", last_visit="2026-03-10",
            total_visits_ever=1, visits_mar_2026=1,
            lifetime_revenue=78.00, lifetime_tips=10.00, revenue_mar_2026=78.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_new_in_period=True),

    # 1015: 40_to_55 — Westside Mar 15 (Swedish Massage)
    _client(1015, "Patricia", "Moore", "patricia.moore@example.com", "305-555-0115",
            49, "1976-08-09", 25.0,
            first_visit_ever="2026-03-15", last_visit="2026-03-15",
            total_visits_ever=1, visits_mar_2026=1,
            lifetime_revenue=92.00, lifetime_tips=12.00, revenue_mar_2026=92.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_new_in_period=True),

    # 1016: 55_plus — Main St Mar 22 (Manicure)
    _client(1016, "Barbara", "Clark", "barbara.clark@example.com", "305-555-0116",
            63, "1962-04-27", 10.0,
            first_visit_ever="2026-03-22", last_visit="2026-03-22",
            total_visits_ever=1, visits_mar_2026=1,
            lifetime_revenue=33.00, lifetime_tips=5.00, revenue_mar_2026=33.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_new_in_period=True),

    # 1017: unknown age (NULL DOB) — Westside Mar 27 (Facial)
    _client(1017, "Alex", "Nguyen", "alex.nguyen@example.com", "305-555-0117",
            None, None, 15.0,
            first_visit_ever="2026-03-27", last_visit="2026-03-27",
            total_visits_ever=1, visits_mar_2026=1,
            lifetime_revenue=78.00, lifetime_tips=10.00, revenue_mar_2026=78.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_new_in_period=True),

    # ══ NEW IN FEB 2026, RETURNING IN MAR (1018-1022) ═══════════════════════
    # 1018: Main St, MEMBER (converted to membership fast)
    _client(1018, "Rachel", "Green", "rachel.green@example.com", "305-555-0118",
            34, "1991-05-20", 85.0,
            first_visit_ever="2026-02-08", last_visit="2026-03-15",
            total_visits_ever=3, visits_mar_2026=1,
            lifetime_revenue=234.00, lifetime_tips=28.00, revenue_mar_2026=78.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_member=True, is_returning_in_period=True),

    # 1019: Westside
    _client(1019, "Thomas", "Baker", "thomas.baker@example.com", "305-555-0119",
            41, "1984-09-11", 45.0,
            first_visit_ever="2026-02-12", last_visit="2026-03-10",
            total_visits_ever=2, visits_mar_2026=1,
            lifetime_revenue=184.00, lifetime_tips=20.00, revenue_mar_2026=92.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_returning_in_period=True),

    # 1020: Main St
    _client(1020, "Nicole", "Hall", "nicole.hall@example.com", "305-555-0120",
            28, "1997-03-30", 35.0,
            first_visit_ever="2026-02-18", last_visit="2026-03-22",
            total_visits_ever=2, visits_mar_2026=1,
            lifetime_revenue=156.00, lifetime_tips=20.00, revenue_mar_2026=78.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_returning_in_period=True),

    # 1021: Westside
    _client(1021, "Kevin", "Young", "kevin.young@example.com", "305-555-0121",
            45, "1980-11-14", 40.0,
            first_visit_ever="2026-02-22", last_visit="2026-03-28",
            total_visits_ever=2, visits_mar_2026=1,
            lifetime_revenue=170.00, lifetime_tips=18.00, revenue_mar_2026=92.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_returning_in_period=True),

    # 1022: Main St
    _client(1022, "Amanda", "Scott", "amanda.scott@example.com", "305-555-0122",
            37, "1988-07-02", 50.0,
            first_visit_ever="2026-02-26", last_visit="2026-03-18",
            total_visits_ever=2, visits_mar_2026=1,
            lifetime_revenue=156.00, lifetime_tips=18.00, revenue_mar_2026=78.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_returning_in_period=True),

    # ══ AT-RISK (1023-1027) — last visit Dec 2025 / Jan 2026 ════════════════
    # These are Tom Rivera's former Westside regulars (Manicure/Pedicure)
    # who haven't been back since he left. Days-since > 60 as of Apr 1 2026.
    # 1023: Tom's ex-regular, last visit Jan 15 2026 (76 days)
    _client(1023, "Jennifer", "Lewis", "jennifer.lewis@example.com", "305-555-0123",
            39, "1986-02-14", 180.0,
            first_visit_ever="2024-02-10", last_visit="2026-01-15",
            total_visits_ever=18, visits_mar_2026=0,
            lifetime_revenue=890.00, lifetime_tips=89.00, revenue_mar_2026=0.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_returning_in_period=False),

    # 1024: last visit Dec 20 2025 (102 days)
    _client(1024, "Mark", "Walker", "mark.walker@example.com", "305-555-0124",
            50, "1975-06-25", 150.0,
            first_visit_ever="2023-11-04", last_visit="2025-12-20",
            total_visits_ever=22, visits_mar_2026=0,
            lifetime_revenue=1080.00, lifetime_tips=108.00, revenue_mar_2026=0.00,
            home_loc_id=2, first_visit_loc_id=2),

    # 1025: last visit Jan 5 2026 (86 days)
    _client(1025, "Karen", "Hall", "karen.hall@example.com", "305-555-0125",
            56, "1969-10-18", 130.0,
            first_visit_ever="2024-05-20", last_visit="2026-01-05",
            total_visits_ever=16, visits_mar_2026=0,
            lifetime_revenue=760.00, lifetime_tips=76.00, revenue_mar_2026=0.00,
            home_loc_id=2, first_visit_loc_id=2),

    # 1026: last visit Nov 28 2025 (124 days)
    _client(1026, "Steven", "Allen", "steven.allen@example.com", "305-555-0126",
            43, "1982-12-12", 95.0,
            first_visit_ever="2024-07-08", last_visit="2025-11-28",
            total_visits_ever=12, visits_mar_2026=0,
            lifetime_revenue=540.00, lifetime_tips=54.00, revenue_mar_2026=0.00,
            home_loc_id=2, first_visit_loc_id=2),

    # 1027: last visit Jan 28 2026 (63 days — just over threshold)
    _client(1027, "Laura", "Wright", "laura.wright@example.com", "305-555-0127",
            31, "1994-08-07", 110.0,
            first_visit_ever="2025-03-22", last_visit="2026-01-28",
            total_visits_ever=14, visits_mar_2026=0,
            lifetime_revenue=680.00, lifetime_tips=68.00, revenue_mar_2026=0.00,
            home_loc_id=1, first_visit_loc_id=1),

    # ══ REACTIVATED IN MAR 2026 (1028-1030) — returned after 90+ day gap ═══
    # 1028: first visit 2024, gone Oct 2025 – Mar 2026 (135 day gap), MEMBER
    _client(1028, "Megan", "Adams", "megan.adams@example.com", "305-555-0128",
            35, "1990-01-12", 240.0,
            first_visit_ever="2024-01-15", last_visit="2026-03-14",
            total_visits_ever=16, visits_mar_2026=1,
            lifetime_revenue=1040.00, lifetime_tips=104.00, revenue_mar_2026=78.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_member=True, is_returning_in_period=True, is_reactivated_in_period=True),

    # 1029: first visit 2023, gone Nov 2025 – Mar 2026 (105 day gap)
    _client(1029, "Ryan", "Nelson", "ryan.nelson@example.com", "305-555-0129",
            46, "1979-04-28", 200.0,
            first_visit_ever="2023-08-20", last_visit="2026-03-20",
            total_visits_ever=21, visits_mar_2026=1,
            lifetime_revenue=1280.00, lifetime_tips=128.00, revenue_mar_2026=92.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_returning_in_period=True, is_reactivated_in_period=True),

    # 1030: first visit 2024, gone Nov 2025 – Mar 2026 (120 day gap)
    _client(1030, "Lisa", "Mitchell", "lisa.mitchell@example.com", "305-555-0130",
            40, "1985-09-05", 170.0,
            first_visit_ever="2024-04-12", last_visit="2026-03-27",
            total_visits_ever=14, visits_mar_2026=1,
            lifetime_revenue=880.00, lifetime_tips=88.00, revenue_mar_2026=78.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_returning_in_period=True, is_reactivated_in_period=True),

    # ══ SPECIAL CASES (1031-1035) ═══════════════════════════════════════════
    # 1031: Jane Smith — PII decline test target. Normal active. MEMBER.
    _client(1031, "Jane", "Smith", "jane.smith@example.com", "305-555-0131",
            34, "1991-07-19", 220.0,
            first_visit_ever="2024-11-14", last_visit="2026-03-26",
            total_visits_ever=19, visits_mar_2026=2,
            lifetime_revenue=1120.00, lifetime_tips=112.00, revenue_mar_2026=156.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_member=True, is_returning_in_period=True),

    # 1032: Unsubscribed from email (tbl_custorg.EmailUnsubscribe=1)
    _client(1032, "Peter", "Carter", "peter.carter@example.com", "305-555-0132",
            38, "1987-02-28", 85.0,
            first_visit_ever="2025-05-08", last_visit="2026-03-19",
            total_visits_ever=12, visits_mar_2026=1,
            lifetime_revenue=580.00, lifetime_tips=58.00, revenue_mar_2026=78.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_reachable_email=False,          # ← unsubscribed from email
            is_returning_in_period=True),

    # 1033: Unsubscribed from SMS (tbl_custorg.SMSUnsubscribe=1)
    _client(1033, "Rebecca", "Harris", "rebecca.harris@example.com", "305-555-0133",
            53, "1972-11-22", 70.0,
            first_visit_ever="2025-06-15", last_visit="2026-03-24",
            total_visits_ever=10, visits_mar_2026=1,
            lifetime_revenue=480.00, lifetime_tips=48.00, revenue_mar_2026=92.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_reachable_sms=False,            # ← unsubscribed from SMS
            is_returning_in_period=True),

    # 1034: Soft-deleted globally (tbl_customers.Active=0)
    _client(1034, "Henry", "Evans", "henry.evans@example.com", "305-555-0134",
            62, "1963-06-07", 0.0,
            first_visit_ever="2023-04-10", last_visit="2025-08-22",
            total_visits_ever=8, visits_mar_2026=0,
            lifetime_revenue=390.00, lifetime_tips=39.00, revenue_mar_2026=0.00,
            home_loc_id=2, first_visit_loc_id=2,
            is_not_deleted=False,              # ← soft-deleted
            is_reachable_email=False, is_reachable_sms=False),

    # 1035: Soft-deleted per-tenant (tbl_custorg.Active=0)
    _client(1035, "Olivia", "Turner", "olivia.turner@example.com", "305-555-0135",
            30, "1995-01-15", 0.0,
            first_visit_ever="2024-12-03", last_visit="2025-09-18",
            total_visits_ever=5, visits_mar_2026=0,
            lifetime_revenue=260.00, lifetime_tips=26.00, revenue_mar_2026=0.00,
            home_loc_id=1, first_visit_loc_id=1,
            is_not_deleted=False,              # ← soft-deleted per-tenant
            is_reachable_email=False, is_reachable_sms=False),

    # ══ NEW IN FEB 2026, DID NOT RETURN IN MAR (1036-1038) ══════════════════
    # These exist to make Feb new_clients=8 vs Mar new_clients=5 (a real drop
    # for Q10). Their last visit was in Feb, so as of Apr 1 they're at ~35-45
    # days since — not yet at-risk (threshold 60), but at risk of becoming so.
    # 1036: Main St, one-time visitor
    _client(1036, "Vincent", "Parker", "vincent.parker@example.com", "305-555-0136",
            41, "1984-05-11", 20.0,
            first_visit_ever="2026-02-14", last_visit="2026-02-14",
            total_visits_ever=1, visits_mar_2026=0,
            lifetime_revenue=78.00, lifetime_tips=9.00, revenue_mar_2026=0.00,
            home_loc_id=1, first_visit_loc_id=1),

    # 1037: Westside, one-time visitor
    _client(1037, "Claire", "Roberts", "claire.roberts@example.com", "305-555-0137",
            27, "1998-10-04", 18.0,
            first_visit_ever="2026-02-20", last_visit="2026-02-20",
            total_visits_ever=1, visits_mar_2026=0,
            lifetime_revenue=92.00, lifetime_tips=11.00, revenue_mar_2026=0.00,
            home_loc_id=2, first_visit_loc_id=2),

    # 1038: Main St, one-time visitor
    _client(1038, "Marcus", "Collins", "marcus.collins@example.com", "305-555-0138",
            35, "1990-03-19", 15.0,
            first_visit_ever="2026-02-25", last_visit="2026-02-25",
            total_visits_ever=1, visits_mar_2026=0,
            lifetime_revenue=43.00, lifetime_tips=5.00, revenue_mar_2026=0.00,
            home_loc_id=1, first_visit_loc_id=1),
]


# ─────────────────────────────────────────────────────────────────────────────
# Assign ranks and LTV decile
# ─────────────────────────────────────────────────────────────────────────────

def _assign_ranks(clients):
    """Populate ltv_rank, frequency_rank, points_rank, ltv_percentile_decile."""
    n = len(clients)

    # LTV rank
    by_ltv = sorted(clients, key=lambda c: -c["lifetime_revenue"])
    for i, c in enumerate(by_ltv, start=1):
        c["ltv_rank"] = i
        # NTILE(10): top 10% = decile 1
        c["ltv_percentile_decile"] = min(10, ((i - 1) * 10 // n) + 1)

    # Frequency rank (by visits_in_period)
    by_freq = sorted(clients, key=lambda c: -c["visits_in_period"])
    for i, c in enumerate(by_freq, start=1):
        c["frequency_rank"] = i

    # Points rank
    by_points = sorted(clients, key=lambda c: -c["points"])
    for i, c in enumerate(by_points, start=1):
        c["points_rank"] = i


_assign_ranks(_CLIENTS_42)


# ─────────────────────────────────────────────────────────────────────────────
# Business 99 — 5 clients for tenant isolation testing
# ─────────────────────────────────────────────────────────────────────────────
# These IDs intentionally do NOT overlap with biz 42's 1001-1035.
# None of these should ever appear when business_id=42 is requested.

_CLIENTS_99_RAW = [
    {
        "client_id": 2001, "business_id": 99,
        "first_name": "Tenant99", "last_name": "ClientA",
        "age": 33, "age_bracket": "25_to_40", "points": 50.0,
        "first_visit_ever_date": "2025-11-01", "last_visit_date": "2026-03-15",
        "days_since_last_visit": 17, "total_visits_ever": 8, "visits_in_period": 1,
        "lifetime_revenue": 420.00, "lifetime_tips": 42.00, "lifetime_total_paid": 462.00,
        "revenue_in_period": 85.00, "avg_ticket": 52.50,
        "home_location_id": 10, "home_location_name": "Biz99-LocA",
        "first_visit_location_id": 10, "first_visit_location_name": "Biz99-LocA",
        "is_not_deleted": True, "is_reachable_email": True, "is_reachable_sms": True,
        "is_member": False, "is_new_in_period": False, "is_returning_in_period": True,
        "is_reactivated_in_period": False, "at_risk_flag": False,
        "ltv_rank": 1, "frequency_rank": 1, "points_rank": 1, "ltv_percentile_decile": 1,
    },
    # 4 more minimal clients for biz 99 (just enough to confirm isolation works)
    {"client_id": 2002, "business_id": 99, "first_name": "Tenant99", "last_name": "ClientB",
     "age": 28, "age_bracket": "25_to_40", "points": 20.0,
     "first_visit_ever_date": "2026-01-15", "last_visit_date": "2026-03-01",
     "days_since_last_visit": 31, "total_visits_ever": 3, "visits_in_period": 1,
     "lifetime_revenue": 180.00, "lifetime_tips": 18.00, "lifetime_total_paid": 198.00,
     "revenue_in_period": 60.00, "avg_ticket": 60.00,
     "home_location_id": 10, "home_location_name": "Biz99-LocA",
     "first_visit_location_id": 10, "first_visit_location_name": "Biz99-LocA",
     "is_not_deleted": True, "is_reachable_email": True, "is_reachable_sms": True,
     "is_member": False, "is_new_in_period": False, "is_returning_in_period": True,
     "is_reactivated_in_period": False, "at_risk_flag": False,
     "ltv_rank": 2, "frequency_rank": 2, "points_rank": 2, "ltv_percentile_decile": 4},
    {"client_id": 2003, "business_id": 99, "first_name": "Tenant99", "last_name": "ClientC",
     "age": 45, "age_bracket": "40_to_55", "points": 15.0,
     "first_visit_ever_date": "2025-08-10", "last_visit_date": "2025-11-20",
     "days_since_last_visit": 132, "total_visits_ever": 4, "visits_in_period": 0,
     "lifetime_revenue": 220.00, "lifetime_tips": 22.00, "lifetime_total_paid": 242.00,
     "revenue_in_period": 0.00, "avg_ticket": 55.00,
     "home_location_id": 10, "home_location_name": "Biz99-LocA",
     "first_visit_location_id": 10, "first_visit_location_name": "Biz99-LocA",
     "is_not_deleted": True, "is_reachable_email": True, "is_reachable_sms": True,
     "is_member": False, "is_new_in_period": False, "is_returning_in_period": False,
     "is_reactivated_in_period": False, "at_risk_flag": True,   # >60 days
     "ltv_rank": 3, "frequency_rank": 3, "points_rank": 3, "ltv_percentile_decile": 6},
    {"client_id": 2004, "business_id": 99, "first_name": "Tenant99", "last_name": "ClientD",
     "age": 22, "age_bracket": "under_25", "points": 10.0,
     "first_visit_ever_date": "2026-03-20", "last_visit_date": "2026-03-20",
     "days_since_last_visit": 12, "total_visits_ever": 1, "visits_in_period": 1,
     "lifetime_revenue": 45.00, "lifetime_tips": 5.00, "lifetime_total_paid": 50.00,
     "revenue_in_period": 45.00, "avg_ticket": 45.00,
     "home_location_id": 10, "home_location_name": "Biz99-LocA",
     "first_visit_location_id": 10, "first_visit_location_name": "Biz99-LocA",
     "is_not_deleted": True, "is_reachable_email": True, "is_reachable_sms": True,
     "is_member": False, "is_new_in_period": True, "is_returning_in_period": False,
     "is_reactivated_in_period": False, "at_risk_flag": False,
     "ltv_rank": 5, "frequency_rank": 4, "points_rank": 4, "ltv_percentile_decile": 10},
    {"client_id": 2005, "business_id": 99, "first_name": "Tenant99", "last_name": "ClientE",
     "age": 51, "age_bracket": "40_to_55", "points": 30.0,
     "first_visit_ever_date": "2025-05-05", "last_visit_date": "2026-03-10",
     "days_since_last_visit": 22, "total_visits_ever": 6, "visits_in_period": 1,
     "lifetime_revenue": 340.00, "lifetime_tips": 34.00, "lifetime_total_paid": 374.00,
     "revenue_in_period": 72.00, "avg_ticket": 56.67,
     "home_location_id": 10, "home_location_name": "Biz99-LocA",
     "first_visit_location_id": 10, "first_visit_location_name": "Biz99-LocA",
     "is_not_deleted": True, "is_reachable_email": True, "is_reachable_sms": True,
     "is_member": True, "is_new_in_period": False, "is_returning_in_period": True,
     "is_reactivated_in_period": False, "at_risk_flag": False,
     "ltv_rank": 4, "frequency_rank": 5, "points_rank": 5, "ltv_percentile_decile": 8},
]


# ─────────────────────────────────────────────────────────────────────────────
# EP1 — Retention Snapshot fixtures
# ─────────────────────────────────────────────────────────────────────────────

RETENTION_SNAPSHOT = {
    "business_id":          42,
    "period_start":         "2026-03-01",
    "period_end":           "2026-03-31",
    "ref_date":             "2026-04-01",
    "churn_threshold_days": 60,
    "generated_at":         "2026-04-19T10:00:00Z",
    "total_count":          len(_CLIENTS_42),
    "returned_count":       len(_CLIENTS_42),
    "data":                 _CLIENTS_42,
}

RETENTION_SNAPSHOT_99 = {
    "business_id":          99,
    "period_start":         "2026-03-01",
    "period_end":           "2026-03-31",
    "ref_date":             "2026-04-01",
    "churn_threshold_days": 60,
    "generated_at":         "2026-04-19T10:00:00Z",
    "total_count":          len(_CLIENTS_99_RAW),
    "returned_count":       len(_CLIENTS_99_RAW),
    "data":                 _CLIENTS_99_RAW,
}


# ─────────────────────────────────────────────────────────────────────────────
# EP2 — Cohort Monthly fixtures
# ─────────────────────────────────────────────────────────────────────────────
# Counts derived from _CLIENTS_42:
#
# March 2026:
#   clients_total          = 35
#   new_clients            = 5  (1013-1017)
#   returning_clients      = 18 (all with is_returning_in_period=True — 1001-1012,
#                                 1018-1022 minus soft-deletes) — recount below
#   reactivated_clients    = 3  (1028, 1029, 1030)
#   active_clients_in_period = 23 (visits_in_period >= 1)
#   at_risk_clients        = 5  (1023-1027 with days_since > 60 AND is_not_deleted)
#   active_members         = 5  (members with visits_in_period >= 1)
#   reachable_email        = 30 (is_not_deleted AND email reachable — excludes
#                                 1032 unsubscribed, 1034 deleted, 1035 deleted)
#   total_revenue_in_period = sum of revenue_mar_2026 across all clients
#   unique_visitors_in_period = 23
#
# February 2026 (synthetic — matches app fixtures' weaker month):
#   new_clients = 5 (1018-1022 first visited in Feb)
#
# January 2026 (synthetic):
#   new_clients = 3 (imagine prior cohort, not enumerated here)

def _compute_mar_2026_aggregates():
    clients = _CLIENTS_42
    total_revenue_in_period = sum(c["revenue_in_period"] for c in clients)
    new_count = sum(1 for c in clients if c["is_new_in_period"])
    returning_count = sum(1 for c in clients if c["is_returning_in_period"])
    reactivated_count = sum(1 for c in clients if c["is_reactivated_in_period"])
    active_count = sum(1 for c in clients if c["visits_in_period"] >= 1)
    at_risk_count = sum(1 for c in clients if c["at_risk_flag"])
    active_members = sum(1 for c in clients
                         if c["is_member"] and c["visits_in_period"] >= 1)
    reachable_email = sum(1 for c in clients
                          if c["is_reachable_email"] and c["is_not_deleted"])
    reachable_sms = sum(1 for c in clients
                        if c["is_reachable_sms"] and c["is_not_deleted"])
    unique_visitors = active_count

    # top 10% revenue share: top 10% by lifetime_revenue = top 4 of 35
    top10 = sorted(clients, key=lambda c: -c["lifetime_revenue"])[:4]
    top10_rev = sum(c["revenue_in_period"] for c in top10)
    top10pct_share = round(top10_rev / total_revenue_in_period * 100, 2) if total_revenue_in_period else None

    return {
        "clients_total":            len(clients),
        "new_clients":              new_count,
        "returning_clients":        returning_count,
        "reactivated_clients":      reactivated_count,
        "active_clients_in_period": active_count,
        "at_risk_clients":          at_risk_count,
        "active_members":           active_members,
        "reachable_email":          reachable_email,
        "reachable_sms":            reachable_sms,
        "total_revenue_in_period":  round(total_revenue_in_period, 2),
        "unique_visitors_in_period": unique_visitors,
        "top10pct_revenue_share":   top10pct_share,
    }


_mar_agg = _compute_mar_2026_aggregates()

# Story-based prior-month values:
# Feb 2026 was weak (per appointments fixtures).
# Jan 2026 ≈ moderate.
_jan_2026 = {
    "clients_total":            38,
    "new_clients":              3,
    "returning_clients":        16,
    "reactivated_clients":      1,
    "active_clients_in_period": 19,
    "at_risk_clients":          3,
    "active_members":           4,
    "reachable_email":          33,
    "reachable_sms":            34,
    "total_revenue_in_period":  1820.00,
    "unique_visitors_in_period": 19,
    "top10pct_revenue_share":   38.5,
}

_feb_2026 = {
    "clients_total":            38,
    "new_clients":              8,       # 5 returners (1018-1022) + 3 non-returners (1036-1038)
    "returning_clients":        15,
    "reactivated_clients":      0,       # no reactivations in weak month
    "active_clients_in_period": 23,      # 15 returners + 8 new
    "at_risk_clients":          4,
    "active_members":           4,
    "reachable_email":          33,
    "reachable_sms":            34,
    "total_revenue_in_period":  2240.00, # weak per-visit but more visits
    "unique_visitors_in_period": 23,
    "top10pct_revenue_share":   39.0,
}

_mar_2026 = {**_mar_agg}  # already computed


def _build_monthly_row(period, curr, prev=None):
    """Build a cohort-monthly row with MoM and retention calcs."""
    def mom_pct(c, p):
        if p in (None, 0): return None
        return round((c - p) / p * 100, 2)

    new_vs_returning = None
    tot = curr["new_clients"] + curr["returning_clients"]
    if tot > 0:
        new_vs_returning = round(curr["new_clients"] / tot * 100, 2)

    churn_rate_pct = round(
        curr["at_risk_clients"] / curr["clients_total"] * 100, 2
    ) if curr["clients_total"] else None

    member_overlap_pct = None
    if curr["active_clients_in_period"] > 0:
        member_overlap_pct = round(
            curr["active_members"] / curr["active_clients_in_period"] * 100, 2
        )

    # Cohort retention (Option A): prev period's actives who came back
    # For synthetic months we hardcode; for Mar we derive from client data
    if period == "2026-03":
        # Of the 23 Feb actives (15 returners + 8 new), how many came back in Mar?
        # The 5 new-Feb returners (1018-1022) did come back = 5
        # Of the 15 other Feb actives (subset of top LTV + normal), ~12 returned
        # The 3 non-returners (1036-1038) did NOT return
        # Total Mar-returners from Feb cohort: ~17 of 23 = 73.9%
        retention_rate_pct = round(17 / 23 * 100, 2)  # = 73.91
    elif period == "2026-02":
        # 16 of 19 Jan actives returned in Feb
        retention_rate_pct = round(16 / 19 * 100, 2)  # = 84.21
    else:
        retention_rate_pct = None

    return {
        "business_id":               42,
        "period":                    period + "-01",
        **curr,
        "prev_new_clients":          prev["new_clients"] if prev else None,
        "prev_at_risk_clients":      prev["at_risk_clients"] if prev else None,
        "prev_active_clients":       prev["active_clients_in_period"] if prev else None,
        "new_clients_mom_pct":       mom_pct(curr["new_clients"], prev["new_clients"]) if prev else None,
        "at_risk_mom_pct":           mom_pct(curr["at_risk_clients"], prev["at_risk_clients"]) if prev else None,
        "new_vs_returning_split":    new_vs_returning,
        "retention_rate_pct":        retention_rate_pct,
        "churn_rate_pct":            churn_rate_pct,
        "member_overlap_pct":        member_overlap_pct,
    }


COHORT_MONTHLY = {
    "business_id":          42,
    "start_month":          "2026-01-01",
    "end_month":            "2026-03-01",
    "ref_date":             "2026-04-01",
    "churn_threshold_days": 60,
    "generated_at":         "2026-04-19T10:00:00Z",
    "data": [
        # Ordered period DESC (most recent first)
        _build_monthly_row("2026-03", _mar_2026, _feb_2026),
        _build_monthly_row("2026-02", _feb_2026, _jan_2026),
        _build_monthly_row("2026-01", _jan_2026, None),
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# EP3 — Per-Location Monthly fixtures
# ─────────────────────────────────────────────────────────────────────────────
# Main St wins new-client count in Mar 2026 (matches WELCOME10 pattern).
# Counts derived from _CLIENTS_42 grouped by first_visit_location_id:
#   Mar 2026 new clients:
#     Main St (loc 1): 1013, 1014, 1016 = 3
#     Westside (loc 2): 1015, 1017 = 2

def _per_loc_mar_2026():
    new_by_loc = {}
    homed_by_loc = {}
    active_by_loc = {}
    rev_by_loc = {}

    for c in _CLIENTS_42:
        hloc = c["home_location_id"]
        fvloc = c["first_visit_location_id"]
        if fvloc is None:
            continue
        if c["is_new_in_period"]:
            new_by_loc[fvloc] = new_by_loc.get(fvloc, 0) + 1
        if hloc is not None:
            homed_by_loc[hloc] = homed_by_loc.get(hloc, 0) + 1
            if c["visits_in_period"] >= 1:
                active_by_loc[hloc] = active_by_loc.get(hloc, 0) + 1
                rev_by_loc[hloc] = rev_by_loc.get(hloc, 0) + c["revenue_in_period"]

    rows = []
    for loc_id, loc_name in [(1, "Main St"), (2, "Westside")]:
        rows.append({
            "location_id":        loc_id,
            "location_name":      loc_name,
            "new_clients_here":   new_by_loc.get(loc_id, 0),
            "clients_homed_here": homed_by_loc.get(loc_id, 0),
            "active_clients_here": active_by_loc.get(loc_id, 0),
            "revenue_here":       round(rev_by_loc.get(loc_id, 0), 2),
        })

    # Assign ranks
    sorted_by_new = sorted(rows, key=lambda r: -r["new_clients_here"])
    for i, r in enumerate(sorted_by_new, start=1):
        r["rank_by_new_clients"] = i
    sorted_by_active = sorted(rows, key=lambda r: -r["active_clients_here"])
    for i, r in enumerate(sorted_by_active, start=1):
        r["rank_by_active_clients"] = i

    # Re-sort by rank_by_new
    rows.sort(key=lambda r: r["rank_by_new_clients"])
    # Add common fields
    for r in rows:
        r["business_id"] = 42
        r["period"] = "2026-03-01"
    return rows


PER_LOCATION_MONTHLY = {
    "business_id":    42,
    "start_month":    "2026-03-01",
    "end_month":      "2026-03-01",
    "generated_at":   "2026-04-19T10:00:00Z",
    "data":           _per_loc_mar_2026(),
}


# ─────────────────────────────────────────────────────────────────────────────
# Fixture lookup — endpoint path → response
# ─────────────────────────────────────────────────────────────────────────────

FIXTURES: dict[str, dict] = {
    "/api/v1/leo/clients/retention-snapshot":      RETENTION_SNAPSHOT,
    "/api/v1/leo/clients/cohort-monthly":           COHORT_MONTHLY,
    "/api/v1/leo/clients/per-location-monthly":     PER_LOCATION_MONTHLY,
}

# For biz 99 isolation tests — not part of FIXTURES dict
# (mock server patches business_id into response based on request body, so the
# FIXTURES dict is keyed per path and we return a copy with business_id swapped.
# Isolation is enforced by AUTHORISED_BUSINESS_IDS in the mock server.)

# Exported for visibility
__all__ = [
    "RETENTION_SNAPSHOT",
    "RETENTION_SNAPSHOT_99",
    "COHORT_MONTHLY",
    "PER_LOCATION_MONTHLY",
    "FIXTURES",
]
