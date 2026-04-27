"""
tests/fixtures/forms_fixtures.py
=================================
Locked fixture for Sprint 10 (Forms domain).

Business: 42 (matches gift cards / revenue / clients fixtures for cross-domain S1)
Snapshot date: 2026-03-31
Window for monthly queries: 2025-01-01 → 2026-03-31

DESIGN NOTES
============
Schema is thin: tbl_form (templates) + tbl_formcust (submissions). No service /
employee / location linkage available (FF2-FF4). Volumes are intentionally small
to mirror real production data (~85 templates / ~72 submissions across all orgs
in dev DB).

This fixture has 4 templates and 18 submissions — enough cardinality to make
F7 (most-submitted) and F8 (dormant) meaningfully testable, while staying
realistic for the per-tenant size.

ANCHOR NUMBERS (snapshot 2026-03-31)
=====================================
Template catalog (FQ1):
  - 4 templates total (3 active + 1 inactive)
  - 1 active dormant (form 4 — never submitted)
  - 0 inactive dormant (form 3 had submissions before deactivation)

Submissions by month (FQ2):
  - Mar 2026: 5 submissions (3 complete + 1 approved + 1 ready)
  - Feb 2026: 4 submissions (2 complete + 2 ready)
  - Jan 2026: 3 submissions (2 complete + 1 ready)
  - Dec 2025: 1 submission (complete)
  - Nov 2025: 2 submissions (1 approved + 1 ready)
  - Oct 2025: 2 submissions (1 complete + 1 approved)
  - Sep 2025: 1 submission (complete)
  Total in window: 18 submissions across 7 months
  Mar 2026 vs Feb 2026: +25% MoM (5 vs 4)

Per-form ranking (FQ3):
  - Form 1 "Intake Questionnaire" — 8 submissions (rank 1)
  - Form 2 "Post-Visit Feedback" — 6 submissions (rank 2)
  - Form 3 "Pre-Treatment Consent" — 4 submissions (rank 3) — INACTIVE
  - Form 4 "New Customer Welcome" — 0 submissions (rank 4) — DORMANT-ACTIVE

Lifecycle (FQ4):
  - Total: 18 submissions
  - Ready: 5 / Complete: 10 / Approved: 3 / Unknown: 0
  - Completion rate: (10+3)/18 = 72.22%
  - Stuck-at-ready (older than 7 days from snapshot 2026-03-31, i.e. RecDate < 2026-03-24): 4 of 5 ready
"""

from datetime import date, datetime, timedelta
from decimal import Decimal


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

BUSINESS_ID    = 42
SNAPSHOT_DATE  = date(2026, 3, 31)
STUCK_THRESHOLD = SNAPSHOT_DATE - timedelta(days=7)  # 2026-03-24


# ─────────────────────────────────────────────────────────────────────────────
# Templates — tbl_form
# ─────────────────────────────────────────────────────────────────────────────

TEMPLATES = [
    {
        "Id":          1,
        "Name":        "Intake Questionnaire",
        "Description": "Standard new-client health and history form",
        "OrgId":       BUSINESS_ID,
        "Active":      1,
        "CategoryId":  1,
        "RecDate":     datetime(2024, 6, 15, 10, 0),
    },
    {
        "Id":          2,
        "Name":        "Post-Visit Feedback",
        "Description": "Quick rating + comments after each appointment",
        "OrgId":       BUSINESS_ID,
        "Active":      1,
        "CategoryId":  1,
        "RecDate":     datetime(2024, 8, 1, 14, 30),
    },
    {
        "Id":          3,
        "Name":        "Pre-Treatment Consent",
        "Description": "Legacy consent form, replaced by new flow Mar 2026",
        "OrgId":       BUSINESS_ID,
        "Active":      0,           # deactivated
        "CategoryId":  2,
        "RecDate":     datetime(2024, 3, 10, 9, 0),
    },
    {
        "Id":          4,
        "Name":        "New Customer Welcome",
        "Description": "Welcome email + intro form — never used in production",
        "OrgId":       BUSINESS_ID,
        "Active":      1,           # active but DORMANT (no submissions)
        "CategoryId":  1,
        "RecDate":     datetime(2025, 11, 20, 16, 0),
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Submissions — tbl_formcust
# ─────────────────────────────────────────────────────────────────────────────
# Distribution:
#   Form 1 (Intake): 8 subs across 7 months — flagship form
#   Form 2 (Feedback): 6 subs across 5 months — second-most used
#   Form 3 (Consent, INACTIVE): 4 subs in older months — fell off after deactivation
#   Form 4 (Welcome, ACTIVE-DORMANT): 0 subs

SUBMISSIONS = [
    # ── March 2026 (5 subs) ─────────────────────────────────────────────────
    {"Id": 101, "FormId": 1, "CustId": 501, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "complete", "RecDate": datetime(2026, 3, 28, 11, 15)},
    {"Id": 102, "FormId": 1, "CustId": 502, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "approved", "RecDate": datetime(2026, 3, 26, 9, 45)},
    {"Id": 103, "FormId": 2, "CustId": 503, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "complete", "RecDate": datetime(2026, 3, 22, 16, 0)},  # 9 days old → stuck if ready (it's complete)
    {"Id": 104, "FormId": 2, "CustId": 504, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "ready",    "RecDate": datetime(2026, 3, 30, 10, 0)},  # 1 day → NOT stuck
    {"Id": 105, "FormId": 1, "CustId": 505, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "complete", "RecDate": datetime(2026, 3, 15, 14, 20)},

    # ── February 2026 (4 subs) ──────────────────────────────────────────────
    {"Id": 106, "FormId": 1, "CustId": 506, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "complete", "RecDate": datetime(2026, 2, 18, 11, 0)},
    {"Id": 107, "FormId": 2, "CustId": 507, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "ready",    "RecDate": datetime(2026, 2, 12, 13, 30)},  # STUCK: >7 days from snapshot
    {"Id": 108, "FormId": 1, "CustId": 508, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "complete", "RecDate": datetime(2026, 2, 8, 15, 45)},
    {"Id": 109, "FormId": 2, "CustId": 509, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "ready",    "RecDate": datetime(2026, 2, 5, 10, 0)},   # STUCK

    # ── January 2026 (3 subs) ───────────────────────────────────────────────
    {"Id": 110, "FormId": 1, "CustId": 510, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "complete", "RecDate": datetime(2026, 1, 25, 9, 30)},
    {"Id": 111, "FormId": 2, "CustId": 511, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "complete", "RecDate": datetime(2026, 1, 20, 14, 0)},
    {"Id": 112, "FormId": 1, "CustId": 512, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "ready",    "RecDate": datetime(2026, 1, 14, 11, 45)},  # STUCK

    # ── December 2025 (1 sub) ───────────────────────────────────────────────
    {"Id": 113, "FormId": 1, "CustId": 513, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "complete", "RecDate": datetime(2025, 12, 18, 10, 0)},

    # ── November 2025 (2 subs) ──────────────────────────────────────────────
    {"Id": 114, "FormId": 2, "CustId": 514, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "approved", "RecDate": datetime(2025, 11, 25, 13, 15)},
    {"Id": 115, "FormId": 3, "CustId": 515, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "ready",    "RecDate": datetime(2025, 11, 12, 16, 0)},  # STUCK (form 3 since deactivated)

    # ── October 2025 (2 subs) ───────────────────────────────────────────────
    {"Id": 116, "FormId": 3, "CustId": 516, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "complete", "RecDate": datetime(2025, 10, 22, 12, 0)},
    {"Id": 117, "FormId": 3, "CustId": 517, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "approved", "RecDate": datetime(2025, 10, 8, 10, 30)},

    # ── September 2025 (1 sub) ──────────────────────────────────────────────
    {"Id": 118, "FormId": 3, "CustId": 518, "Orgid": BUSINESS_ID, "Active": 1,
        "Status": "complete", "RecDate": datetime(2025, 9, 14, 14, 45)},
]


# ─────────────────────────────────────────────────────────────────────────────
# Pre-computed warehouse-row anchors
# ─────────────────────────────────────────────────────────────────────────────
# These are what the API endpoints (Step 4) and warehouse rows (Step 4) MUST
# return for the fixture above. Step 6 acceptance tests will assert against
# these via natural-language answers.

ANCHORS = {

    # FQ1 — Catalog snapshot
    "catalog": {
        "snapshot_date":               SNAPSHOT_DATE,
        "total_template_count":         4,
        "active_template_count":        3,    # forms 1, 2, 4
        "inactive_template_count":      1,    # form 3
        "active_dormant_count":         1,    # form 4 (active but never submitted)
        "inactive_dormant_count":       0,    # form 3 was used 4x before deactivation
        "lifetime_submission_total":   18,
        "recent_90d_submission_total": 13,    # Jan-Mar 2026 = 12 + Dec 2025 partial; recompute below
        "most_recent_template_added":   datetime(2025, 11, 20, 16, 0),
        "distinct_category_ids":        [1, 2],
    },

    # FQ2 — Monthly summary (DESC by period_start)
    "monthly": [
        # period_start, sub_count, ready, complete, approved, distinct_forms, distinct_custs, mom%, yoy%
        {"period_start": date(2026, 3, 1), "submission_count": 5, "ready_count": 1, "complete_count": 3, "approved_count": 1,
            "distinct_forms_used": 2, "distinct_customers_filling": 5, "mom_submission_pct": 25.00, "yoy_submission_pct": None},
        {"period_start": date(2026, 2, 1), "submission_count": 4, "ready_count": 2, "complete_count": 2, "approved_count": 0,
            "distinct_forms_used": 2, "distinct_customers_filling": 4, "mom_submission_pct": 33.33, "yoy_submission_pct": None},
        {"period_start": date(2026, 1, 1), "submission_count": 3, "ready_count": 1, "complete_count": 2, "approved_count": 0,
            "distinct_forms_used": 2, "distinct_customers_filling": 3, "mom_submission_pct": 200.00, "yoy_submission_pct": None},
        {"period_start": date(2025, 12, 1), "submission_count": 1, "ready_count": 0, "complete_count": 1, "approved_count": 0,
            "distinct_forms_used": 1, "distinct_customers_filling": 1, "mom_submission_pct": -50.00, "yoy_submission_pct": None},
        {"period_start": date(2025, 11, 1), "submission_count": 2, "ready_count": 1, "complete_count": 0, "approved_count": 1,
            "distinct_forms_used": 2, "distinct_customers_filling": 2, "mom_submission_pct": 0.00, "yoy_submission_pct": None},
        {"period_start": date(2025, 10, 1), "submission_count": 2, "ready_count": 0, "complete_count": 1, "approved_count": 1,
            "distinct_forms_used": 1, "distinct_customers_filling": 2, "mom_submission_pct": 100.00, "yoy_submission_pct": None},
        {"period_start": date(2025, 9, 1),  "submission_count": 1, "ready_count": 0, "complete_count": 1, "approved_count": 0,
            "distinct_forms_used": 1, "distinct_customers_filling": 1, "mom_submission_pct": None, "yoy_submission_pct": None},
    ],

    # FQ3 — Per-form snapshot (rank by lifetime submissions)
    "per_form": [
        {
            "snapshot_date": SNAPSHOT_DATE,
            "form_id": 1, "form_name": "Intake Questionnaire",
            "form_description": "Standard new-client health and history form",
            "is_active": True, "category_id": 1,
            "template_created_at": datetime(2024, 6, 15, 10, 0),
            "lifetime_submission_count": 8,
            "complete_count": 6, "approved_count": 1, "ready_count": 1,
            "submissions_last_30d": 3,    # Mar 2026 only: 101, 102, 105
            "submissions_last_90d": 7,    # Jan-Mar 2026: 101, 102, 105, 106, 108, 110, 112
            "most_recent_submission_at": datetime(2026, 3, 28, 11, 15),
            "distinct_customers": 8,
            "is_dormant": False, "is_active_dormant": False,
            "completion_rate_pct": 87.50,  # (6+1)/8 = 87.5%
            "rank_by_submissions": 1,
        },
        {
            "snapshot_date": SNAPSHOT_DATE,
            "form_id": 2, "form_name": "Post-Visit Feedback",
            "form_description": "Quick rating + comments after each appointment",
            "is_active": True, "category_id": 1,
            "template_created_at": datetime(2024, 8, 1, 14, 30),
            "lifetime_submission_count": 6,
            "complete_count": 2, "approved_count": 1, "ready_count": 3,
            "submissions_last_30d": 2,    # Mar 2026: 103, 104
            "submissions_last_90d": 5,    # Jan-Mar 2026: 103, 104, 107, 109, 111
            "most_recent_submission_at": datetime(2026, 3, 30, 10, 0),
            "distinct_customers": 6,
            "is_dormant": False, "is_active_dormant": False,
            "completion_rate_pct": 50.00,  # (2+1)/6 = 50%
            "rank_by_submissions": 2,
        },
        {
            "snapshot_date": SNAPSHOT_DATE,
            "form_id": 3, "form_name": "Pre-Treatment Consent",
            "form_description": "Legacy consent form, replaced by new flow Mar 2026",
            "is_active": False, "category_id": 2,
            "template_created_at": datetime(2024, 3, 10, 9, 0),
            "lifetime_submission_count": 4,
            "complete_count": 2, "approved_count": 1, "ready_count": 1,
            "submissions_last_30d": 0,
            "submissions_last_90d": 0,
            "most_recent_submission_at": datetime(2025, 11, 12, 16, 0),
            "distinct_customers": 4,
            "is_dormant": False, "is_active_dormant": False,
            "completion_rate_pct": 75.00,  # (2+1)/4 = 75%
            "rank_by_submissions": 3,
        },
        {
            "snapshot_date": SNAPSHOT_DATE,
            "form_id": 4, "form_name": "New Customer Welcome",
            "form_description": "Welcome email + intro form — never used in production",
            "is_active": True, "category_id": 1,
            "template_created_at": datetime(2025, 11, 20, 16, 0),
            "lifetime_submission_count": 0,
            "complete_count": 0, "approved_count": 0, "ready_count": 0,
            "submissions_last_30d": 0,
            "submissions_last_90d": 0,
            "most_recent_submission_at": None,
            "distinct_customers": 0,
            "is_dormant": True, "is_active_dormant": True,    # F11 actionable
            "completion_rate_pct": None,                       # divide-by-zero protected
            "rank_by_submissions": 4,
        },
    ],

    # FQ4 — Lifecycle snapshot (always-emit)
    "lifecycle": {
        "snapshot_date":             SNAPSHOT_DATE,
        "total_submissions":         18,
        "ready_count":                5,    # 104, 107, 109, 112, 115
        "complete_count":            10,
        "approved_count":             3,
        "unknown_status_count":       0,
        "completion_rate_pct":       72.22,  # (10+3)/18 = 72.22%
        # Stuck-at-ready: RecDate < 2026-03-24 (snapshot - 7d) AND status='ready'
        # Submissions with status='ready': 104, 107, 109, 112, 115
        # Of those, RecDate < 2026-03-24:
        #   104: 2026-03-30 — NOT stuck (1 day old)
        #   107: 2026-02-12 — STUCK
        #   109: 2026-02-05 — STUCK
        #   112: 2026-01-14 — STUCK
        #   115: 2025-11-12 — STUCK
        # → 4 stuck submissions
        "stuck_ready_count":          4,
        "stuck_ready_submission_ids": [115, 112, 109, 107],   # ORDER BY RecDate ASC
        "most_recent_submission_at":  datetime(2026, 3, 30, 10, 0),
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# _validate_anchors — runs at import time so a bad fixture fails loud + early
# ─────────────────────────────────────────────────────────────────────────────

def _validate_anchors():
    """Re-derive anchors from raw TEMPLATES + SUBMISSIONS and assert they
    match the precomputed ANCHORS block. Catches typos in either direction."""

    # Total submissions
    total_subs = len(SUBMISSIONS)
    assert total_subs == 18, f"Expected 18 submissions, got {total_subs}"
    assert ANCHORS["lifecycle"]["total_submissions"] == 18

    # Templates: 4 total, 3 active, 1 inactive
    active = sum(1 for t in TEMPLATES if t["Active"] == 1)
    inactive = sum(1 for t in TEMPLATES if t["Active"] == 0)
    assert active == 3, f"Expected 3 active templates, got {active}"
    assert inactive == 1, f"Expected 1 inactive template, got {inactive}"

    # Status counts
    ready    = sum(1 for s in SUBMISSIONS if s["Status"] == "ready")
    complete = sum(1 for s in SUBMISSIONS if s["Status"] == "complete")
    approved = sum(1 for s in SUBMISSIONS if s["Status"] == "approved")
    assert ready == 5, f"Expected 5 ready, got {ready}"
    assert complete == 10, f"Expected 10 complete, got {complete}"
    assert approved == 3, f"Expected 3 approved, got {approved}"
    assert ready + complete + approved == total_subs

    # Per-form lifetime counts
    from collections import Counter
    per_form_counts = Counter(s["FormId"] for s in SUBMISSIONS)
    assert per_form_counts[1] == 8, f"Form 1 expected 8 subs, got {per_form_counts[1]}"
    assert per_form_counts[2] == 6, f"Form 2 expected 6 subs, got {per_form_counts[2]}"
    assert per_form_counts[3] == 4, f"Form 3 expected 4 subs, got {per_form_counts[3]}"
    assert per_form_counts.get(4, 0) == 0, "Form 4 must be dormant"

    # Stuck-at-ready: ready AND RecDate < snapshot - 7d
    stuck = [s for s in SUBMISSIONS
             if s["Status"] == "ready" and s["RecDate"].date() < STUCK_THRESHOLD]
    assert len(stuck) == 4, f"Expected 4 stuck-at-ready, got {len(stuck)}"
    stuck_ids_chronological = sorted([s["Id"] for s in stuck],
                                      key=lambda i: next(s["RecDate"] for s in SUBMISSIONS if s["Id"] == i))
    expected = ANCHORS["lifecycle"]["stuck_ready_submission_ids"]
    assert stuck_ids_chronological == expected, \
        f"Stuck-ready id ordering mismatch: derived={stuck_ids_chronological} expected={expected}"

    # Completion rate (FQ4)
    rate = round((complete + approved) / total_subs * 100, 2)
    assert rate == 72.22, f"Completion rate expected 72.22, got {rate}"

    # Per-form rank ordering (FQ3)
    ranked = sorted(per_form_counts.items(), key=lambda x: -x[1])
    assert ranked[0] == (1, 8), "Form 1 must rank #1"
    assert ranked[1] == (2, 6), "Form 2 must rank #2"
    assert ranked[2] == (3, 4), "Form 3 must rank #3"

    # Form 2 completion rate (R10 small-sample test): (2 complete + 1 approved) / 6 = 50%
    f2_complete = sum(1 for s in SUBMISSIONS if s["FormId"] == 2 and s["Status"] == "complete")
    f2_approved = sum(1 for s in SUBMISSIONS if s["FormId"] == 2 and s["Status"] == "approved")
    assert round((f2_complete + f2_approved) / 6 * 100, 2) == 50.00

    # Form 1 completion rate: (6 complete + 1 approved) / 8 = 87.5%
    f1_complete = sum(1 for s in SUBMISSIONS if s["FormId"] == 1 and s["Status"] == "complete")
    f1_approved = sum(1 for s in SUBMISSIONS if s["FormId"] == 1 and s["Status"] == "approved")
    assert round((f1_complete + f1_approved) / 8 * 100, 2) == 87.50, \
        f"Form 1 expected 87.5% completion, got {(f1_complete + f1_approved) / 8 * 100}"

    # Recent 90d (catalog) — submissions with RecDate >= snapshot - 90d (= 2026-01-01)
    cutoff_90d = SNAPSHOT_DATE - timedelta(days=90)
    recent_90d = sum(1 for s in SUBMISSIONS if s["RecDate"].date() >= cutoff_90d)
    # Catalog anchor will be updated below to match what the SQL would return
    assert recent_90d == 12, f"Expected 12 subs in last 90d (since {cutoff_90d}), got {recent_90d}"
    # Patch ANCHORS catalog to derived value
    ANCHORS["catalog"]["recent_90d_submission_total"] = 12


_validate_anchors()


# ─────────────────────────────────────────────────────────────────────────────
# Public exports
# ─────────────────────────────────────────────────────────────────────────────

__all__ = [
    "BUSINESS_ID",
    "SNAPSHOT_DATE",
    "STUCK_THRESHOLD",
    "TEMPLATES",
    "SUBMISSIONS",
    "ANCHORS",
]


if __name__ == "__main__":
    print(f"✓ Forms fixture validated for biz {BUSINESS_ID}")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Templates: {len(TEMPLATES)} ({sum(1 for t in TEMPLATES if t['Active']==1)} active)")
    print(f"  Submissions: {len(SUBMISSIONS)}")
    print(f"  Anchors: catalog={len(ANCHORS['catalog'])} fields, "
          f"monthly={len(ANCHORS['monthly'])} rows, "
          f"per_form={len(ANCHORS['per_form'])} rows, "
          f"lifecycle={len(ANCHORS['lifecycle'])} fields")
    print()
    print("Top anchors:")
    print(f"  - 4 templates (3 active, 1 inactive)")
    print(f"  - 18 lifetime submissions")
    print(f"  - Form 1 (Intake) ranks #1 with 8 subs (87.5% completion)")
    print(f"  - Form 4 (Welcome) is active-dormant — F11 actionable target")
    print(f"  - Completion rate: 72.22% org-wide")
    print(f"  - 4 stuck-at-ready submissions older than 7 days from snapshot")