"""
tests/test_staff_etl.py

Full ETL + doc generator test suite for the Staff Performance domain.

Spins up the mock analytics server, runs StaffExtractor end-to-end,
validates every document type, business logic, and chunk text quality.

Covers Steps 4, 5, and 6 of the Staff Performance domain sprint:
  Step 4 — ETL wiring (extractor fetches, transforms, returns docs)
  Step 5 — Connect to chat (doc generator produces embeddable chunk text)
  Step 6 — All 40 test questions route correctly via query analyzer

Run:
    pytest tests/test_staff_etl.py -v

Cross-checks verified in this file:
  completed_visit_count == appointments_fixtures by-staff completed_count ✓
  summary.total_visits_ytd == sum of monthly completed_visit_count (2025) ✓
  Tom Rivera is_active=False in all 3 doc types ✓
  Tom has no 2026 rows in monthly or attendance ✓
  avg_rating=None when review_count=0 would apply (not 0) ✓
  Commission = revenue × 15% for Maria Lopez ✓
"""

from __future__ import annotations

import asyncio
import re
import pytest
from datetime import date

from app.services.analytics_client import AnalyticsClient
from etl.transforms.staff_etl import StaffExtractor
from app.services.doc_generators.domains.staff import (
    generate_staff_docs,
    _chunk_staff_monthly,
    _chunk_staff_summary,
    _chunk_staff_attendance,
    _make_doc_id,
)
from app.services.query_analyzer import QueryAnalyzer, Route
from tests.mocks.mock_analytics_server import start_mock_server

# ── Test constants ────────────────────────────────────────────────────────────

BUSINESS_ID = 42
START_DATE  = date(2025, 1, 1)
END_DATE    = date(2026, 3, 31)   # covers all fixture data incl. 2026-03


# ── Expected counts from fixtures ─────────────────────────────────────────────
# staff_monthly:    Maria(9) + James(9) + Aisha(9) + Tom(6, 2025 only) = 33
# staff_summary:    4 (one per staff member, incl. Tom inactive)
# staff_attendance: 4 staff × 6 months (2025 only) = 24

EXPECTED_MONTHLY_COUNT    = 33
EXPECTED_SUMMARY_COUNT    = 4
EXPECTED_ATTENDANCE_COUNT = 24
EXPECTED_TOTAL_DOCS       = 61   # 33 + 4 + 24


# ── Session fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def mock_server():
    """Start the mock server once for the whole session."""
    server = start_mock_server()
    yield server
    server.stop()


@pytest.fixture(scope="session")
def client(mock_server):
    return AnalyticsClient(base_url=mock_server.base_url)


@pytest.fixture(scope="session")
def docs(client):
    """Run the StaffExtractor once and share the output across all tests."""
    extractor = StaffExtractor(client=client)  # wh_pool=None → skip warehouse write
    return asyncio.get_event_loop().run_until_complete(
        extractor.run(BUSINESS_ID, START_DATE, END_DATE)
    )


def of_type(docs, doc_type):
    return [d for d in docs if d["doc_type"] == doc_type]

def for_staff(docs, staff_id):
    return [d for d in docs if d.get("staff_id") == staff_id]

def for_period(docs, period_label):
    return [d for d in docs if d.get("period_label") == period_label]


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 Tests — ETL shape and field correctness
# ─────────────────────────────────────────────────────────────────────────────

class TestDocShape:
    def test_produces_documents(self, docs):
        assert len(docs) > 0, "StaffExtractor produced no documents"

    def test_total_document_count(self, docs):
        assert len(docs) == EXPECTED_TOTAL_DOCS, (
            f"Expected {EXPECTED_TOTAL_DOCS} docs, got {len(docs)}"
        )

    def test_all_three_doc_types_present(self, docs):
        types = {d["doc_type"] for d in docs}
        assert types == {"staff_monthly", "staff_summary", "staff_attendance"}

    def test_monthly_doc_count(self, docs):
        assert len(of_type(docs, "staff_monthly")) == EXPECTED_MONTHLY_COUNT

    def test_summary_doc_count(self, docs):
        assert len(of_type(docs, "staff_summary")) == EXPECTED_SUMMARY_COUNT

    def test_attendance_doc_count(self, docs):
        assert len(of_type(docs, "staff_attendance")) == EXPECTED_ATTENDANCE_COUNT

    def test_four_staff_members_in_summary(self, docs):
        staff_ids = {d["staff_id"] for d in of_type(docs, "staff_summary")}
        assert staff_ids == {9, 12, 15, 21}, f"Got staff_ids: {staff_ids}"

    def test_three_active_staff_in_2026(self, docs):
        """Tom left after Jun 2025 — only 3 staff should have 2026 monthly rows."""
        docs_2026 = [d for d in of_type(docs, "staff_monthly")
                     if d.get("period_label", "").startswith("2026")]
        staff_2026 = {d["staff_id"] for d in docs_2026}
        assert 21 not in staff_2026, "Tom Rivera should have no 2026 monthly rows"
        assert staff_2026 == {9, 12, 15}, f"Expected Maria/James/Aisha in 2026, got {staff_2026}"

    def test_three_active_staff_in_attendance(self, docs):
        """Attendance fixture only covers 2025 (6 months × 4 staff = 24 rows)."""
        att_staff = {d["staff_id"] for d in of_type(docs, "staff_attendance")}
        assert att_staff == {9, 12, 15, 21}, f"Got {att_staff}"

    def test_no_2026_attendance_rows_for_tom(self, docs):
        tom_att = [d for d in of_type(docs, "staff_attendance")
                   if d["staff_id"] == 21
                   and d.get("period_label", "").startswith("2026")]
        assert tom_att == [], "Tom Rivera should have no 2026 attendance rows"


class TestTenantIsolation:
    def test_all_docs_have_correct_tenant_id(self, docs):
        wrong = [d for d in docs if d.get("tenant_id") != BUSINESS_ID]
        assert wrong == [], f"{len(wrong)} docs with wrong tenant_id"

    def test_domain_field_is_staff(self, docs):
        wrong = [d for d in docs if d.get("domain") != "staff"]
        assert wrong == [], f"Docs with wrong domain: {[d['doc_type'] for d in wrong]}"

    def test_rejected_unknown_business_id(self, client):
        """Mock server returns 403 for unrecognised business_id."""
        import httpx

        async def _try():
            async with httpx.AsyncClient(timeout=5) as http:
                r = await http.post(
                    f"{client.base_url}/api/v1/leo/staff-performance",
                    json={"business_id": 9999, "start_date": "2025-01-01",
                          "end_date": "2025-06-30", "mode": "monthly"},
                )
                return r.status_code

        status = asyncio.get_event_loop().run_until_complete(_try())
        assert status == 403, f"Expected 403 for unknown business_id, got {status}"


class TestMonthlyFieldCorrectness:
    def test_all_required_fields_present(self, docs):
        required = [
            "tenant_id", "doc_type", "domain", "staff_id", "staff_full_name",
            "is_active", "location_id", "location_name", "period_label",
            "completed_visit_count", "revenue", "commission_earned",
            "review_count",
        ]
        for doc in of_type(docs, "staff_monthly"):
            for field in required:
                assert field in doc, (
                    f"Missing field '{field}' in staff_monthly doc "
                    f"for {doc.get('staff_full_name')} {doc.get('period_label')}"
                )

    def test_avg_rating_is_none_not_zero_when_missing(self, docs):
        """avg_rating must be None when no reviews — never 0 (misleading)."""
        # All staff in our fixture have reviews, so avg_rating should be non-None
        # But the field must be explicitly stored (not absent) to test the None contract
        for doc in of_type(docs, "staff_monthly"):
            assert "avg_rating" in doc, "avg_rating key must always be present"
            # If review_count is 0, avg_rating must be None — never 0
            if doc.get("review_count", 0) == 0:
                assert doc["avg_rating"] is None, (
                    f"avg_rating should be None when review_count=0, "
                    f"got {doc['avg_rating']}"
                )

    def test_revenue_positive_for_active_staff(self, docs):
        active_monthly = [d for d in of_type(docs, "staff_monthly") if d["is_active"]]
        for doc in active_monthly:
            assert doc["revenue"] > 0, (
                f"Active staff {doc['staff_full_name']} {doc['period_label']} "
                f"has zero revenue"
            )

    def test_commission_within_reasonable_range(self, docs):
        """Commission should be between 10% and 20% of revenue for all staff."""
        for doc in of_type(docs, "staff_monthly"):
            if doc["revenue"] > 0 and doc["commission_earned"] > 0:
                rate = doc["commission_earned"] / doc["revenue"]
                assert 0.10 <= rate <= 0.20, (
                    f"{doc['staff_full_name']} {doc['period_label']}: "
                    f"commission rate {rate:.1%} outside 10-20% range"
                )

    def test_maria_commission_rate_is_15_pct(self, docs):
        """Maria Lopez has a confirmed 15% commission rate."""
        maria_docs = [d for d in of_type(docs, "staff_monthly") if d["staff_id"] == 12]
        for doc in maria_docs:
            expected = round(doc["revenue"] * 0.15, 2)
            actual   = doc["commission_earned"]
            assert abs(actual - expected) < 0.05, (
                f"Maria commission wrong in {doc['period_label']}: "
                f"expected ${expected}, got ${actual}"
            )

    def test_total_pay_equals_revenue_plus_tips(self, docs):
        for doc in of_type(docs, "staff_monthly"):
            expected = round(doc["revenue"] + doc["tips"], 2)
            actual   = round(doc["total_pay"], 2)
            assert abs(actual - expected) < 0.01, (
                f"total_pay mismatch for {doc['staff_full_name']} "
                f"{doc['period_label']}: {actual} != {expected}"
            )

    def test_avg_revenue_per_visit_matches_calculation(self, docs):
        """avg_revenue_per_visit = revenue / completed_visit_count."""
        for doc in of_type(docs, "staff_monthly"):
            visits = doc["completed_visit_count"]
            if visits > 0:
                expected = round(doc["revenue"] / visits, 2)
                actual   = round(doc["avg_revenue_per_visit"], 2)
                assert abs(actual - expected) < 0.05, (
                    f"avg_revenue_per_visit wrong for {doc['staff_full_name']} "
                    f"{doc['period_label']}: got {actual}, expected {expected}"
                )


class TestCrossCheckWithAppointments:
    """
    completed_visit_count in staff_monthly MUST match appointments by-staff
    completed_count for the same staff_id + period. This is the key
    cross-domain consistency guarantee from Step 2.
    """

    # Ground truth from appointments_fixtures.py by-staff
    APPT_COMPLETED = {
        (12, "2025-01"): 52, (12, "2025-02"): 47, (12, "2025-03"): 58,
        (12, "2025-04"): 56, (12, "2025-05"): 62, (12, "2025-06"): 68,
        (15, "2025-01"): 48, (15, "2025-02"): 43, (15, "2025-03"): 52,
        (15, "2025-04"): 50, (15, "2025-05"): 54, (15, "2025-06"): 58,
        ( 9, "2025-01"): 46, ( 9, "2025-02"): 42, ( 9, "2025-03"): 54,
        ( 9, "2025-04"): 52, ( 9, "2025-05"): 57, ( 9, "2025-06"): 61,
        (21, "2025-01"): 32, (21, "2025-02"): 31, (21, "2025-03"): 37,
        (21, "2025-04"): 37, (21, "2025-05"): 37, (21, "2025-06"): 36,
        # 2026 (from appointments_fixtures_2026)
        (12, "2026-02"): 83, (12, "2026-03"): 87,
        (15, "2026-02"): 74, (15, "2026-03"): 79,
        ( 9, "2026-02"): 77, ( 9, "2026-03"): 81,
    }

    def test_completed_visit_count_matches_appointments_fixture(self, docs):
        mismatches = []
        for doc in of_type(docs, "staff_monthly"):
            key = (doc["staff_id"], doc["period_label"])
            if key in self.APPT_COMPLETED:
                expected = self.APPT_COMPLETED[key]
                actual   = doc["completed_visit_count"]
                if actual != expected:
                    mismatches.append(
                        f"{doc['staff_full_name']} {doc['period_label']}: "
                        f"got {actual}, expected {expected}"
                    )
        assert mismatches == [], (
            f"completed_visit_count mismatches vs appointments fixture:\n"
            + "\n".join(mismatches)
        )

    def test_org_2025_total_visits_equals_1170(self, docs):
        """Sum of all 2025 completed_visit_count must equal 1170 (revenue total)."""
        total = sum(
            d["completed_visit_count"]
            for d in of_type(docs, "staff_monthly")
            if d.get("period_label", "").startswith("2025")
        )
        assert total == 1170, f"Org 2025 total visits: expected 1170, got {total}"


class TestInactiveStaff:
    def test_tom_is_inactive_in_all_monthly_docs(self, docs):
        tom_monthly = [d for d in of_type(docs, "staff_monthly") if d["staff_id"] == 21]
        assert len(tom_monthly) == 6, f"Tom should have 6 monthly rows, got {len(tom_monthly)}"
        for doc in tom_monthly:
            assert doc["is_active"] is False, (
                f"Tom Rivera should be inactive in {doc['period_label']}"
            )

    def test_tom_is_inactive_in_summary(self, docs):
        tom_summary = [d for d in of_type(docs, "staff_summary") if d["staff_id"] == 21]
        assert len(tom_summary) == 1
        assert tom_summary[0]["is_active"] is False

    def test_tom_is_inactive_in_attendance(self, docs):
        tom_att = [d for d in of_type(docs, "staff_attendance") if d["staff_id"] == 21]
        assert len(tom_att) == 6
        for doc in tom_att:
            assert doc["is_active"] is False

    def test_active_staff_are_active(self, docs):
        for doc in of_type(docs, "staff_summary"):
            if doc["staff_id"] in {9, 12, 15}:
                assert doc["is_active"] is True, (
                    f"{doc['staff_full_name']} should be active"
                )


class TestSummaryAggregation:
    def test_summary_visits_match_monthly_sum_2025(self, docs):
        """summary.total_visits_ytd must equal sum of 2025 monthly rows per staff."""
        monthly_by_staff: dict[int, int] = {}
        for doc in of_type(docs, "staff_monthly"):
            if doc.get("period_label", "").startswith("2025"):
                sid = doc["staff_id"]
                monthly_by_staff[sid] = (
                    monthly_by_staff.get(sid, 0) + doc["completed_visit_count"]
                )

        for doc in of_type(docs, "staff_summary"):
            sid      = doc["staff_id"]
            expected = monthly_by_staff.get(sid, 0)
            actual   = doc["total_visits_ytd"]
            assert actual == expected, (
                f"{doc['staff_full_name']} summary.total_visits_ytd={actual}, "
                f"sum of 2025 monthly={expected}"
            )

    def test_maria_is_top_revenue_ytd(self, docs):
        summary = of_type(docs, "staff_summary")
        revenues = {d["staff_id"]: d["total_revenue_ytd"] for d in summary}
        top_id = max(revenues, key=revenues.get)
        assert top_id == 12, (
            f"Maria Lopez (12) should have top revenue, got staff_id={top_id}"
        )

    def test_tom_has_lowest_rating(self, docs):
        summary = [d for d in of_type(docs, "staff_summary")
                   if d.get("overall_avg_rating") is not None]
        lowest = min(summary, key=lambda d: d["overall_avg_rating"])
        assert lowest["staff_id"] == 21, (
            f"Tom Rivera (21) should have lowest rating, "
            f"got staff_id={lowest['staff_id']}"
        )

    def test_tom_revenue_pct_is_none(self, docs):
        """Tom is inactive in the latest period — revenue_pct_of_org_latest must be None."""
        tom = next(d for d in of_type(docs, "staff_summary") if d["staff_id"] == 21)
        assert tom.get("revenue_pct_of_org_latest") is None, (
            f"Tom's revenue_pct_of_org_latest should be None, "
            f"got {tom.get('revenue_pct_of_org_latest')}"
        )


class TestAttendanceFieldCorrectness:
    def test_all_required_attendance_fields_present(self, docs):
        required = [
            "tenant_id", "doc_type", "domain", "staff_id", "staff_full_name",
            "is_active", "location_id", "location_name", "period_label",
            "days_with_signin", "days_fully_recorded", "days_missing_signout",
            "total_hours_worked",
        ]
        for doc in of_type(docs, "staff_attendance"):
            for field in required:
                assert field in doc, (
                    f"Missing field '{field}' in staff_attendance doc "
                    f"for {doc.get('staff_full_name')} {doc.get('period_label')}"
                )

    def test_maria_has_most_hours(self, docs):
        """Maria Lopez (12) should have highest total hours per period."""
        by_period: dict[str, dict[int, float]] = {}
        for doc in of_type(docs, "staff_attendance"):
            period = doc["period_label"]
            by_period.setdefault(period, {})[doc["staff_id"]] = (
                doc["total_hours_worked"]
            )

        for period, hours_by_staff in by_period.items():
            if len(hours_by_staff) > 1:
                top_id = max(hours_by_staff, key=hours_by_staff.get)
                assert top_id == 12, (
                    f"Maria (12) should have most hours in {period}, "
                    f"got staff_id={top_id} with {hours_by_staff[top_id]}h"
                )

    def test_tom_hours_decline_over_2025(self, docs):
        """Tom's total_hours_worked should trend downward Jan→Jun 2025."""
        tom_att = sorted(
            [d for d in of_type(docs, "staff_attendance") if d["staff_id"] == 21],
            key=lambda d: d["period_label"],
        )
        hours = [d["total_hours_worked"] for d in tom_att]
        # Net trend: first month > last month
        assert hours[0] > hours[-1], (
            f"Tom's hours should decline Jan→Jun 2025: {hours}"
        )

    def test_days_fully_recorded_leq_days_with_signin(self, docs):
        for doc in of_type(docs, "staff_attendance"):
            assert doc["days_fully_recorded"] <= doc["days_with_signin"], (
                f"{doc['staff_full_name']} {doc['period_label']}: "
                f"days_fully_recorded ({doc['days_fully_recorded']}) > "
                f"days_with_signin ({doc['days_with_signin']})"
            )

    def test_days_missing_signout_equals_gap(self, docs):
        """days_missing_signout = days_with_signin - days_fully_recorded."""
        for doc in of_type(docs, "staff_attendance"):
            expected = doc["days_with_signin"] - doc["days_fully_recorded"]
            actual   = doc["days_missing_signout"]
            assert actual == expected, (
                f"{doc['staff_full_name']} {doc['period_label']}: "
                f"days_missing_signout={actual}, expected={expected}"
            )

    def test_avg_hours_per_day_is_none_when_no_full_days(self, docs):
        """avg_hours_per_day must be None (not 0) when days_fully_recorded = 0."""
        for doc in of_type(docs, "staff_attendance"):
            if doc["days_fully_recorded"] == 0:
                assert doc.get("avg_hours_per_day") is None, (
                    f"avg_hours_per_day should be None when days_fully_recorded=0"
                )


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 Tests — Chunk text quality and vocabulary
# ─────────────────────────────────────────────────────────────────────────────

class TestChunkTextQuality:
    def test_all_docs_produce_non_empty_text(self, docs):
        """generate_staff_docs must produce non-empty chunk text for every doc."""
        for doc in docs:
            doc_type = doc["doc_type"]
            if doc_type == "staff_monthly":
                text = _chunk_staff_monthly(doc)
            elif doc_type == "staff_summary":
                text = _chunk_staff_summary(doc)
            elif doc_type == "staff_attendance":
                text = _chunk_staff_attendance(doc)
            else:
                continue
            assert len(text.strip()) > 20, (
                f"Chunk text too short for {doc_type} "
                f"{doc.get('staff_full_name')} {doc.get('period_label')}: '{text}'"
            )

    def test_monthly_chunk_contains_revenue(self, docs):
        maria = next(
            d for d in of_type(docs, "staff_monthly")
            if d["staff_id"] == 12 and d["period_label"] == "2026-03"
        )
        text = _chunk_staff_monthly(maria)
        assert "5,959.50" in text, "Revenue amount should appear in monthly chunk"
        assert "commission" in text.lower()
        assert "rating" in text.lower()

    def test_monthly_chunk_marks_inactive_staff(self, docs):
        tom = next(
            d for d in of_type(docs, "staff_monthly")
            if d["staff_id"] == 21 and d["period_label"] == "2025-01"
        )
        text = _chunk_staff_monthly(tom)
        assert "deactivated" in text.lower() or "no longer active" in text.lower(), (
            "Inactive staff chunk should mention deactivated status"
        )

    def test_summary_chunk_contains_mvp_vocabulary(self, docs):
        """Summary chunks must contain 'top performer' / 'MVP' for Q30 routing."""
        maria_summary = next(
            d for d in of_type(docs, "staff_summary") if d["staff_id"] == 12
        )
        text = _chunk_staff_summary(maria_summary)
        assert "top performer" in text.lower() or "mvp" in text.lower(), (
            "Summary chunk must contain 'top performer' or 'MVP' for Q30 vocab"
        )

    def test_attendance_chunk_contains_hours_vocabulary(self, docs):
        """Attendance chunks must contain 'hours worked' / 'clocked' for Q33."""
        maria_att = next(
            d for d in of_type(docs, "staff_attendance")
            if d["staff_id"] == 12 and d["period_label"] == "2025-06"
        )
        text = _chunk_staff_attendance(maria_att)
        assert "hours" in text.lower()
        assert "clocked" in text.lower() or "worked" in text.lower(), (
            "Attendance chunk must contain hours/clocked vocabulary for Q33"
        )

    def test_doc_ids_are_unique_across_all_docs(self, docs):
        doc_ids = [_make_doc_id(BUSINESS_ID, d["doc_type"], d) for d in docs]
        assert len(doc_ids) == len(set(doc_ids)), (
            f"Doc ID collision detected: {len(doc_ids)} total, "
            f"{len(set(doc_ids))} unique"
        )

    def test_monthly_chunk_mentions_location(self, docs):
        """Location name must appear in monthly chunk for Q15–Q19 retrieval."""
        aisha = next(
            d for d in of_type(docs, "staff_monthly")
            if d["staff_id"] == 9 and d["period_label"] == "2025-06"
        )
        text = _chunk_staff_monthly(aisha)
        assert "Westside" in text, "Location name should appear in monthly chunk"

    def test_summary_chunk_contains_tenure_info(self, docs):
        """Tenure (hire date / active period) needed for Q22."""
        tom_summary = next(
            d for d in of_type(docs, "staff_summary") if d["staff_id"] == 21
        )
        text = _chunk_staff_summary(tom_summary)
        assert "2020" in text, "Tom's tenure start year should appear in summary chunk"


# ─────────────────────────────────────────────────────────────────────────────
# Step 6 Tests — All 40 test questions route to RAG
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryRouting:
    """
    Every one of the 40 Step 1 test questions must route to RAG.
    Uses rules only (no gateway) — this is fast and deterministic.
    """

    ALL_40_QUESTIONS = [
        # Category 1 — Basic lookups
        "How many appointments did Sarah complete last month?",
        "How much revenue did Marcus generate in Q1?",
        "What is Emma's average customer rating?",
        "Show me all staff members and their total revenue this month.",
        "Is Jake currently active on the team?",
        # Category 2 — Rankings
        "Who is my top-performing staff member this month by revenue?",
        "Who completed the most appointments last quarter?",
        "Which staff member has the highest average customer rating?",
        "Rank all my staff by revenue generated this year.",
        "Who has the lowest rating on my team?",
        # Category 3 — Trends
        "Has Sarah's revenue been increasing or decreasing over the last 3 months?",
        "Which staff member improved the most in bookings from last month to this month?",
        "Show me Marcus's performance month by month for the past 6 months.",
        "Did any staff member's revenue drop significantly this month compared to last?",
        # Category 4 — Location
        "Who is the top performer at my downtown location?",
        "How does staff performance compare between my two locations?",
        "Show me all staff working at the Main Street branch and their revenue this month.",
        "Which location has the strongest team overall?",
        "Does Sarah work across multiple locations, and if so, how does her revenue split?",
        # Category 5 — Edge cases
        "What about a staff member who had zero visits this month — do they still show up?",
        "What if a staff member was deactivated mid-month — does their partial data still count?",
        "Show me performance for staff who joined this year only.",
        "What happens if a visit has no assigned staff member — where does that revenue go?",
        "A staff member processed a visit but it was later refunded — does that revenue still count?",
        # Category 6 — Vocabulary variants
        "Who are my best workers?",
        "Which employee made the most money for us?",
        "Who's been slacking lately?",
        "Which stylist got the best reviews?",
        "How's my team doing?",
        "Who's my MVP this month?",
        "Give me the team's numbers.",
        "Which technician handled the most clients?",
        "Who clocked the most hours?",
        # Category 7 — Commission & pay
        "How much commission did the team earn in total last month?",
        "What percentage of total business revenue did Marcus generate this quarter?",
        "Which staff member earns the highest commission rate?",
        "Show me commission earned per staff member this month.",
        # Category 8 — Root cause
        "Why did revenue drop this month — was it a staffing issue?",
        "Is there a staff member causing a high number of cancellations?",
        "Which staff member has the most no-shows linked to them?",
    ]

    @pytest.fixture(scope="class")
    def analyzer(self):
        return QueryAnalyzer()  # rules only — no gateway needed

    @pytest.mark.parametrize("question", ALL_40_QUESTIONS)
    def test_routes_to_rag(self, analyzer, question):
        result = asyncio.get_event_loop().run_until_complete(
            analyzer.analyze(question, business_id="test_42")
        )
        assert result.route == Route.RAG, (
            f"Expected RAG for: '{question}'\n"
            f"Got: {result.route} (confidence={result.confidence:.2f}, "
            f"method={result.method}, keywords={result.matched_keywords})"
        )

    def test_general_advice_still_routes_direct(self, analyzer):
        """Regression check — general advice must NOT route to RAG."""
        advice_questions = [
            "How can salons improve customer retention?",
            "What are the best practices for upselling?",
            "Give me tips on staff scheduling.",
            "Explain what a cancellation rate means.",
        ]
        for q in advice_questions:
            result = asyncio.get_event_loop().run_until_complete(
                analyzer.analyze(q, business_id="test_42")
            )
            assert result.route == Route.DIRECT, (
                f"General advice question routed to RAG: '{q}'"
            )