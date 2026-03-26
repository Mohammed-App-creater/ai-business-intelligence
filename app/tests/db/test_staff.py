"""
Tests for queries/staff.py
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.queries.staff import (
    get_staff_performance,
    get_staff_ratings,
    get_visit_reviews,
    get_staff_hours,
    get_staff_commission_structure,
    get_google_reviews_summary,
    get_staff_roster,
)

EMP_ID = 12


# ---------------------------------------------------------------------------
# get_staff_performance
# ---------------------------------------------------------------------------

class TestGetStaffPerformance:

    async def test_returns_fetchall_result(self):
        rows = [{"emp_id": 12, "first_name": "Maria", "last_name": "Garcia",
                 "visit_count": 87, "total_revenue": 5200.0,
                 "avg_ticket": 59.77, "tips": 420.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_staff_performance(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id_first(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_performance(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_performance(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_sql_joins_tbl_emp(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_performance(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_emp" in sql

    async def test_sql_filters_active_employees(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_performance(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql

    async def test_sql_filters_successful_payments(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_performance(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql

    async def test_sql_includes_avg_ticket(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_performance(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "AVG" in sql

    async def test_sql_orders_by_revenue_desc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_performance(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql and "DESC" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_staff_performance(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_staff_ratings
# ---------------------------------------------------------------------------

class TestGetStaffRatings:

    async def test_returns_fetchall_result(self):
        rows = [{"emp_id": 12, "first_name": "Maria", "last_name": "Garcia",
                 "review_count": 42, "avg_rating": 4.8,
                 "five_star_count": 35, "one_star_count": 1}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_staff_ratings(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_ratings(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_joins_tbl_visit_for_org_scoping(self):
        """tbl_emp_reviews has no OrgId — must join tbl_visit."""
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_ratings(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_visit" in sql
        assert "OrganizationId" in sql

    async def test_sql_joins_tbl_emp_reviews(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_ratings(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_emp_reviews" in sql

    async def test_sql_includes_avg_rating(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_ratings(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "AVG" in sql and "rating" in sql.lower()

    async def test_sql_includes_five_star_count(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_ratings(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "5" in sql


# ---------------------------------------------------------------------------
# get_visit_reviews
# ---------------------------------------------------------------------------

class TestGetVisitReviews:

    async def test_returns_fetchall_result(self):
        rows = [{"review_count": 87, "avg_rating": 4.6,
                 "five_star_count": 60, "four_star_count": 18,
                 "three_star_count": 5, "two_star_count": 2,
                 "one_star_count": 2}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_visit_reviews(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_visit_reviews(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_queries_visit_review_table(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_visit_reviews(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_visit_review" in sql

    async def test_sql_org_scoped_directly(self):
        """tbl_visit_review has OrganizationId — scoped without joining tbl_visit."""
        pool, cursor = make_mock_pool(rows=[])
        await get_visit_reviews(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "OrganizationId" in sql
        # tbl_visit_review contains the substring 'tbl_visit' — verify no JOIN
        assert "JOIN" not in sql.upper()

    async def test_sql_includes_rating_distribution(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_visit_reviews(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        # Should have CASE WHEN for each star level
        assert sql.count("CASE WHEN") >= 5


# ---------------------------------------------------------------------------
# get_staff_hours
# ---------------------------------------------------------------------------

class TestGetStaffHours:

    async def test_returns_fetchall_result(self):
        rows = [{"emp_id": 12, "first_name": "Maria", "last_name": "Garcia",
                 "days_attended": 22, "total_hours": 176.5}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_staff_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_joins_tbl_emp(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_emp" in sql

    async def test_sql_excludes_sentinel_zero_values(self):
        """time_sign_in/out default is '0' — must be excluded."""
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "!= '0'" in sql or "NULLIF" in sql

    async def test_sql_uses_timediff_for_duration(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "TIMEDIFF" in sql

    async def test_sql_converts_to_hours(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_hours(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "3600" in sql or "TIME_TO_SEC" in sql


# ---------------------------------------------------------------------------
# get_staff_commission_structure
# ---------------------------------------------------------------------------

class TestGetStaffCommissionStructure:

    async def test_returns_fetchall_result(self):
        rows = [{"emp_id": 12, "first_name": "Maria", "last_name": "Garcia",
                 "service_id": 5, "service_name": "Balayage",
                 "commission_type": "%", "commission": 30.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_staff_commission_structure(pool, ORG_ID)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_commission_structure(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_no_emp_id_filter_by_default(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_commission_structure(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert len(params) == 1

    async def test_emp_id_filter_when_provided(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_commission_structure(pool, ORG_ID, emp_id=EMP_ID)
        params = cursor.execute.call_args[0][1]
        assert EMP_ID in params

    async def test_sql_filters_active_commission_records(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_commission_structure(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql

    async def test_sql_joins_tbl_service(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_commission_structure(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_service" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_staff_commission_structure(pool, ORG_ID)
        assert result == []


# ---------------------------------------------------------------------------
# get_google_reviews_summary
# ---------------------------------------------------------------------------

class TestGetGoogleReviewsSummary:

    async def test_returns_fetchall_result(self):
        rows = [{"location_id": 1, "review_count": 24,
                 "avg_rating": 4.3, "bad_review_count": 2,
                 "replied_count": 18}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_google_reviews_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_google_reviews_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_queries_google_review_table(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_google_reviews_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_google_review" in sql

    async def test_sql_includes_bad_review_count(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_google_reviews_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "is_bad_review" in sql

    async def test_sql_groups_by_location(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_google_reviews_summary(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "location_id" in sql.lower()
        assert "GROUP BY" in sql


# ---------------------------------------------------------------------------
# get_staff_roster
# ---------------------------------------------------------------------------

class TestGetStaffRoster:

    async def test_returns_fetchall_result(self):
        rows = [{"emp_id": 12, "first_name": "Maria", "last_name": "Garcia",
                 "hire_date": "2022-03-15", "role_id": 3, "active": 1}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_staff_roster(pool, ORG_ID)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_roster(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_active_only_true_adds_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_roster(pool, ORG_ID, active_only=True)
        sql = cursor.execute.call_args[0][0]
        assert "Active = 1" in sql

    async def test_active_only_false_omits_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_staff_roster(pool, ORG_ID, active_only=False)
        sql = cursor.execute.call_args[0][0]
        assert "AND Active = 1" not in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_staff_roster(pool, ORG_ID)
        assert result == []
