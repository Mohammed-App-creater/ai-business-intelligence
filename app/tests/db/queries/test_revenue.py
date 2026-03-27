"""
Tests for queries/revenue.py

Tests verify:
  - Correct SQL params passed to cursor.execute()
  - Return value is passed through from fetchall()
  - PaymentStatus=1 filter is always in the query
  - All functions require org_id (tenant isolation)
  - Empty results handled correctly
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, revenue_rows, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.queries.revenue import (
    get_monthly_revenue,
    get_monthly_revenue_totals,
    get_revenue_by_payment_type,
    get_revenue_by_staff,
    get_daily_revenue,
    get_promo_usage,
)


# ---------------------------------------------------------------------------
# get_monthly_revenue
# ---------------------------------------------------------------------------

class TestGetMonthlyRevenue:

    async def test_returns_fetchall_result(self):
        rows = revenue_rows(3)
        pool, _ = make_mock_pool(rows=rows)
        result = await get_monthly_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id_as_first_param(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_sql_contains_payment_status_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql
        assert "= 1" in sql

    async def test_sql_groups_by_payment_type(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentType" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_monthly_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []

    async def test_returns_list(self):
        pool, _ = make_mock_pool(rows=revenue_rows(1))
        result = await get_monthly_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# get_monthly_revenue_totals
# ---------------------------------------------------------------------------

class TestGetMonthlyRevenueTotals:

    async def test_returns_fetchall_result(self):
        rows = [
            {"month": "2026-01", "visit_count": 130, "total_revenue": 12000.0},
            {"month": "2026-02", "visit_count": 145, "total_revenue": 13100.0},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_monthly_revenue_totals(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_revenue_totals(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_contains_payment_status_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_revenue_totals(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql

    async def test_sql_does_not_group_by_payment_type(self):
        """Totals query collapses payment types — no PaymentType in GROUP BY."""
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_revenue_totals(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        # GROUP BY should only contain the month expression, not PaymentType
        assert "GROUP BY" in sql
        # Should not split by payment type
        group_by_section = sql[sql.upper().index("GROUP BY"):]
        assert "PaymentType" not in group_by_section


# ---------------------------------------------------------------------------
# get_revenue_by_payment_type
# ---------------------------------------------------------------------------

class TestGetRevenueByPaymentType:

    async def test_returns_fetchall_result(self):
        rows = [
            {"payment_type": "Card", "visit_count": 98, "total": 7800.0},
            {"payment_type": "Cash", "visit_count": 52, "total": 3200.0},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_revenue_by_payment_type(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_revenue_by_payment_type(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_groups_by_payment_type(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_revenue_by_payment_type(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentType" in sql
        assert "GROUP BY" in sql

    async def test_sql_filters_successful_payments(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_revenue_by_payment_type(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql


# ---------------------------------------------------------------------------
# get_revenue_by_staff
# ---------------------------------------------------------------------------

class TestGetRevenueByStaff:

    async def test_returns_fetchall_result(self):
        rows = [
            {
                "emp_id": 12, "first_name": "Maria", "last_name": "Garcia",
                "visit_count": 87, "total_revenue": 5200.0,
                "avg_ticket": 59.77, "tips": 420.0,
            }
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_revenue_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_revenue_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_joins_tbl_emp(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_revenue_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_emp" in sql

    async def test_sql_filters_active_employees(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_revenue_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql

    async def test_sql_includes_avg_ticket(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_revenue_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "AVG" in sql

    async def test_sql_orders_by_revenue_desc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_revenue_by_staff(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql
        assert "DESC" in sql


# ---------------------------------------------------------------------------
# get_daily_revenue
# ---------------------------------------------------------------------------

class TestGetDailyRevenue:

    async def test_returns_fetchall_result(self):
        rows = [
            {"date": "2026-03-01", "visit_count": 12, "total_revenue": 980.0},
            {"date": "2026-03-02", "visit_count": 8,  "total_revenue": 640.0},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_daily_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_daily_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_groups_by_date(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_daily_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "DATE(" in sql

    async def test_sql_filters_successful_payments(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_daily_revenue(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql


# ---------------------------------------------------------------------------
# get_promo_usage
# ---------------------------------------------------------------------------

class TestGetPromoUsage:

    async def test_returns_fetchall_result(self):
        rows = [
            {
                "promo_id": 3, "promo_code": "SUMMER20",
                "times_used": 14, "total_discount": 280.0,
            }
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_promo_usage(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_usage(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_joins_tbl_promo(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_usage(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_promo" in sql

    async def test_sql_filters_non_null_promo(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_usage(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "IS NOT NULL" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_promo_usage(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []
