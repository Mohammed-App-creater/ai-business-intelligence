"""
Tests for queries/expenses.py
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, expense_rows, net_profit_rows, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.queries.expenses import (
    get_monthly_expenses,
    get_monthly_expense_totals,
    get_net_profit,
    get_expenses_by_category,
    get_expenses_by_location,
)


# ---------------------------------------------------------------------------
# get_monthly_expenses
# ---------------------------------------------------------------------------

class TestGetMonthlyExpenses:

    async def test_returns_fetchall_result(self):
        rows = expense_rows(3)
        pool, _ = make_mock_pool(rows=rows)
        result = await get_monthly_expenses(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id_as_first_param(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_expenses(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_expenses(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_sql_filters_deleted(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_expenses(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "isDeleted" in sql
        assert "= 0" in sql

    async def test_sql_joins_both_category_tables(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_expenses(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_expense_category" in sql
        assert "tbl_expense_subcategory" in sql

    async def test_sql_groups_by_month_and_category(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_expenses(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql
        assert "Category" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_monthly_expenses(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []

    async def test_returns_list(self):
        pool, _ = make_mock_pool(rows=expense_rows(1))
        result = await get_monthly_expenses(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# get_monthly_expense_totals
# ---------------------------------------------------------------------------

class TestGetMonthlyExpenseTotals:

    async def test_returns_fetchall_result(self):
        rows = [
            {"month": "2026-01", "expense_count": 20, "total": 3200.0},
            {"month": "2026-02", "expense_count": 22, "total": 3400.0},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_monthly_expense_totals(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_expense_totals(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_filters_deleted(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_expense_totals(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "isDeleted" in sql

    async def test_sql_does_not_join_category_tables(self):
        """Totals query needs no category join — keep it fast."""
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_expense_totals(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_expense_category" not in sql

    async def test_sql_groups_by_month_only(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_expense_totals(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql
        assert "%%Y-%%m" in sql or "%Y-%m" in sql


# ---------------------------------------------------------------------------
# get_net_profit
# ---------------------------------------------------------------------------

class TestGetNetProfit:

    async def test_returns_fetchall_result(self):
        rows = net_profit_rows()
        pool, _ = make_mock_pool(rows=rows)
        result = await get_net_profit(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id_multiple_times(self):
        """Net profit query uses org_id in 4 subqueries."""
        pool, cursor = make_mock_pool(rows=[])
        await get_net_profit(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        org_id_count = sum(1 for p in params if p == ORG_ID)
        assert org_id_count == 4

    async def test_sql_contains_union(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_net_profit(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "UNION" in sql

    async def test_sql_contains_left_join(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_net_profit(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "LEFT JOIN" in sql

    async def test_sql_references_both_tables(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_net_profit(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_visit" in sql
        assert "tbl_expense" in sql

    async def test_sql_filters_payment_status_for_revenue(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_net_profit(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql

    async def test_total_params_count(self):
        """Expect exactly 12 params: org_id + from + to repeated 4 times."""
        pool, cursor = make_mock_pool(rows=[])
        await get_net_profit(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert len(params) == 12

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_net_profit(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_expenses_by_category
# ---------------------------------------------------------------------------

class TestGetExpensesByCategory:

    async def test_returns_fetchall_result(self):
        rows = [
            {"category": "Supplies", "expense_count": 15, "total": 2400.0, "pct_of_total": 45.2},
            {"category": "Rent",     "expense_count": 1,  "total": 2000.0, "pct_of_total": 37.7},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_expenses_by_category(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_expenses_by_category(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_joins_category_table(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_expenses_by_category(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_expense_category" in sql

    async def test_sql_includes_percentage(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_expenses_by_category(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "OVER" in sql or "pct" in sql.lower()

    async def test_sql_orders_by_total_desc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_expenses_by_category(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql
        assert "DESC" in sql


# ---------------------------------------------------------------------------
# get_expenses_by_location
# ---------------------------------------------------------------------------

class TestGetExpensesByLocation:

    async def test_returns_fetchall_result(self):
        rows = [
            {"location_id": 1, "expense_count": 18, "total": 1800.0},
            {"location_id": 2, "expense_count": 10, "total": 1000.0},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_expenses_by_location(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_expenses_by_location(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_groups_by_location(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_expenses_by_location(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "LocationID" in sql
        assert "GROUP BY" in sql