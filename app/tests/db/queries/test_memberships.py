"""
Tests for queries/memberships.py
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.queries.memberships import (
    get_active_subscription_count,
    get_subscription_growth,
    get_subscriptions_by_service,
    get_upcoming_renewals,
    get_subscription_cancellations,
)


# ---------------------------------------------------------------------------
# get_active_subscription_count
# ---------------------------------------------------------------------------

class TestGetActiveSubscriptionCount:

    async def test_returns_single_dict(self):
        rows = [{"active_count": 42, "inactive_count": 8, "total_count": 50,
                 "monthly_recurring_revenue": 2100.0,
                 "avg_subscription_amount": 50.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_active_subscription_count(pool, ORG_ID)
        assert isinstance(result, dict)

    async def test_returns_correct_values(self):
        rows = [{"active_count": 42, "inactive_count": 8, "total_count": 50,
                 "monthly_recurring_revenue": 2100.0,
                 "avg_subscription_amount": 50.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_active_subscription_count(pool, ORG_ID)
        assert result["active_count"] == 42
        assert result["monthly_recurring_revenue"] == 2100.0

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[{"active_count": 0,
                                             "inactive_count": 0,
                                             "total_count": 0,
                                             "monthly_recurring_revenue": 0,
                                             "avg_subscription_amount": 0}])
        await get_active_subscription_count(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_queries_custsubscription(self):
        pool, cursor = make_mock_pool(rows=[{"active_count": 0,
                                             "inactive_count": 0,
                                             "total_count": 0,
                                             "monthly_recurring_revenue": 0,
                                             "avg_subscription_amount": 0}])
        await get_active_subscription_count(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_custsubscription" in sql

    async def test_sql_computes_mrr(self):
        pool, cursor = make_mock_pool(rows=[{"active_count": 0,
                                             "inactive_count": 0,
                                             "total_count": 0,
                                             "monthly_recurring_revenue": 0,
                                             "avg_subscription_amount": 0}])
        await get_active_subscription_count(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "Amount" in sql and "Discount" in sql

    async def test_empty_fetchall_returns_empty_dict(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_active_subscription_count(pool, ORG_ID)
        assert result == {}


# ---------------------------------------------------------------------------
# get_subscription_growth
# ---------------------------------------------------------------------------

class TestGetSubscriptionGrowth:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "new_subscriptions": 8, "total_value": 400.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_subscription_growth(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_subscription_growth(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_subscription_growth(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_subscription_growth(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql

    async def test_sql_filters_by_create_date(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_subscription_growth(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "SubCreateDate" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_subscription_growth(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_subscriptions_by_service
# ---------------------------------------------------------------------------

class TestGetSubscriptionsByService:

    async def test_returns_fetchall_result(self):
        rows = [{"service_id": 5, "service_name": "Monthly Balayage",
                 "active_count": 18, "total_monthly_value": 900.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_subscriptions_by_service(pool, ORG_ID)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_subscriptions_by_service(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_joins_tbl_service(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_subscriptions_by_service(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_service" in sql

    async def test_sql_groups_by_service(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_subscriptions_by_service(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql
        assert "ServiceID" in sql

    async def test_sql_includes_active_count(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_subscriptions_by_service(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_subscriptions_by_service(pool, ORG_ID)
        assert result == []


# ---------------------------------------------------------------------------
# get_upcoming_renewals
# ---------------------------------------------------------------------------

class TestGetUpcomingRenewals:

    async def test_returns_fetchall_result(self):
        rows = [{"subscription_id": 22, "cust_id": 441,
                 "service_id": 5, "amount": 50.0, "discount": 0.0,
                 "execution_date": "2026-04-01", "interval_days": 30}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_upcoming_renewals(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_upcoming_renewals(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_filters_active_subscriptions(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_upcoming_renewals(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql and "= 1" in sql

    async def test_sql_filters_by_execution_date(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_upcoming_renewals(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "SubExecutionDate" in sql

    async def test_sql_orders_by_execution_date_asc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_upcoming_renewals(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql and "ASC" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_upcoming_renewals(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_subscription_cancellations
# ---------------------------------------------------------------------------

class TestGetSubscriptionCancellations:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "cancelled_count": 3,
                 "lost_monthly_value": 150.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_subscription_cancellations(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_subscription_cancellations(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_filters_inactive(self):
        """Cancelled = Active = 0."""
        pool, cursor = make_mock_pool(rows=[])
        await get_subscription_cancellations(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql and "= 0" in sql

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_subscription_cancellations(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_subscription_cancellations(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []
