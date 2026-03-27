"""
Tests for queries/promos.py
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.queries.promos import (
    get_promo_catalog,
    get_active_promos,
    get_promo_usage_by_org,
    get_monthly_promo_impact,
)


# ---------------------------------------------------------------------------
# get_promo_catalog
# ---------------------------------------------------------------------------

class TestGetPromoCatalog:

    async def test_returns_fetchall_result(self):
        rows = [{"id": 3, "promo_code": "SUMMER20",
                 "description": "Summer discount", "amount": 20.0,
                 "expiration_date": "2026-08-31",
                 "subscription_cycle": 3, "active": 1}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_promo_catalog(pool)
        assert result == rows

    async def test_no_params_by_default(self):
        """tbl_promo has no OrgId — no org param needed."""
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_catalog(pool)
        params = cursor.execute.call_args[0][1]
        assert params == ()

    async def test_active_only_true_adds_where_clause(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_catalog(pool, active_only=True)
        sql = cursor.execute.call_args[0][0]
        assert "Active = 1" in sql

    async def test_active_only_false_omits_where_clause(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_catalog(pool, active_only=False)
        sql = cursor.execute.call_args[0][0]
        assert "Active = 1" not in sql

    async def test_sql_queries_tbl_promo(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_catalog(pool)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_promo" in sql

    async def test_sql_has_no_org_scoping(self):
        """tbl_promo is platform-level — no OrgId filter ever."""
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_catalog(pool)
        sql = cursor.execute.call_args[0][0]
        assert "OrgId" not in sql
        assert "OrganizationId" not in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_promo_catalog(pool)
        assert result == []


# ---------------------------------------------------------------------------
# get_active_promos
# ---------------------------------------------------------------------------

class TestGetActivePromos:

    async def test_returns_fetchall_result(self):
        rows = [{"id": 3, "promo_code": "SUMMER20",
                 "description": "Summer discount", "amount": 20.0,
                 "expiration_date": "2026-08-31", "subscription_cycle": 3}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_active_promos(pool)
        assert result == rows

    async def test_no_params(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_active_promos(pool)
        params = cursor.execute.call_args[0][1]
        assert params == ()

    async def test_sql_filters_active(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_active_promos(pool)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql and "= 1" in sql

    async def test_sql_filters_not_expired_using_curdate(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_active_promos(pool)
        sql = cursor.execute.call_args[0][0]
        assert "PromoExpiration" in sql
        assert "CURDATE()" in sql

    async def test_sql_orders_asc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_active_promos(pool)
        sql = cursor.execute.call_args[0][0]
        assert "ASC" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_active_promos(pool)
        assert result == []


# ---------------------------------------------------------------------------
# get_promo_usage_by_org
# ---------------------------------------------------------------------------

class TestGetPromoUsageByOrg:

    async def test_returns_fetchall_result(self):
        rows = [{"promo_id": 3, "promo_code": "SUMMER20",
                 "description": "Summer discount",
                 "times_used": 14, "total_discount": 280.0,
                 "avg_discount_per_visit": 20.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_promo_usage_by_org(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id_first(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_usage_by_org(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_usage_by_org(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_sql_joins_visit_to_promo(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_usage_by_org(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_visit" in sql
        assert "tbl_promo" in sql
        assert "JOIN" in sql

    async def test_sql_scopes_by_org_via_visit(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_usage_by_org(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "OrganizationId" in sql

    async def test_sql_filters_successful_payments(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_usage_by_org(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql

    async def test_sql_excludes_null_promo_codes(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_usage_by_org(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "IS NOT NULL" in sql

    async def test_sql_orders_by_times_used_desc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_promo_usage_by_org(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql and "DESC" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_promo_usage_by_org(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_monthly_promo_impact
# ---------------------------------------------------------------------------

class TestGetMonthlyPromoImpact:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "promo_visits": 18,
                 "total_discount_given": 360.0,
                 "pct_visits_with_promo": 12.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_monthly_promo_impact(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_promo_impact(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_promo_impact(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_promo_impact(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql

    async def test_sql_uses_case_when_for_promo_visits(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_promo_impact(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "CASE WHEN" in sql
        assert "PromoCode" in sql

    async def test_sql_computes_percentage(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_promo_impact(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "100.0" in sql and "NULLIF" in sql

    async def test_sql_filters_successful_payments(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_promo_impact(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_monthly_promo_impact(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []
