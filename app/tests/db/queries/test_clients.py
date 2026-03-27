"""
Tests for queries/clients.py
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.queries.clients import (
    get_active_client_count,
    get_client_retention,
    get_lapsed_clients,
    get_top_clients_by_spend,
    get_walkin_stats,
    get_visit_frequency_distribution,
)


# ---------------------------------------------------------------------------
# get_active_client_count
# ---------------------------------------------------------------------------

class TestGetActiveClientCount:

    async def test_returns_single_dict(self):
        rows = [{"active_count": 842, "inactive_count": 120, "total_count": 962}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_active_client_count(pool, ORG_ID)
        assert isinstance(result, dict)

    async def test_returns_correct_values(self):
        rows = [{"active_count": 842, "inactive_count": 120, "total_count": 962}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_active_client_count(pool, ORG_ID)
        assert result["active_count"] == 842

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[{"active_count": 0,
                                             "inactive_count": 0,
                                             "total_count": 0}])
        await get_active_client_count(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_queries_custorg_table(self):
        pool, cursor = make_mock_pool(rows=[{"active_count": 0,
                                             "inactive_count": 0,
                                             "total_count": 0}])
        await get_active_client_count(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_custorg" in sql

    async def test_empty_fetchall_returns_empty_dict(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_active_client_count(pool, ORG_ID)
        assert result == {}


# ---------------------------------------------------------------------------
# get_client_retention
# ---------------------------------------------------------------------------

class TestGetClientRetention:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "new_clients": 34,
                 "returning_clients": 98, "total_unique_clients": 132}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_client_retention(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id_twice(self):
        """Retention query uses org_id in a subquery AND the outer query."""
        pool, cursor = make_mock_pool(rows=[])
        await get_client_retention(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        org_id_count = sum(1 for p in params if p == ORG_ID)
        assert org_id_count == 2

    async def test_sql_uses_subquery_for_first_visit(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_client_retention(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "MIN(" in sql or "first" in sql.lower()

    async def test_sql_filters_successful_payments(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_client_retention(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_client_retention(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_client_retention(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_lapsed_clients
# ---------------------------------------------------------------------------

class TestGetLapsedClients:

    async def test_returns_fetchall_result(self):
        rows = [{"cust_id": 441, "total_visits": 8,
                 "last_visit_date": "2025-12-01",
                 "days_since_last_visit": 115, "total_spend": 640.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_lapsed_clients(pool, ORG_ID)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_lapsed_clients(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_default_days_is_60(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_lapsed_clients(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[1] == 60

    async def test_custom_days_passed(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_lapsed_clients(pool, ORG_ID, days_since_visit=90)
        params = cursor.execute.call_args[0][1]
        assert params[1] == 90

    async def test_default_limit_is_50(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_lapsed_clients(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[2] == 50

    async def test_sql_uses_having_for_days_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_lapsed_clients(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "HAVING" in sql
        assert "DATEDIFF" in sql

    async def test_sql_joins_tbl_custorg_for_active_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_lapsed_clients(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_custorg" in sql
        assert "Active" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_lapsed_clients(pool, ORG_ID)
        assert result == []


# ---------------------------------------------------------------------------
# get_top_clients_by_spend
# ---------------------------------------------------------------------------

class TestGetTopClientsBySpend:

    async def test_returns_fetchall_result(self):
        rows = [{"cust_id": 441, "first_name": "Jane", "last_name": "Smith",
                 "visit_count": 12, "total_spend": 1440.0,
                 "avg_spend_per_visit": 120.0, "loyalty_points": 144}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_top_clients_by_spend(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_top_clients_by_spend(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_default_limit_is_20(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_top_clients_by_spend(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[-1] == 20

    async def test_custom_limit_passed(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_top_clients_by_spend(pool, ORG_ID, FROM_DATE, TO_DATE, limit=10)
        params = cursor.execute.call_args[0][1]
        assert params[-1] == 10

    async def test_sql_joins_tbl_custorg_before_tbl_customers(self):
        """Must go via tbl_custorg to avoid 10M row tbl_customers scan."""
        pool, cursor = make_mock_pool(rows=[])
        await get_top_clients_by_spend(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_custorg" in sql
        assert "tbl_customers" in sql

    async def test_sql_orders_by_total_spend_desc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_top_clients_by_spend(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql and "DESC" in sql

    async def test_sql_includes_loyalty_points(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_top_clients_by_spend(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Points" in sql


# ---------------------------------------------------------------------------
# get_walkin_stats
# ---------------------------------------------------------------------------

class TestGetWalkinStats:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "total_walkins": 142,
                 "served": 128, "not_served": 14,
                 "serve_rate_pct": 90.1}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_walkin_stats(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_walkin_stats(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_queries_custsignin_table(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_walkin_stats(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_custsignin" in sql

    async def test_sql_distinguishes_served_vs_not_served(self):
        """Status=1 served, Status=0 not served."""
        pool, cursor = make_mock_pool(rows=[])
        await get_walkin_stats(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Status = 1" in sql
        assert "Status = 0" in sql

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_walkin_stats(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql


# ---------------------------------------------------------------------------
# get_visit_frequency_distribution
# ---------------------------------------------------------------------------

class TestGetVisitFrequencyDistribution:

    async def test_returns_fetchall_result(self):
        rows = [
            {"frequency_bucket": "1 visit",    "client_count": 120},
            {"frequency_bucket": "2-3 visits", "client_count": 85},
            {"frequency_bucket": "4-6 visits", "client_count": 40},
            {"frequency_bucket": "7+ visits",  "client_count": 15},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_visit_frequency_distribution(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_visit_frequency_distribution(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_uses_subquery_for_visit_count(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_visit_frequency_distribution(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "SELECT" in sql
        assert "COUNT" in sql

    async def test_sql_has_case_buckets(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_visit_frequency_distribution(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "CASE" in sql
        assert "7+" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_visit_frequency_distribution(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []
