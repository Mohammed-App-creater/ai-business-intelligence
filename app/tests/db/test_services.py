"""
Tests for queries/services.py
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, service_rows, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.queries.services import (
    get_service_popularity,
    get_service_popularity_trend,
    get_services_by_staff,
    get_walkin_service_demand,
    get_service_catalog,
    get_service_inventory,
)

EMP_ID     = 12
SERVICE_ID = 5


# ---------------------------------------------------------------------------
# get_service_popularity
# ---------------------------------------------------------------------------

class TestGetServicePopularity:

    async def test_returns_fetchall_result(self):
        rows = service_rows(3)
        pool, _ = make_mock_pool(rows=rows)
        result = await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id_as_first_param(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_default_limit_is_20(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[3] == 20

    async def test_custom_limit_passed(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE, limit=5)
        params = cursor.execute.call_args[0][1]
        assert params[3] == 5

    async def test_sql_joins_via_tbl_visit_for_org_scoping(self):
        """tbl_service_visit has no OrgId — must join tbl_visit for tenant isolation."""
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_visit" in sql
        assert "OrganizationId" in sql

    async def test_sql_joins_tbl_service(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_service" in sql

    async def test_sql_filters_successful_payments(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql

    async def test_sql_orders_by_booking_count_desc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql
        assert "DESC" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []

    async def test_returns_list(self):
        pool, _ = make_mock_pool(rows=service_rows(1))
        result = await get_service_popularity(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# get_service_popularity_trend
# ---------------------------------------------------------------------------

class TestGetServicePopularityTrend:

    async def test_returns_fetchall_result(self):
        rows = [
            {"month": "2026-01", "booking_count": 35, "revenue": 4200.0},
            {"month": "2026-02", "booking_count": 40, "revenue": 4800.0},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_service_popularity_trend(
            pool, ORG_ID, SERVICE_ID, FROM_DATE, TO_DATE
        )
        assert result == rows

    async def test_passes_org_id_and_service_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity_trend(pool, ORG_ID, SERVICE_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID
        assert params[1] == SERVICE_ID

    async def test_sql_filters_by_service_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity_trend(pool, ORG_ID, SERVICE_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ServiceID" in sql

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity_trend(pool, ORG_ID, SERVICE_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql

    async def test_sql_joins_tbl_visit_for_org_scoping(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_popularity_trend(pool, ORG_ID, SERVICE_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_visit" in sql
        assert "OrganizationId" in sql


# ---------------------------------------------------------------------------
# get_services_by_staff
# ---------------------------------------------------------------------------

class TestGetServicesByStaff:

    async def test_returns_fetchall_result(self):
        rows = [
            {"service_id": 5, "service_name": "Balayage", "booking_count": 38, "revenue": 4560.0},
            {"service_id": 3, "service_name": "Haircut",  "booking_count": 25, "revenue": 1250.0},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_services_by_staff(pool, ORG_ID, EMP_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id_and_emp_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_services_by_staff(pool, ORG_ID, EMP_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID
        assert params[1] == EMP_ID

    async def test_sql_filters_by_emp_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_services_by_staff(pool, ORG_ID, EMP_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "EmpID" in sql

    async def test_sql_joins_tbl_service(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_services_by_staff(pool, ORG_ID, EMP_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_service" in sql

    async def test_sql_joins_tbl_visit_for_org_scoping(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_services_by_staff(pool, ORG_ID, EMP_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_visit" in sql


# ---------------------------------------------------------------------------
# get_walkin_service_demand
# ---------------------------------------------------------------------------

class TestGetWalkinServiceDemand:

    async def test_returns_fetchall_result(self):
        rows = [
            {"service_id": 3, "service_name": "Haircut", "request_count": 87},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_walkin_service_demand(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_walkin_service_demand(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_default_limit_is_10(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_walkin_service_demand(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[-1] == 10

    async def test_sql_joins_tbl_custsignin_for_org_scoping(self):
        """tbl_signinservice has no OrgId — must join tbl_custsignin."""
        pool, cursor = make_mock_pool(rows=[])
        await get_walkin_service_demand(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_custsignin" in sql
        assert "OrgId" in sql

    async def test_sql_joins_tbl_service(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_walkin_service_demand(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_service" in sql


# ---------------------------------------------------------------------------
# get_service_catalog
# ---------------------------------------------------------------------------

class TestGetServiceCatalog:

    async def test_returns_fetchall_result(self):
        rows = [
            {"service_id": 3, "name": "Haircut", "price": 35.0, "duration": 30,
             "category_id": 1, "active": 1},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_service_catalog(pool, ORG_ID)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_catalog(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_active_only_true_adds_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_catalog(pool, ORG_ID, active_only=True)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql

    async def test_active_only_false_omits_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_catalog(pool, ORG_ID, active_only=False)
        sql = cursor.execute.call_args[0][0]
        # Active filter should be absent when active_only=False
        # (the clause "AND Active = 1" should not be in the SQL)
        assert "AND Active = 1" not in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_service_catalog(pool, ORG_ID)
        assert result == []


# ---------------------------------------------------------------------------
# get_service_inventory
# ---------------------------------------------------------------------------

class TestGetServiceInventory:

    async def test_returns_fetchall_result(self):
        rows = [
            {"service_id": 5, "service_name": "Balayage", "location_id": 1, "quantity": 12},
        ]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_service_inventory(pool, ORG_ID)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_inventory(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_joins_tbl_service(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_inventory(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_service" in sql

    async def test_sql_filters_active_services(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_service_inventory(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql
