"""
Tests for queries/forms.py
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.queries.forms import (
    get_form_catalog,
    get_form_completion_summary,
    get_pending_forms,
    get_form_completions_by_month,
    get_client_form_status,
)

CUST_ID = 441


# ---------------------------------------------------------------------------
# get_form_catalog
# ---------------------------------------------------------------------------

class TestGetFormCatalog:

    async def test_returns_fetchall_result(self):
        rows = [{"form_id": 4, "name": "New Client Intake",
                 "description": "Standard form", "category_id": 1,
                 "active": 1, "created_date": "2025-01-15"}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_form_catalog(pool, ORG_ID)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_catalog(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_queries_tbl_form(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_catalog(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_form" in sql

    async def test_sql_filters_org_with_orgid_column(self):
        """tbl_form uses OrgId (capital I)."""
        pool, cursor = make_mock_pool(rows=[])
        await get_form_catalog(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "OrgId" in sql

    async def test_active_only_true_adds_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_catalog(pool, ORG_ID, active_only=True)
        sql = cursor.execute.call_args[0][0]
        assert "Active = 1" in sql

    async def test_active_only_false_omits_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_catalog(pool, ORG_ID, active_only=False)
        sql = cursor.execute.call_args[0][0]
        assert "AND Active = 1" not in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_form_catalog(pool, ORG_ID)
        assert result == []


# ---------------------------------------------------------------------------
# get_form_completion_summary
# ---------------------------------------------------------------------------

class TestGetFormCompletionSummary:

    async def test_returns_fetchall_result(self):
        rows = [{"form_id": 4, "form_name": "New Client Intake",
                 "total_assigned": 42, "ready_count": 8,
                 "complete_count": 28, "approved_count": 6,
                 "completion_rate_pct": 80.9}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_form_completion_summary(pool, ORG_ID)
        assert result == rows

    async def test_passes_org_id_twice(self):
        """org_id used in both the JOIN ON clause and the WHERE clause."""
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completion_summary(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params.count(ORG_ID) == 2

    async def test_sql_uses_lowercase_orgid_for_formcust(self):
        """tbl_formcust uses Orgid (lowercase i) — critical exact case."""
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completion_summary(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "fc.Orgid" in sql

    async def test_sql_uses_uppercase_orgid_for_form(self):
        """tbl_form uses OrgId (capital I)."""
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completion_summary(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "f.OrgId" in sql

    async def test_sql_left_joins_formcust(self):
        """LEFT JOIN so forms with zero submissions still appear."""
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completion_summary(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "LEFT JOIN" in sql
        assert "tbl_formcust" in sql

    async def test_sql_counts_all_three_statuses(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completion_summary(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "'ready'" in sql
        assert "'complete'" in sql
        assert "'approved'" in sql

    async def test_sql_computes_completion_rate(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completion_summary(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "NULLIF" in sql or "100.0" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_form_completion_summary(pool, ORG_ID)
        assert result == []


# ---------------------------------------------------------------------------
# get_pending_forms
# ---------------------------------------------------------------------------

class TestGetPendingForms:

    async def test_returns_fetchall_result(self):
        rows = [{"formcust_id": 22, "form_id": 4,
                 "form_name": "New Client Intake", "cust_id": 441,
                 "status": "ready", "assigned_date": "2026-03-01"}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_pending_forms(pool, ORG_ID)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_pending_forms(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_default_limit_is_50(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_pending_forms(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[-1] == 50

    async def test_custom_limit(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_pending_forms(pool, ORG_ID, limit=10)
        params = cursor.execute.call_args[0][1]
        assert params[-1] == 10

    async def test_sql_filters_status_ready(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_pending_forms(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "'ready'" in sql

    async def test_sql_uses_lowercase_orgid(self):
        """tbl_formcust uses Orgid (lowercase i)."""
        pool, cursor = make_mock_pool(rows=[])
        await get_pending_forms(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "fc.Orgid" in sql or "Orgid" in sql

    async def test_sql_joins_tbl_form_for_name(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_pending_forms(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_form" in sql
        assert "JOIN" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_pending_forms(pool, ORG_ID)
        assert result == []


# ---------------------------------------------------------------------------
# get_form_completions_by_month
# ---------------------------------------------------------------------------

class TestGetFormCompletionsByMonth:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "completed": 18,
                 "approved": 12, "total": 30}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_form_completions_by_month(
            pool, ORG_ID, FROM_DATE, TO_DATE
        )
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completions_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completions_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_sql_filters_complete_and_approved_only(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completions_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "'complete'" in sql
        assert "'approved'" in sql

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completions_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql

    async def test_sql_uses_lowercase_orgid(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_form_completions_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Orgid" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_form_completions_by_month(
            pool, ORG_ID, FROM_DATE, TO_DATE
        )
        assert result == []


# ---------------------------------------------------------------------------
# get_client_form_status
# ---------------------------------------------------------------------------

class TestGetClientFormStatus:

    async def test_returns_fetchall_result(self):
        rows = [{"form_id": 4, "form_name": "New Client Intake",
                 "status": "complete", "assigned_date": "2026-03-01"}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_client_form_status(pool, ORG_ID, CUST_ID)
        assert result == rows

    async def test_passes_org_id_and_cust_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_client_form_status(pool, ORG_ID, CUST_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID
        assert params[1] == CUST_ID

    async def test_sql_filters_by_cust_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_client_form_status(pool, ORG_ID, CUST_ID)
        sql = cursor.execute.call_args[0][0]
        assert "CustId" in sql

    async def test_sql_uses_lowercase_orgid(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_client_form_status(pool, ORG_ID, CUST_ID)
        sql = cursor.execute.call_args[0][0]
        assert "Orgid" in sql

    async def test_sql_joins_tbl_form_for_name(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_client_form_status(pool, ORG_ID, CUST_ID)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_form" in sql

    async def test_sql_orders_by_recdate_desc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_client_form_status(pool, ORG_ID, CUST_ID)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql and "DESC" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_client_form_status(pool, ORG_ID, CUST_ID)
        assert result == []
