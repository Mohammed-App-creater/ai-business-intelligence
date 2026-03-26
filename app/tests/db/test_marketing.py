"""
Tests for queries/marketing.py
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, FROM_DATE, TO_DATE

from app.services.db.queries.marketing import (
    get_campaigns,
    get_campaign_performance,
    get_campaign_summary,
    get_active_campaigns,
    get_monthly_campaign_volume,
)

TENANT_ID = 42


# ---------------------------------------------------------------------------
# get_campaigns
# ---------------------------------------------------------------------------

class TestGetCampaigns:

    async def test_returns_fetchall_result(self):
        rows = [{"id": 12, "name": "Summer Promo", "status": "completed",
                 "start_date": "2026-06-01", "expiration_date": "2026-06-30",
                 "emails_count": 500, "promo_code": "SUMMER20",
                 "recurring": 0, "active": 1}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_campaigns(pool, TENANT_ID)
        assert result == rows

    async def test_passes_tenant_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaigns(pool, TENANT_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == TENANT_ID

    async def test_sql_uses_tenant_id_not_org_id(self):
        """tbl_mrkcampaign uses TenantID — not OrganizationId."""
        pool, cursor = make_mock_pool(rows=[])
        await get_campaigns(pool, TENANT_ID)
        sql = cursor.execute.call_args[0][0]
        assert "TenantID" in sql
        assert "OrganizationId" not in sql

    async def test_sql_excludes_deleted_campaigns(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaigns(pool, TENANT_ID)
        sql = cursor.execute.call_args[0][0]
        assert "Delete" in sql

    async def test_active_only_false_by_default(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaigns(pool, TENANT_ID, active_only=False)
        sql = cursor.execute.call_args[0][0]
        assert "AND Active = 1" not in sql

    async def test_active_only_true_adds_filter(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaigns(pool, TENANT_ID, active_only=True)
        sql = cursor.execute.call_args[0][0]
        assert "Active = 1" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_campaigns(pool, TENANT_ID)
        assert result == []


# ---------------------------------------------------------------------------
# get_campaign_performance
# ---------------------------------------------------------------------------

class TestGetCampaignPerformance:

    async def test_returns_fetchall_result(self):
        rows = [{"campaign_id": 12, "campaign_name": "Summer Promo",
                 "execution_date": "2026-06-01", "total": 500,
                 "delivered": 480, "failed": 20, "opened": 144,
                 "clicked": 48, "open_rate_pct": 30.0,
                 "click_rate_pct": 10.0, "delivery_rate_pct": 96.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_campaign_performance(pool, TENANT_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_tenant_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaign_performance(pool, TENANT_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == TENANT_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaign_performance(pool, TENANT_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_sql_joins_executecampaign_to_mrkcampaign(self):
        """tbl_executecampaign has no TenantID — must join tbl_mrkcampaign."""
        pool, cursor = make_mock_pool(rows=[])
        await get_campaign_performance(pool, TENANT_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_executecampaign" in sql
        assert "tbl_mrkcampaign" in sql
        assert "JOIN" in sql

    async def test_sql_computes_open_rate(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaign_performance(pool, TENANT_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Opened" in sql
        assert "NULLIF" in sql

    async def test_sql_computes_click_rate(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaign_performance(pool, TENANT_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Clicked" in sql

    async def test_sql_excludes_deleted(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaign_performance(pool, TENANT_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "Delete" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_campaign_performance(pool, TENANT_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_campaign_summary
# ---------------------------------------------------------------------------

class TestGetCampaignSummary:

    async def test_returns_fetchall_result(self):
        rows = [{"campaign_id": 12, "campaign_name": "Summer Promo",
                 "status": "completed", "execution_count": 3,
                 "total_sent": 1500, "total_delivered": 1440,
                 "total_opened": 432, "total_clicked": 144,
                 "avg_open_rate_pct": 30.0, "avg_click_rate_pct": 10.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_campaign_summary(pool, TENANT_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_tenant_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaign_summary(pool, TENANT_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == TENANT_ID

    async def test_sql_groups_by_campaign(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaign_summary(pool, TENANT_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql
        assert "m.id" in sql or "CampaignId" in sql

    async def test_sql_aggregates_across_executions(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaign_summary(pool, TENANT_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "SUM(" in sql
        assert "COUNT(" in sql

    async def test_sql_orders_by_total_opened_desc(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_campaign_summary(pool, TENANT_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY" in sql and "DESC" in sql


# ---------------------------------------------------------------------------
# get_active_campaigns
# ---------------------------------------------------------------------------

class TestGetActiveCampaigns:

    async def test_returns_fetchall_result(self):
        rows = [{"id": 12, "name": "Summer Promo", "status": "ready",
                 "promo_code": "SUMMER20", "start_date": "2026-06-01",
                 "expiration_date": "2026-06-30", "recurring": 0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_active_campaigns(pool, TENANT_ID)
        assert result == rows

    async def test_passes_tenant_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_active_campaigns(pool, TENANT_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == TENANT_ID

    async def test_sql_filters_active(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_active_campaigns(pool, TENANT_ID)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql and "= 1" in sql

    async def test_sql_filters_not_expired(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_active_campaigns(pool, TENANT_ID)
        sql = cursor.execute.call_args[0][0]
        assert "ExpirationDate" in sql
        assert "CURDATE()" in sql

    async def test_sql_excludes_deleted(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_active_campaigns(pool, TENANT_ID)
        sql = cursor.execute.call_args[0][0]
        assert "Delete" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_active_campaigns(pool, TENANT_ID)
        assert result == []


# ---------------------------------------------------------------------------
# get_monthly_campaign_volume
# ---------------------------------------------------------------------------

class TestGetMonthlyCampaignVolume:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "campaigns_run": 4,
                 "total_sent": 2000, "total_opened": 600,
                 "total_clicked": 200, "avg_open_rate_pct": 30.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_monthly_campaign_volume(pool, TENANT_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_tenant_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_campaign_volume(pool, TENANT_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == TENANT_ID

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_campaign_volume(pool, TENANT_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql
        assert "%%Y-%%m" in sql or "%Y-%m" in sql

    async def test_sql_counts_distinct_campaigns(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_campaign_volume(pool, TENANT_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "DISTINCT" in sql

    async def test_sql_joins_mrkcampaign_for_tenant_scoping(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_monthly_campaign_volume(pool, TENANT_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_mrkcampaign" in sql
        assert "TenantID" in sql
