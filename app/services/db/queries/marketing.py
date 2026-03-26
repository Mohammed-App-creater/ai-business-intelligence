"""
queries/marketing.py
====================
Marketing campaign queries (Production DB).

Tables used:
  tbl_mrkcampaign       — campaign list per tenant
  tbl_executecampaign   — execution results per campaign run
  tbl_mrkpricing        — pricing tiers per tenant

CRITICAL scoping notes:
  tbl_mrkcampaign   uses TenantID (NOT OrganizationId) for org scoping.
  tbl_executecampaign has NO TenantID/OrgId — must join tbl_mrkcampaign
  on CampaignId to scope to a tenant.

tbl_mrkcampaign.Status values: 'completed', 'pending', 'ready', 'Delete'
tbl_mrkcampaign.Recurring: 0 = one-time, 1 = recurring

tbl_executecampaign columns:
  Total     — total recipients targeted
  Successed — successfully delivered
  Failed    — delivery failures
  Opened    — emails opened
  Clicked   — links clicked
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Campaign list for a tenant
# ---------------------------------------------------------------------------

async def get_campaigns(
    pool,
    tenant_id: int,
    active_only: bool = False,
) -> list[dict[str, Any]]:
    """
    All campaigns for a tenant with their status and basic metadata.

    Returns rows like:
        {
          "id": 12,
          "name": "Summer Promo",
          "status": "completed",
          "start_date": "2026-06-01",
          "expiration_date": "2026-06-30",
          "emails_count": 500,
          "promo_code": "SUMMER20",
          "recurring": 0,
          "active": 1
        }
    """
    active_clause = "AND Active = 1" if active_only else ""
    sql = f"""
        SELECT
            id               AS id,
            Name             AS name,
            Status           AS status,
            StartDate        AS start_date,
            ExpirationDate   AS expiration_date,
            EmailsCampaignCount AS emails_count,
            PromoCode        AS promo_code,
            Recurring        AS recurring,
            Active           AS active
        FROM tbl_mrkcampaign
        WHERE TenantID = %s
          AND Status  != 'Delete'
          {active_clause}
        ORDER BY StartDate DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (tenant_id,))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Campaign performance (execution results)
# ---------------------------------------------------------------------------

async def get_campaign_performance(
    pool,
    tenant_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Execution results per campaign — delivery, open rate, click rate.
    Joins tbl_executecampaign to tbl_mrkcampaign for tenant scoping.

    Returns rows like:
        {
          "campaign_id": 12,
          "campaign_name": "Summer Promo",
          "execution_date": "2026-06-01",
          "total": 500,
          "delivered": 480,
          "failed": 20,
          "opened": 144,
          "clicked": 48,
          "open_rate_pct": 30.0,
          "click_rate_pct": 10.0,
          "delivery_rate_pct": 96.0
        }
    """
    sql = """
        SELECT
            m.id                                       AS campaign_id,
            m.Name                                     AS campaign_name,
            e.ExecutionDate                            AS execution_date,
            e.Total                                    AS total,
            e.Successed                                AS delivered,
            e.Failed                                   AS failed,
            e.Opened                                   AS opened,
            e.Clicked                                  AS clicked,
            ROUND(
                e.Opened * 100.0 / NULLIF(e.Successed, 0), 1
            )                                          AS open_rate_pct,
            ROUND(
                e.Clicked * 100.0 / NULLIF(e.Successed, 0), 1
            )                                          AS click_rate_pct,
            ROUND(
                e.Successed * 100.0 / NULLIF(e.Total, 0), 1
            )                                          AS delivery_rate_pct
        FROM tbl_executecampaign e
        JOIN tbl_mrkcampaign m ON e.CampaignId = m.id
        WHERE m.TenantID      = %s
          AND e.ExecutionDate BETWEEN %s AND %s
          AND m.Status       != 'Delete'
        ORDER BY e.ExecutionDate DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (tenant_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Campaign summary — aggregated across all executions
# ---------------------------------------------------------------------------

async def get_campaign_summary(
    pool,
    tenant_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Per-campaign totals aggregated across all execution runs in a period.
    Best for "which campaign performed best?" questions.

    Returns rows like:
        {
          "campaign_id": 12,
          "campaign_name": "Summer Promo",
          "execution_count": 3,
          "total_sent": 1500,
          "total_delivered": 1440,
          "total_opened": 432,
          "total_clicked": 144,
          "avg_open_rate_pct": 30.0,
          "avg_click_rate_pct": 10.0
        }
    """
    sql = """
        SELECT
            m.id                                           AS campaign_id,
            m.Name                                         AS campaign_name,
            m.Status                                       AS status,
            COUNT(e.id)                                    AS execution_count,
            SUM(e.Total)                                   AS total_sent,
            SUM(e.Successed)                               AS total_delivered,
            SUM(e.Opened)                                  AS total_opened,
            SUM(e.Clicked)                                 AS total_clicked,
            ROUND(
                SUM(e.Opened) * 100.0 / NULLIF(SUM(e.Successed), 0), 1
            )                                              AS avg_open_rate_pct,
            ROUND(
                SUM(e.Clicked) * 100.0 / NULLIF(SUM(e.Successed), 0), 1
            )                                              AS avg_click_rate_pct
        FROM tbl_executecampaign e
        JOIN tbl_mrkcampaign m ON e.CampaignId = m.id
        WHERE m.TenantID      = %s
          AND e.ExecutionDate BETWEEN %s AND %s
          AND m.Status       != 'Delete'
        GROUP BY m.id, m.Name, m.Status
        ORDER BY total_opened DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (tenant_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Active campaigns (not expired, not deleted)
# ---------------------------------------------------------------------------

async def get_active_campaigns(
    pool,
    tenant_id: int,
) -> list[dict[str, Any]]:
    """
    Currently active campaigns that have not expired.
    Used for LOOKUP route ("what promos are we running right now?").

    Returns rows like:
        {
          "id": 12,
          "name": "Summer Promo",
          "status": "ready",
          "promo_code": "SUMMER20",
          "start_date": "2026-06-01",
          "expiration_date": "2026-06-30",
          "recurring": 0
        }
    """
    sql = """
        SELECT
            id              AS id,
            Name            AS name,
            Status          AS status,
            PromoCode       AS promo_code,
            StartDate       AS start_date,
            ExpirationDate  AS expiration_date,
            Recurring       AS recurring
        FROM tbl_mrkcampaign
        WHERE TenantID      = %s
          AND Active        = 1
          AND Status       != 'Delete'
          AND (ExpirationDate IS NULL OR ExpirationDate >= CURDATE())
        ORDER BY StartDate DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (tenant_id,))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Monthly campaign volume
# ---------------------------------------------------------------------------

async def get_monthly_campaign_volume(
    pool,
    tenant_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Monthly totals across all campaigns — how many emails went out,
    how many were opened, how many clicked.

    Returns rows like:
        {
          "month": "2026-03",
          "campaigns_run": 4,
          "total_sent": 2000,
          "total_opened": 600,
          "total_clicked": 200,
          "avg_open_rate_pct": 30.0
        }
    """
    sql = """
        SELECT
            DATE_FORMAT(e.ExecutionDate, '%%Y-%%m')    AS month,
            COUNT(DISTINCT e.CampaignId)               AS campaigns_run,
            SUM(e.Total)                               AS total_sent,
            SUM(e.Opened)                              AS total_opened,
            SUM(e.Clicked)                             AS total_clicked,
            ROUND(
                SUM(e.Opened) * 100.0 / NULLIF(SUM(e.Successed), 0), 1
            )                                          AS avg_open_rate_pct
        FROM tbl_executecampaign e
        JOIN tbl_mrkcampaign m ON e.CampaignId = m.id
        WHERE m.TenantID      = %s
          AND e.ExecutionDate BETWEEN %s AND %s
          AND m.Status       != 'Delete'
        GROUP BY DATE_FORMAT(e.ExecutionDate, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (tenant_id, from_date, to_date))
            return await cur.fetchall()
