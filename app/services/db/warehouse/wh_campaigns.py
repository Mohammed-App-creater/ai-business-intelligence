"""Warehouse campaign performance: wh_campaign_performance."""

from __future__ import annotations

from datetime import date


async def get_campaign_history(pool, org_id: int, limit: int = 20) -> list[dict]:
    """All campaigns ordered by execution_date DESC."""
    sql = """
        SELECT * FROM wh_campaign_performance
        WHERE business_id = $1
        ORDER BY execution_date DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, limit)
    return [dict(r) for r in rows]


async def get_campaign_detail(pool, org_id: int, campaign_id: int) -> list[dict]:
    """All executions for a single campaign, ordered by execution_date DESC."""
    sql = """
        SELECT * FROM wh_campaign_performance
        WHERE business_id = $1 AND campaign_id = $2
        ORDER BY execution_date DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, campaign_id)
    return [dict(r) for r in rows]


async def get_top_campaigns_by_open_rate(
    pool, org_id: int, limit: int = 10
) -> list[dict]:
    """Top N campaigns by open_rate."""
    sql = """
        SELECT * FROM wh_campaign_performance
        WHERE business_id = $1
        ORDER BY open_rate DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, limit)
    return [dict(r) for r in rows]


async def get_campaign_monthly_summary(
    pool, org_id: int, period_start: date | str
) -> list[dict]:
    """All campaigns executed in a given month."""
    sql = """
        SELECT * FROM wh_campaign_performance
        WHERE business_id = $1
          AND execution_date >= $2
          AND execution_date < ($2::date + INTERVAL '1 month')
        ORDER BY execution_date DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start)
    return [dict(r) for r in rows]


async def get_recurring_campaigns(pool, org_id: int) -> list[dict]:
    """Recurring campaigns (is_recurring=TRUE), most recent execution first."""
    sql = """
        SELECT * FROM wh_campaign_performance
        WHERE business_id = $1 AND is_recurring = TRUE
        ORDER BY execution_date DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id)
    return [dict(r) for r in rows]
