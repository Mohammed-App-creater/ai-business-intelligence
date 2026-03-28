"""Warehouse subscription revenue: wh_subscription_revenue."""

from __future__ import annotations

from datetime import date


async def get_subscription_monthly_summary(
    pool, org_id: int, period_start: date | str
) -> dict | None:
    """Single month subscription KPIs (location_id=0)."""
    sql = """
        SELECT * FROM wh_subscription_revenue
        WHERE business_id = $1 AND location_id = 0 AND period_start = $2
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, org_id, period_start)
    return dict(row) if row else None


async def get_subscription_trend(pool, org_id: int, months: int = 6) -> list[dict]:
    """Last N months of subscription revenue and counts, newest first."""
    sql = """
        SELECT * FROM wh_subscription_revenue
        WHERE business_id = $1 AND location_id = 0
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_mrr_trend(pool, org_id: int, months: int = 6) -> list[dict]:
    """MRR proxy: net_subscription_revenue per month."""
    sql = """
        SELECT period_start, net_subscription_revenue, active_subscriptions
        FROM wh_subscription_revenue
        WHERE business_id = $1 AND location_id = 0
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_subscription_growth(pool, org_id: int, months: int = 6) -> list[dict]:
    """New vs cancelled subscriptions per month."""
    sql = """
        SELECT
            period_start,
            new_subscriptions,
            cancelled_subscriptions,
            active_subscriptions
        FROM wh_subscription_revenue
        WHERE business_id = $1 AND location_id = 0
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]
