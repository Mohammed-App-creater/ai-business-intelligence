"""Warehouse revenue queries: wh_monthly_revenue, wh_daily_revenue."""

from __future__ import annotations

from datetime import date


async def get_monthly_trend(pool, org_id: int, months: int = 6) -> list[dict]:
    """Last N months of revenue KPIs, ordered newest first."""
    sql = """
        SELECT * FROM wh_monthly_revenue
        WHERE business_id = $1 AND location_id = 0
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_monthly_by_location(
    pool, org_id: int, period_start: date | str, location_id: int
) -> dict | None:
    """Single month revenue for a specific location."""
    sql = """
        SELECT * FROM wh_monthly_revenue
        WHERE business_id = $1 AND location_id = $2 AND period_start = $3
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, org_id, location_id, period_start)
    return dict(row) if row else None


async def get_revenue_comparison(
    pool, org_id: int, period_a: date | str, period_b: date | str
) -> list[dict]:
    """Two months side-by-side for MoM comparison."""
    sql = """
        SELECT * FROM wh_monthly_revenue
        WHERE business_id = $1 AND location_id = 0
          AND period_start IN ($2, $3)
        ORDER BY period_start
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_a, period_b)
    return [dict(r) for r in rows]


async def get_daily_trend(
    pool, org_id: int, from_date: date | str, to_date: date | str
) -> list[dict]:
    """Daily revenue between two dates, ordered oldest first."""
    sql = """
        SELECT * FROM wh_daily_revenue
        WHERE business_id = $1 AND location_id = 0
          AND revenue_date BETWEEN $2 AND $3
        ORDER BY revenue_date ASC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, from_date, to_date)
    return [dict(r) for r in rows]


async def get_best_revenue_days(pool, org_id: int, limit: int = 10) -> list[dict]:
    """Top N days by gross_revenue."""
    sql = """
        SELECT * FROM wh_daily_revenue
        WHERE business_id = $1
        ORDER BY gross_revenue DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, limit)
    return [dict(r) for r in rows]


async def get_location_revenue_summary(
    pool, org_id: int, period_start: date | str
) -> list[dict]:
    """All locations for a given month, ordered by gross_revenue DESC."""
    sql = """
        SELECT * FROM wh_monthly_revenue
        WHERE business_id = $1 AND period_start = $2
        ORDER BY gross_revenue DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start)
    return [dict(r) for r in rows]
