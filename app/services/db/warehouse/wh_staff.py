"""Warehouse staff queries: wh_staff_performance."""

from __future__ import annotations

from datetime import date


async def get_staff_monthly_performance(
    pool, org_id: int, period_start: date | str
) -> list[dict]:
    """All staff KPIs for a given month, ordered by total_revenue DESC."""
    sql = """
        SELECT * FROM wh_staff_performance
        WHERE business_id = $1 AND period_start = $2
        ORDER BY total_revenue DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start)
    return [dict(r) for r in rows]


async def get_staff_trend(
    pool, org_id: int, employee_id: int, months: int = 6
) -> list[dict]:
    """Single staff member's KPIs over last N months, newest first."""
    sql = """
        SELECT * FROM wh_staff_performance
        WHERE business_id = $1 AND employee_id = $2
        ORDER BY period_start DESC
        LIMIT $3
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, employee_id, months)
    return [dict(r) for r in rows]


async def get_top_performers(
    pool, org_id: int, period_start: date | str, limit: int = 5
) -> list[dict]:
    """Top N staff by total_revenue for a given month."""
    sql = """
        SELECT * FROM wh_staff_performance
        WHERE business_id = $1 AND period_start = $2
        ORDER BY total_revenue DESC
        LIMIT $3
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start, limit)
    return [dict(r) for r in rows]


async def get_staff_rating_ranking(
    pool, org_id: int, period_start: date | str
) -> list[dict]:
    """Staff ranked by avg_rating DESC for a given month (excludes NULLs)."""
    sql = """
        SELECT * FROM wh_staff_performance
        WHERE business_id = $1 AND period_start = $2
          AND avg_rating IS NOT NULL
        ORDER BY avg_rating DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start)
    return [dict(r) for r in rows]


async def get_staff_utilisation(
    pool, org_id: int, period_start: date | str
) -> list[dict]:
    """Staff ordered by utilisation_rate DESC."""
    sql = """
        SELECT * FROM wh_staff_performance
        WHERE business_id = $1 AND period_start = $2
        ORDER BY utilisation_rate DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start)
    return [dict(r) for r in rows]


async def get_underperforming_staff(
    pool, org_id: int, period_start: date | str, min_visits: int = 1
) -> list[dict]:
    """Staff with total_visits >= min_visits but below-average revenue."""
    sql = """
        WITH avg_rev AS (
            SELECT AVG(total_revenue) AS avg_revenue
            FROM wh_staff_performance
            WHERE business_id = $1 AND period_start = $2
              AND total_visits >= $3
        )
        SELECT sp.* FROM wh_staff_performance sp, avg_rev
        WHERE sp.business_id = $1 AND sp.period_start = $2
          AND sp.total_visits >= $3
          AND sp.total_revenue < avg_rev.avg_revenue
        ORDER BY sp.total_revenue ASC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start, min_visits)
    return [dict(r) for r in rows]
