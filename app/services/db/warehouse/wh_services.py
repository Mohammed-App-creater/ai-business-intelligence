"""Warehouse service queries: wh_service_performance."""

from __future__ import annotations

from datetime import date


async def get_service_monthly_performance(
    pool, org_id: int, period_start: date | str
) -> list[dict]:
    """All services for a given month, ordered by booking_count DESC."""
    sql = """
        SELECT * FROM wh_service_performance
        WHERE business_id = $1 AND period_start = $2
        ORDER BY booking_count DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start)
    return [dict(r) for r in rows]


async def get_top_services(
    pool, org_id: int, period_start: date | str, limit: int = 10
) -> list[dict]:
    """Top N services by booking_count for a month."""
    sql = """
        SELECT * FROM wh_service_performance
        WHERE business_id = $1 AND period_start = $2
        ORDER BY booking_count DESC
        LIMIT $3
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start, limit)
    return [dict(r) for r in rows]


async def get_service_trend(
    pool, org_id: int, service_id: int, months: int = 6
) -> list[dict]:
    """Single service KPIs over last N months, newest first."""
    sql = """
        SELECT * FROM wh_service_performance
        WHERE business_id = $1 AND service_id = $2
        ORDER BY period_start DESC
        LIMIT $3
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, service_id, months)
    return [dict(r) for r in rows]


async def get_service_revenue_ranking(
    pool, org_id: int, period_start: date | str
) -> list[dict]:
    """Services ordered by revenue DESC for a month."""
    sql = """
        SELECT * FROM wh_service_performance
        WHERE business_id = $1 AND period_start = $2
        ORDER BY revenue DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start)
    return [dict(r) for r in rows]


async def get_declining_services(
    pool,
    org_id: int,
    current_period: date | str,
    prev_period: date | str,
) -> list[dict]:
    """Services where booking_count dropped from prev_period to current_period."""
    sql = """
        SELECT
            curr.service_id,
            curr.service_name,
            prev.booking_count AS prev_bookings,
            curr.booking_count AS curr_bookings,
            (curr.booking_count - prev.booking_count) AS change
        FROM wh_service_performance curr
        INNER JOIN wh_service_performance prev
            ON curr.business_id = prev.business_id
           AND curr.service_id = prev.service_id
        WHERE curr.business_id = $1
          AND curr.period_start = $2
          AND prev.period_start = $3
          AND curr.booking_count < prev.booking_count
        ORDER BY change ASC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, current_period, prev_period)
    return [dict(r) for r in rows]
