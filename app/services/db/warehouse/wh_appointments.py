"""Warehouse appointment metrics: wh_appointment_metrics."""

from __future__ import annotations

from datetime import date


async def get_appointment_monthly_summary(
    pool, org_id: int, period_start: date | str
) -> dict | None:
    """Single month appointment funnel, all locations combined (location_id=0)."""
    sql = """
        SELECT * FROM wh_appointment_metrics
        WHERE business_id = $1 AND location_id = 0 AND period_start = $2
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, org_id, period_start)
    return dict(row) if row else None


async def get_appointment_trend(pool, org_id: int, months: int = 6) -> list[dict]:
    """Last N months of appointment KPIs, location_id=0, newest first."""
    sql = """
        SELECT * FROM wh_appointment_metrics
        WHERE business_id = $1 AND location_id = 0
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_cancellation_rate_trend(
    pool, org_id: int, months: int = 6
) -> list[dict]:
    """Last N months of cancellation_rate + completion_rate only."""
    sql = """
        SELECT period_start, cancellation_rate, completion_rate
        FROM wh_appointment_metrics
        WHERE business_id = $1 AND location_id = 0
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_walkin_vs_booked_trend(
    pool, org_id: int, months: int = 6
) -> list[dict]:
    """Last N months of walkin_count vs app_booking_count."""
    sql = """
        SELECT period_start, walkin_count, app_booking_count, total_booked
        FROM wh_appointment_metrics
        WHERE business_id = $1 AND location_id = 0
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_location_appointment_comparison(
    pool, org_id: int, period_start: date | str
) -> list[dict]:
    """All locations for a month, ordered by total_booked DESC."""
    sql = """
        SELECT * FROM wh_appointment_metrics
        WHERE business_id = $1 AND period_start = $2
        ORDER BY total_booked DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start)
    return [dict(r) for r in rows]
