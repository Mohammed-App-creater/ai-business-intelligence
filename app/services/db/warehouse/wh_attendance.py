"""Warehouse attendance summary: wh_attendance_summary."""

from __future__ import annotations

from datetime import date


async def get_staff_attendance_monthly(
    pool, org_id: int, period_start: date | str
) -> list[dict]:
    """All staff attendance for a month, ordered by total_hours_worked DESC."""
    sql = """
        SELECT * FROM wh_attendance_summary
        WHERE business_id = $1 AND period_start = $2
        ORDER BY total_hours_worked DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start)
    return [dict(r) for r in rows]


async def get_staff_attendance_trend(
    pool, org_id: int, employee_id: int, months: int = 6
) -> list[dict]:
    """Single staff member's attendance over N months, newest first."""
    sql = """
        SELECT * FROM wh_attendance_summary
        WHERE business_id = $1 AND employee_id = $2
        ORDER BY period_start DESC
        LIMIT $3
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, employee_id, months)
    return [dict(r) for r in rows]


async def get_total_hours_summary(
    pool, org_id: int, period_start: date | str
) -> dict | None:
    """Total hours worked across all staff for a month."""
    sql = """
        SELECT
            SUM(total_hours_worked) AS total_hours,
            SUM(days_worked) AS total_days,
            COUNT(DISTINCT employee_id) AS staff_count
        FROM wh_attendance_summary
        WHERE business_id = $1 AND period_start = $2
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, org_id, period_start)
    return dict(row) if row else None


async def get_low_attendance_staff(
    pool, org_id: int, period_start: date | str, min_days: int = 1
) -> list[dict]:
    """Staff with days_worked < min_days."""
    sql = """
        SELECT * FROM wh_attendance_summary
        WHERE business_id = $1 AND period_start = $2
          AND days_worked < $3
        ORDER BY days_worked ASC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start, min_days)
    return [dict(r) for r in rows]
