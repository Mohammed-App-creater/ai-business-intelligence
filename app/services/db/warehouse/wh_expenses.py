"""Warehouse expense summary: wh_expense_summary."""

from __future__ import annotations

from datetime import date


async def get_expense_monthly_summary(
    pool, org_id: int, period_start: date | str
) -> list[dict]:
    """All categories for a given month, ordered by total_amount DESC."""
    sql = """
        SELECT * FROM wh_expense_summary
        WHERE business_id = $1 AND period_start = $2
        ORDER BY total_amount DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start)
    return [dict(r) for r in rows]


async def get_expense_trend(
    pool, org_id: int, category_id: int, months: int = 6
) -> list[dict]:
    """Single category expense trend over N months, newest first."""
    sql = """
        SELECT * FROM wh_expense_summary
        WHERE business_id = $1 AND category_id = $2
        ORDER BY period_start DESC
        LIMIT $3
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, category_id, months)
    return [dict(r) for r in rows]


async def get_top_expense_categories(
    pool, org_id: int, period_start: date | str, limit: int = 5
) -> list[dict]:
    """Top N categories by total_amount for a month."""
    sql = """
        SELECT * FROM wh_expense_summary
        WHERE business_id = $1 AND period_start = $2
        ORDER BY total_amount DESC
        LIMIT $3
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_start, limit)
    return [dict(r) for r in rows]


async def get_expense_total(
    pool, org_id: int, period_start: date | str
) -> dict | None:
    """Total expenses across all categories for a month (location rollup)."""
    sql = """
        SELECT
            SUM(total_amount) AS total,
            SUM(expense_count) AS count
        FROM wh_expense_summary
        WHERE business_id = $1 AND period_start = $2 AND location_id = 0
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, org_id, period_start)
    return dict(row) if row else None


async def get_expense_comparison(
    pool, org_id: int, period_a: date | str, period_b: date | str
) -> list[dict]:
    """Two months side-by-side per category for MoM expense comparison."""
    sql = """
        SELECT
            category_id,
            category_name,
            SUM(CASE WHEN period_start = $2 THEN total_amount ELSE 0 END) AS period_a_amount,
            SUM(CASE WHEN period_start = $3 THEN total_amount ELSE 0 END) AS period_b_amount
        FROM wh_expense_summary
        WHERE business_id = $1 AND period_start IN ($2, $3)
        GROUP BY category_id, category_name
        ORDER BY period_b_amount DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, period_a, period_b)
    return [dict(r) for r in rows]
