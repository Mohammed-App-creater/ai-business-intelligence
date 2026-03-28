"""Warehouse review summary: wh_review_summary."""

from __future__ import annotations

from datetime import date


async def get_review_monthly_summary(
    pool, org_id: int, period_start: date | str
) -> dict | None:
    """Single month review KPIs across all sources."""
    sql = """
        SELECT * FROM wh_review_summary
        WHERE business_id = $1 AND period_start = $2
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, org_id, period_start)
    return dict(row) if row else None


async def get_review_trend(pool, org_id: int, months: int = 6) -> list[dict]:
    """Last N months of overall_avg_rating and total_review_count, newest first."""
    sql = """
        SELECT period_start, overall_avg_rating, total_review_count
        FROM wh_review_summary
        WHERE business_id = $1
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_google_review_trend(pool, org_id: int, months: int = 6) -> list[dict]:
    """Last N months of google_avg_rating, google_review_count, google_bad_review_count."""
    sql = """
        SELECT
            period_start,
            google_avg_rating,
            google_review_count,
            google_bad_review_count
        FROM wh_review_summary
        WHERE business_id = $1
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_rating_decline_periods(
    pool, org_id: int, threshold: float = 3.5
) -> list[dict]:
    """Months where overall_avg_rating < threshold, ordered by period_start DESC."""
    sql = """
        SELECT * FROM wh_review_summary
        WHERE business_id = $1
          AND overall_avg_rating IS NOT NULL
          AND overall_avg_rating < $2
        ORDER BY period_start DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, threshold)
    return [dict(r) for r in rows]
