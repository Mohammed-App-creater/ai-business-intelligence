"""Warehouse payment breakdown: wh_payment_breakdown."""

from __future__ import annotations

from datetime import date


async def get_payment_monthly_breakdown(
    pool, org_id: int, period_start: date | str
) -> dict | None:
    """Single month payment method split (location_id=0)."""
    sql = """
        SELECT * FROM wh_payment_breakdown
        WHERE business_id = $1 AND location_id = 0 AND period_start = $2
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, org_id, period_start)
    return dict(row) if row else None


async def get_payment_trend(pool, org_id: int, months: int = 6) -> list[dict]:
    """Last N months of payment method totals, newest first."""
    sql = """
        SELECT * FROM wh_payment_breakdown
        WHERE business_id = $1 AND location_id = 0
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_cash_vs_card_trend(pool, org_id: int, months: int = 6) -> list[dict]:
    """Cash and card amounts/counts only for N months."""
    sql = """
        SELECT
            period_start,
            cash_amount,
            cash_count,
            card_amount,
            card_count
        FROM wh_payment_breakdown
        WHERE business_id = $1 AND location_id = 0
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_gift_card_usage_trend(pool, org_id: int, months: int = 6) -> list[dict]:
    """Gift card amount and count trend."""
    sql = """
        SELECT
            period_start,
            gift_card_amount,
            gift_card_count
        FROM wh_payment_breakdown
        WHERE business_id = $1 AND location_id = 0
        ORDER BY period_start DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]
