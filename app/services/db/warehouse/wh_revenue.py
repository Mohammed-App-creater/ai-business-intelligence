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


async def get_payment_type_breakdown(
    pool, org_id: int, months: int = 3
) -> list[dict]:
    """
    Returns payment type revenue breakdown.
    Reads from the warehouse table populated by etl/loaders/revenue.py.
    Columns: payment_type, visit_count, revenue, pct_of_total
    """
    # TODO: implement SQL SELECT against your warehouse revenue table
    # Example shape of expected return:
    # [
    #   {"payment_type": "Card", "visit_count": 142, "revenue": 9820.0, "pct_of_total": 69.5},
    #   {"payment_type": "Cash", "visit_count":  41, "revenue": 3210.0, "pct_of_total": 22.7},
    # ]
    raise NotImplementedError("get_payment_type_breakdown not yet implemented")


async def get_staff_revenue(
    pool, org_id: int, months: int = 3
) -> list[dict]:
    """
    Returns staff revenue ranking for the period.
    Columns: emp_id, staff_name, visit_count, service_revenue,
             tips_collected, avg_ticket, revenue_rank
    """
    raise NotImplementedError("get_staff_revenue not yet implemented")


async def get_location_revenue(
    pool, org_id: int, months: int = 3
) -> list[dict]:
    """
    Returns per-location per-period revenue rows.
    Columns: location_id, location_name, period, visit_count,
             service_revenue, total_tips, avg_ticket, total_discounts,
             gc_redemptions, pct_of_total_revenue, mom_growth_pct
    """
    raise NotImplementedError("get_location_revenue not yet implemented")


async def get_promo_impact(
    pool, org_id: int, months: int = 3
) -> list[dict]:
    """
    Returns promo code discount impact.
    Columns: promo_code, promo_description, location_id, location_name,
             times_used, total_discount_given, revenue_after_discount
    """
    raise NotImplementedError("get_promo_impact not yet implemented")


async def get_failed_refunds(
    pool, org_id: int, months: int = 3
) -> list[dict]:
    """
    Returns failed/refunded/canceled visit revenue loss.
    Columns: status_code, status_label, visit_count,
             lost_revenue, avg_lost_per_visit
    """
    raise NotImplementedError("get_failed_refunds not yet implemented")


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
