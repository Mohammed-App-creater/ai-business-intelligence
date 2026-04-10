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
    sql = """
        WITH recent_periods AS (
            SELECT DISTINCT period_start
            FROM wh_payment_breakdown
            WHERE business_id = $1 AND location_id = 0
            ORDER BY period_start DESC
            LIMIT $2
        ),
        agg AS (
            SELECT
                SUM(cash_amount) AS cash_amount,
                SUM(cash_count) AS cash_count,
                SUM(card_amount) AS card_amount,
                SUM(card_count) AS card_count,
                SUM(gift_card_amount) AS gift_card_amount,
                SUM(gift_card_count) AS gift_card_count,
                SUM(other_amount) AS other_amount,
                SUM(other_count) AS other_count,
                SUM(total_amount) AS total_amount
            FROM wh_payment_breakdown p
            INNER JOIN recent_periods r ON p.period_start = r.period_start
            WHERE p.business_id = $1 AND p.location_id = 0
            HAVING COUNT(*) > 0
        )
        SELECT payment_type, visit_count, revenue, pct_of_total
        FROM (
            SELECT
                'Cash'::text AS payment_type,
                cash_count::integer AS visit_count,
                cash_amount AS revenue,
                ROUND((cash_amount / NULLIF(total_amount, 0)) * 100, 1) AS pct_of_total
            FROM agg
            UNION ALL
            SELECT
                'Card'::text,
                card_count::integer,
                card_amount,
                ROUND((card_amount / NULLIF(total_amount, 0)) * 100, 1)
            FROM agg
            UNION ALL
            SELECT
                'GiftCard'::text,
                gift_card_count::integer,
                gift_card_amount,
                ROUND((gift_card_amount / NULLIF(total_amount, 0)) * 100, 1)
            FROM agg
            UNION ALL
            SELECT
                'Other'::text,
                other_count::integer,
                other_amount,
                ROUND((other_amount / NULLIF(total_amount, 0)) * 100, 1)
            FROM agg
        ) t
        ORDER BY
            CASE payment_type
                WHEN 'Cash' THEN 1
                WHEN 'Card' THEN 2
                WHEN 'GiftCard' THEN 3
                WHEN 'Other' THEN 4
            END
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_staff_revenue(
    pool, org_id: int, months: int = 3
) -> list[dict]:
    """
    Returns staff revenue ranking for the period.
    Columns: emp_id, staff_name, visit_count, service_revenue,
             tips_collected, avg_ticket, revenue_rank
    """
    sql = """
        WITH recent_periods AS (
            SELECT DISTINCT period_start
            FROM wh_staff_performance
            WHERE business_id = $1
            ORDER BY period_start DESC
            LIMIT $2
        ),
        filtered AS (
            SELECT sp.*
            FROM wh_staff_performance sp
            INNER JOIN recent_periods r ON sp.period_start = r.period_start
            WHERE sp.business_id = $1
        ),
        by_emp AS (
            SELECT
                employee_id AS emp_id,
                MAX(employee_name) AS staff_name,
                SUM(total_visits)::integer AS visit_count,
                SUM(total_revenue) AS service_revenue,
                SUM(total_tips) AS tips_collected,
                SUM(total_revenue) / NULLIF(SUM(total_visits), 0) AS avg_ticket
            FROM filtered
            GROUP BY employee_id
        )
        SELECT
            emp_id,
            staff_name,
            visit_count,
            service_revenue,
            tips_collected,
            avg_ticket,
            RANK() OVER (ORDER BY service_revenue DESC) AS revenue_rank
        FROM by_emp
        ORDER BY revenue_rank, emp_id
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_location_revenue(
    pool, org_id: int, months: int = 3
) -> list[dict]:
    """
    Per-location monthly rows from wh_monthly_revenue (location_id > 0 excludes org rollup).

    period is YYYY-MM. service_revenue and avg_ticket map from gross_revenue and avg_visit_value.
    gc_redemptions maps from total_gc_amount. mom_growth_pct is MoM % change in gross_revenue
    via LAG per location (NULL when there is no prior month or prior gross is zero).
    """
    sql = """
        WITH loc_months AS (
            SELECT *
            FROM wh_monthly_revenue
            WHERE business_id = $1 AND location_id > 0
        ),
        lagged AS (
            SELECT
                location_id,
                period_start,
                gross_revenue,
                avg_visit_value,
                total_tips,
                total_discounts,
                total_gc_amount,
                visit_count,
                LAG(gross_revenue) OVER (
                    PARTITION BY location_id
                    ORDER BY period_start
                ) AS prev_gross
            FROM loc_months
        ),
        recent_periods AS (
            SELECT DISTINCT period_start
            FROM loc_months
            ORDER BY period_start DESC
            LIMIT $2
        )
        SELECT
            l.location_id,
            ('Location ' || l.location_id::text) AS location_name,
            to_char(l.period_start, 'YYYY-MM') AS period,
            l.gross_revenue AS service_revenue,
            l.avg_visit_value AS avg_ticket,
            l.total_tips,
            l.total_discounts,
            l.total_gc_amount AS gc_redemptions,
            l.visit_count,
            CASE
                WHEN l.prev_gross IS NULL OR l.prev_gross = 0 THEN NULL
                ELSE ROUND(
                    ((l.gross_revenue - l.prev_gross) / l.prev_gross::numeric) * 100,
                    2
                )
            END AS mom_growth_pct
        FROM lagged l
        INNER JOIN recent_periods p ON l.period_start = p.period_start
        ORDER BY l.period_start DESC, l.location_id
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, months)
    return [dict(r) for r in rows]


async def get_promo_impact(
    pool, org_id: int, months: int = 3
) -> list[dict]:
    """
    Returns promo code discount impact.
    Columns: promo_code, promo_description, location_id, location_name,
             times_used, total_discount_given, revenue_after_discount
    """
    # Promo data not yet in warehouse —
    # populated by analytics backend mock only.
    return []


async def get_failed_refunds(
    pool, org_id: int, months: int = 3
) -> list[dict]:
    """
    Returns failed/refunded/canceled visit revenue loss.
    Columns: status_code, status_label, visit_count,
             lost_revenue, avg_lost_per_visit
    """
    # Failed/refund data not yet in warehouse —
    # populated by analytics backend mock only.
    return []


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
