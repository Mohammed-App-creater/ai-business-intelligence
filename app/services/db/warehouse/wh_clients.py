"""Warehouse client metrics: wh_client_metrics."""

from __future__ import annotations

from datetime import date


async def get_churned_clients(pool, org_id: int, limit: int = 100) -> list[dict]:
    """Clients where is_churned=TRUE, ordered by days_since_last_visit DESC."""
    sql = """
        SELECT * FROM wh_client_metrics
        WHERE business_id = $1 AND is_churned = TRUE
        ORDER BY days_since_last_visit DESC NULLS LAST
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, limit)
    return [dict(r) for r in rows]


async def get_top_clients_by_spend(pool, org_id: int, limit: int = 20) -> list[dict]:
    """Top N clients by total_spend DESC."""
    sql = """
        SELECT * FROM wh_client_metrics
        WHERE business_id = $1
        ORDER BY total_spend DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, limit)
    return [dict(r) for r in rows]


async def get_retention_summary(pool, org_id: int) -> dict | None:
    """Aggregate retention stats for the business."""
    sql = """
        SELECT
            COUNT(*) AS total_clients,
            SUM(CASE WHEN is_churned THEN 1 ELSE 0 END) AS churned_count,
            SUM(CASE WHEN NOT is_churned THEN 1 ELSE 0 END) AS active_count,
            AVG(visit_frequency_days) AS avg_visit_frequency_days,
            AVG(avg_spend_per_visit) AS avg_spend_per_visit
        FROM wh_client_metrics
        WHERE business_id = $1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, org_id)
    return dict(row) if row else None


async def get_new_clients(
    pool, org_id: int, since_date: date | str, limit: int = 100
) -> list[dict]:
    """Clients whose first_visit_date >= since_date, ordered by first_visit_date DESC."""
    sql = """
        SELECT * FROM wh_client_metrics
        WHERE business_id = $1 AND first_visit_date >= $2
        ORDER BY first_visit_date DESC NULLS LAST
        LIMIT $3
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, since_date, limit)
    return [dict(r) for r in rows]


async def get_client_detail(pool, org_id: int, customer_id: int) -> dict | None:
    """Single client's lifetime metrics."""
    sql = """
        SELECT * FROM wh_client_metrics
        WHERE business_id = $1 AND customer_id = $2
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, org_id, customer_id)
    return dict(row) if row else None


async def get_high_value_clients(
    pool, org_id: int, min_spend: float, limit: int = 50
) -> list[dict]:
    """Clients with total_spend >= min_spend, ordered by total_spend DESC."""
    sql = """
        SELECT * FROM wh_client_metrics
        WHERE business_id = $1 AND total_spend >= $2
        ORDER BY total_spend DESC
        LIMIT $3
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, min_spend, limit)
    return [dict(r) for r in rows]
