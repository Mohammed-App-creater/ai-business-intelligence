"""Warehouse ETL log: wh_etl_log."""

from __future__ import annotations


async def get_recent_runs(
    pool, org_id: int | None = None, limit: int = 50
) -> list[dict]:
    """Last N ETL run log entries; org_id None returns all businesses."""
    sql = """
        SELECT * FROM wh_etl_log
        WHERE ($1::int IS NULL OR business_id = $1)
        ORDER BY started_at DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, org_id, limit)
    return [dict(r) for r in rows]


async def get_failed_runs(pool, limit: int = 20) -> list[dict]:
    """Last N failed ETL runs across all businesses."""
    sql = """
        SELECT * FROM wh_etl_log
        WHERE status = 'failed'
        ORDER BY started_at DESC
        LIMIT $1
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, limit)
    return [dict(r) for r in rows]


async def get_last_run_for_table(
    pool, target_table: str, org_id: int | None = None
) -> dict | None:
    """Most recent ETL run for a specific warehouse table."""
    sql = """
        SELECT * FROM wh_etl_log
        WHERE target_table = $1
          AND ($2::int IS NULL OR business_id = $2)
        ORDER BY started_at DESC
        LIMIT 1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, target_table, org_id)
    return dict(row) if row else None


async def get_etl_run_stats(pool, target_table: str) -> dict | None:
    """Aggregate stats across all runs for a table."""
    sql = """
        SELECT
            COUNT(*) AS total_runs,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS fail_count,
            AVG(duration_seconds) AS avg_duration_seconds,
            MAX(started_at) AS last_run_at
        FROM wh_etl_log
        WHERE target_table = $1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, target_table)
    return dict(row) if row else None
