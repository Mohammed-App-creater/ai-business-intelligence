"""
ETL foundation: extractors (MySQL), loaders (warehouse PostgreSQL), run logging,
and attendance time parsing.
"""
from __future__ import annotations

import logging
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime
from typing import AsyncIterator, Callable

_log = logging.getLogger(__name__)


@dataclass
class ETLRunResult:
    run_id: str
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0


class BaseExtractor:
    """
    Base class for all ETL extractors.

    Subclasses read from the production MySQL database and return raw rows
    as list[dict]. They do NOT transform — that is the transform layer's job.

    Subclass contract:
        async def extract(self, org_id: int, **kwargs) -> list[dict]:
            ...

    Usage:
        extractor = MyExtractor(prod_pool)
        rows = await extractor.extract(org_id=42, period_start=date(2026,1,1))
    """

    def __init__(self, prod_pool) -> None:
        self._pool = prod_pool
        self._logger = logging.getLogger(self.__class__.__name__)

    async def fetch_all(self, sql: str, params: tuple = ()) -> list[dict]:
        """
        Execute a SELECT on the production DB and return all rows as list[dict].
        Uses aiomysql DictCursor so column names are keys.
        Logs the query at DEBUG level.
        On any exception, logs ERROR and re-raises.
        """
        self._logger.debug("fetch_all: %s | params=%s", sql, params)
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                    return await cur.fetchall()
        except Exception:
            self._logger.exception("fetch_all failed")
            raise

    async def extract(self, org_id: int, **kwargs) -> list[dict]:
        """Override in subclass."""
        raise NotImplementedError


class BaseLoader:
    """
    Base class for all ETL loaders.

    Subclasses write transformed rows into the PostgreSQL warehouse via
    idempotent upserts (INSERT ... ON CONFLICT DO UPDATE).

    Subclass contract:
        async def load(self, rows: list[dict]) -> tuple[int, int]:
            ...returns (rows_inserted, rows_updated)

    Usage:
        loader = MyLoader(wh_pool)
        inserted, updated = await loader.load(transformed_rows)
    """

    def __init__(self, wh_pool) -> None:
        self._pool = wh_pool
        self._logger = logging.getLogger(self.__class__.__name__)

    async def upsert_many(
        self,
        sql: str,
        rows: list[dict],
        param_fn: Callable[[dict], tuple],
    ) -> tuple[int, int]:
        """
        Execute an upsert for each row in rows.

        asyncpg does not distinguish insert vs update in the status string
        for ON CONFLICT ... DO UPDATE. Each successful execute is counted
        as one insert; updated remains 0 (MVP).
        """
        if not rows:
            return 0, 0
        inserted = 0
        updated = 0
        async with self._pool.acquire() as conn:
            for row in rows:
                params = param_fn(row)
                status = await conn.execute(sql, *params)
                self._logger.debug("upsert_many execute status=%s", status)
                inserted += 1
        return inserted, updated

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        """Override in subclass. Return (inserted, updated)."""
        raise NotImplementedError


class ETLLogger:
    """
    Writes ETL run history to wh_etl_log.

    Usage:
        etl_log = ETLLogger(wh_pool)
        run_id = await etl_log.start(target_table='wh_monthly_revenue', org_id=42,
                                     period_start=date(2026,1,1), period_end=date(2026,1,31))
        # ... do ETL work ...
        await etl_log.success(run_id, rows_inserted=12, rows_updated=3)
        # or on failure:
        await etl_log.fail(run_id, error_message="Connection timeout")
    """

    _START_SQL = """
        INSERT INTO wh_etl_log
            (target_table, business_id, period_start, period_end, status, started_at)
        VALUES ($1, $2, $3, $4, 'running', now())
        RETURNING run_id::text
    """.strip()

    _SUCCESS_SQL = """
        UPDATE wh_etl_log
        SET status='success',
            rows_inserted=$2,
            rows_updated=$3,
            rows_deleted=$4,
            finished_at=now(),
            duration_seconds=EXTRACT(EPOCH FROM (now() - started_at))
        WHERE run_id=$1::uuid
    """.strip()

    _FAIL_SQL = """
        UPDATE wh_etl_log
        SET status='failed',
            error_message=$2,
            finished_at=now(),
            duration_seconds=EXTRACT(EPOCH FROM (now() - started_at))
        WHERE run_id=$1::uuid
    """.strip()

    _CLEANUP_SQL = """
        UPDATE wh_etl_log
        SET status='failed',
            error_message='ETL process died — orphaned run cleaned up',
            finished_at=now(),
            duration_seconds=EXTRACT(EPOCH FROM (now() - started_at))
        WHERE status='running'
          AND started_at < now() - (($1::text || ' minutes')::interval)
        RETURNING id
    """.strip()

    def __init__(self, wh_pool) -> None:
        self._pool = wh_pool
        self._logger = logging.getLogger(self.__class__.__name__)

    async def start(
        self,
        target_table: str,
        org_id: int | None = None,
        period_start: date | None = None,
        period_end: date | None = None,
    ) -> str:
        """Insert a new wh_etl_log row with status='running'. Returns run_id (UUID string)."""
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                self._START_SQL,
                target_table,
                org_id,
                period_start,
                period_end,
            )

    async def success(
        self,
        run_id: str,
        rows_inserted: int = 0,
        rows_updated: int = 0,
        rows_deleted: int = 0,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                self._SUCCESS_SQL,
                run_id,
                rows_inserted,
                rows_updated,
                rows_deleted,
            )

    async def fail(self, run_id: str, error_message: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(self._FAIL_SQL, run_id, error_message)

    async def cleanup_orphaned_runs(self, older_than_minutes: int = 120) -> int:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(self._CLEANUP_SQL, older_than_minutes)
        return len(rows)


def parse_time_str(raw: str | None) -> float | None:
    """
    Parse tbl_attendance.time_sign_in / time_sign_out varchar into
    decimal hours (float).
    """
    if raw is None:
        return None
    s = raw.strip()
    if not s or s == "0":
        return None
    if s == "00:00:00":
        return None

    fmts = ("%H:%M", "%I:%M %p", "%H:%M:%S", "%I:%M:%S %p")
    for fmt in fmts:
        try:
            t = datetime.strptime(s, fmt).time()
            return t.hour + t.minute / 60.0 + t.second / 3600.0
        except ValueError:
            continue

    if re.fullmatch(r"\d+", s) is not None and len(s) in (3, 4):
        padded = s.zfill(4)
        hours = int(padded[:2])
        minutes = int(padded[2:4])
        if hours > 23 or minutes > 59:
            _log.warning("parse_time_str: unrecognisable value %r", raw)
            return None
        return hours + minutes / 60.0

    _log.warning("parse_time_str: unrecognisable value %r", raw)
    return None


@asynccontextmanager
async def run_etl_job(
    etl_logger: ETLLogger,
    target_table: str,
    org_id: int | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
) -> AsyncIterator[ETLRunResult]:
    """
    Async context manager that wraps a single ETL job with logging.

    On clean exit: calls etl_logger.success with counts from the result holder.
    On exception: calls etl_logger.fail then re-raises.
    """
    run_id = await etl_logger.start(
        target_table,
        org_id=org_id,
        period_start=period_start,
        period_end=period_end,
    )
    result = ETLRunResult(run_id=run_id)
    try:
        yield result
    except Exception as exc:
        await etl_logger.fail(result.run_id, str(exc))
        raise
    else:
        await etl_logger.success(
            result.run_id,
            rows_inserted=result.rows_inserted,
            rows_updated=result.rows_updated,
            rows_deleted=result.rows_deleted,
        )
