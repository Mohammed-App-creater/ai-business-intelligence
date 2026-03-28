"""
Unit tests for scripts.etl.base — mocked pools only, no real DB.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from scripts.etl.base import (
    BaseExtractor,
    BaseLoader,
    ETLLogger,
    ETLRunResult,
    parse_time_str,
    run_etl_job,
)


# ---------------------------------------------------------------------------
# parse_time_str
# ---------------------------------------------------------------------------


def test_parse_time_str_colon_format() -> None:
    assert parse_time_str("09:30") == 9.5


def test_parse_time_str_ampm_format() -> None:
    assert parse_time_str("2:30 PM") == 14.5


def test_parse_time_str_seconds_format() -> None:
    assert parse_time_str("09:30:00") == 9.5


def test_parse_time_str_4digit_format() -> None:
    assert parse_time_str("1430") == 14.5


def test_parse_time_str_3digit_format() -> None:
    assert parse_time_str("930") == 9.5


def test_parse_time_str_zero_returns_none() -> None:
    assert parse_time_str("0") is None


def test_parse_time_str_empty_returns_none() -> None:
    assert parse_time_str("") is None


def test_parse_time_str_none_returns_none() -> None:
    assert parse_time_str(None) is None


def test_parse_time_str_midnight_zero() -> None:
    assert parse_time_str("00:00:00") is None


def test_parse_time_str_garbage_returns_none() -> None:
    assert parse_time_str("abc") is None


def test_parse_time_str_whitespace_stripped() -> None:
    assert parse_time_str("  09:30  ") == 9.5


# ---------------------------------------------------------------------------
# BaseExtractor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_base_extractor_fetch_all_returns_list_of_dicts() -> None:
    pool = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cur)
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
    cur.execute = AsyncMock()
    cur.fetchall = AsyncMock(return_value=[{"id": 1}])

    ex = BaseExtractor(pool)
    rows = await ex.fetch_all("SELECT 1")
    assert rows == [{"id": 1}]


@pytest.mark.asyncio
async def test_base_extractor_fetch_all_empty_result() -> None:
    pool = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cur)
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
    cur.execute = AsyncMock()
    cur.fetchall = AsyncMock(return_value=[])

    ex = BaseExtractor(pool)
    rows = await ex.fetch_all("SELECT 1")
    assert rows == []


@pytest.mark.asyncio
async def test_base_extractor_fetch_all_passes_params() -> None:
    pool = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cur)
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
    cur.execute = AsyncMock()
    cur.fetchall = AsyncMock(return_value=[])

    sql = "SELECT * FROM t WHERE id = %s"
    params = (99,)
    ex = BaseExtractor(pool)
    await ex.fetch_all(sql, params)
    cur.execute.assert_awaited_once_with(sql, params)


@pytest.mark.asyncio
async def test_base_extractor_extract_raises_not_implemented() -> None:
    pool = MagicMock()
    ex = BaseExtractor(pool)
    with pytest.raises(NotImplementedError):
        await ex.extract(org_id=1)


@pytest.mark.asyncio
async def test_base_extractor_fetch_all_reraises_on_db_error() -> None:
    pool = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cur)
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
    cur.execute = AsyncMock(side_effect=Exception("DB down"))

    ex = BaseExtractor(pool)
    with pytest.raises(Exception, match="DB down"):
        await ex.fetch_all("SELECT 1")


# ---------------------------------------------------------------------------
# BaseLoader
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_base_loader_upsert_many_calls_execute_per_row() -> None:
    pool = MagicMock()
    conn = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value="INSERT 0 1")

    loader = BaseLoader(pool)
    rows = [{"a": 1}, {"a": 2}]
    await loader.upsert_many(
        "INSERT INTO t (a) VALUES ($1) ON CONFLICT DO NOTHING",
        rows,
        lambda r: (r["a"],),
    )
    assert conn.execute.await_count == 2


@pytest.mark.asyncio
async def test_base_loader_upsert_many_returns_counts() -> None:
    pool = MagicMock()
    conn = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value="INSERT 0 1")

    loader = BaseLoader(pool)
    rows = [{"a": 1}, {"a": 2}, {"a": 3}]
    ins, upd = await loader.upsert_many("SQL", rows, lambda r: (r["a"],))
    assert (ins, upd) == (3, 0)


@pytest.mark.asyncio
async def test_base_loader_upsert_many_empty_rows() -> None:
    pool = MagicMock()
    conn = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock()

    loader = BaseLoader(pool)
    ins, upd = await loader.upsert_many("SQL", [], lambda r: ())
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_base_loader_load_raises_not_implemented() -> None:
    pool = MagicMock()
    loader = BaseLoader(pool)
    with pytest.raises(NotImplementedError):
        await loader.load([])


@pytest.mark.asyncio
async def test_base_loader_param_fn_called_per_row() -> None:
    pool = MagicMock()
    conn = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value="INSERT 0 1")

    param_fn = MagicMock(side_effect=lambda r: (r["a"],))
    loader = BaseLoader(pool)
    rows = [{"a": 1}, {"a": 2}]
    await loader.upsert_many("SQL", rows, param_fn)
    assert param_fn.call_args_list == [call({"a": 1}), call({"a": 2})]


# ---------------------------------------------------------------------------
# ETLLogger
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_etl_logger_start_inserts_running_row() -> None:
    pool = MagicMock()
    conn = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.fetchval = AsyncMock(return_value="some-uuid-string")

    log = ETLLogger(pool)
    run_id = await log.start(
        "wh_monthly_revenue",
        org_id=42,
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
    )
    conn.fetchval.assert_awaited_once()
    assert run_id == "some-uuid-string"


@pytest.mark.asyncio
async def test_etl_logger_start_sql_contains_running_status() -> None:
    pool = MagicMock()
    conn = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.fetchval = AsyncMock(return_value="uuid")

    log = ETLLogger(pool)
    await log.start("wh_monthly_revenue", org_id=42)
    sql = conn.fetchval.await_args[0][0]
    assert "running" in sql.lower()


@pytest.mark.asyncio
async def test_etl_logger_success_updates_row() -> None:
    pool = MagicMock()
    conn = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value="UPDATE 1")

    log = ETLLogger(pool)
    await log.success("some-run-id", rows_inserted=5, rows_updated=2)
    conn.execute.assert_awaited_once()
    sql = conn.execute.await_args[0][0]
    assert "success" in sql.lower()


@pytest.mark.asyncio
async def test_etl_logger_fail_updates_row_with_error() -> None:
    pool = MagicMock()
    conn = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock()

    log = ETLLogger(pool)
    await log.fail("some-run-id", "timeout error")
    conn.execute.assert_awaited_once()
    sql = conn.execute.await_args[0][0]
    assert "failed" in sql.lower()


@pytest.mark.asyncio
async def test_etl_logger_cleanup_orphaned_returns_count() -> None:
    pool = MagicMock()
    conn = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.fetch = AsyncMock(return_value=[{"id": 1}, {"id": 2}])

    log = ETLLogger(pool)
    n = await log.cleanup_orphaned_runs(older_than_minutes=120)
    assert n == 2


# ---------------------------------------------------------------------------
# run_etl_job
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_etl_job_logs_success_on_clean_exit() -> None:
    mock_logger = MagicMock()
    mock_logger.start = AsyncMock(return_value="run-1")
    mock_logger.success = AsyncMock()
    mock_logger.fail = AsyncMock()

    async with run_etl_job(mock_logger, "wh_monthly_revenue") as result:
        assert isinstance(result, ETLRunResult)
        result.rows_inserted = 5

    mock_logger.success.assert_awaited_once_with(
        "run-1",
        rows_inserted=5,
        rows_updated=0,
        rows_deleted=0,
    )
    mock_logger.fail.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_etl_job_logs_failure_on_exception() -> None:
    mock_logger = MagicMock()
    mock_logger.start = AsyncMock(return_value="run-1")
    mock_logger.success = AsyncMock()
    mock_logger.fail = AsyncMock()

    with pytest.raises(ValueError, match="boom"):
        async with run_etl_job(mock_logger, "wh_monthly_revenue") as result:
            raise ValueError("boom")

    mock_logger.fail.assert_awaited_once_with("run-1", "boom")
    mock_logger.success.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_etl_job_result_has_correct_defaults() -> None:
    mock_logger = MagicMock()
    mock_logger.start = AsyncMock(return_value="run-1")
    mock_logger.success = AsyncMock()
    mock_logger.fail = AsyncMock()

    async with run_etl_job(mock_logger, "wh_monthly_revenue") as result:
        assert result.rows_inserted == 0
        assert result.rows_updated == 0
        assert result.rows_deleted == 0


@pytest.mark.asyncio
async def test_run_etl_job_reraises_exception() -> None:
    mock_logger = MagicMock()
    mock_logger.start = AsyncMock(return_value="run-1")
    mock_logger.success = AsyncMock()
    mock_logger.fail = AsyncMock()

    with pytest.raises(RuntimeError, match="crash"):
        async with run_etl_job(mock_logger, "x") as result:
            raise RuntimeError("crash")
