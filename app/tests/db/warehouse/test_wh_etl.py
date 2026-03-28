"""Unit tests for warehouse.wh_etl."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_etl
from app.tests.db.warehouse.conftest import SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_recent_runs",
        "get_failed_runs",
        "get_last_run_for_table",
        "get_etl_run_stats",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_etl, name))


@pytest.mark.asyncio
async def test_get_recent_runs_with_org(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_etl.get_recent_runs(mock_pool, SAMPLE_ORG_ID, limit=25)
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "wh_etl_log" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID
    assert params[1] == 25


@pytest.mark.asyncio
async def test_get_recent_runs_all_businesses(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_etl.get_recent_runs(mock_pool, None, limit=10)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[0] is None


@pytest.mark.asyncio
async def test_get_failed_runs(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_etl.get_failed_runs(mock_pool, limit=15)
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "status" in sql.lower()
    assert "failed" in sql.lower()
    assert params[0] == 15


@pytest.mark.asyncio
async def test_get_last_run_for_table(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    await wh_etl.get_last_run_for_table(mock_pool, "wh_monthly_revenue", SAMPLE_ORG_ID)
    sql, params = _sql_params(mock_conn.fetchrow.call_args)
    assert params[0] == "wh_monthly_revenue"
    assert params[1] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_etl_run_stats(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    await wh_etl.get_etl_run_stats(mock_pool, "wh_daily_revenue")
    sql, params = _sql_params(mock_conn.fetchrow.call_args)
    assert "target_table" in sql.lower()
    assert params[0] == "wh_daily_revenue"
