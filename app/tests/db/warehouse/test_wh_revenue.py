"""Unit tests for warehouse.wh_revenue."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_revenue
from app.tests.db.warehouse.conftest import SAMPLE_DATE, SAMPLE_DATE_B, SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_get_monthly_trend_async(mock_pool, mock_conn):
    assert inspect.iscoroutinefunction(wh_revenue.get_monthly_trend)


@pytest.mark.asyncio
async def test_get_monthly_trend_empty_and_sql(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    out = await wh_revenue.get_monthly_trend(mock_pool, SAMPLE_ORG_ID, months=3)
    assert out == []
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID
    assert params[1] == 3


@pytest.mark.asyncio
async def test_get_monthly_trend_returns_list_of_dict(mock_pool, mock_conn):
    row = {"business_id": SAMPLE_ORG_ID, "total_revenue": "100.00"}
    mock_conn.fetch = AsyncMock(return_value=[row])
    out = await wh_revenue.get_monthly_trend(mock_pool, SAMPLE_ORG_ID)
    assert isinstance(out, list)
    assert out == [row]


@pytest.mark.asyncio
async def test_get_monthly_by_location_fetchrow_none(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    assert (
        await wh_revenue.get_monthly_by_location(
            mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE, location_id=5
        )
        is None
    )
    sql, params = _sql_params(mock_conn.fetchrow.call_args)
    assert "business_id" in sql.lower()
    assert params == [SAMPLE_ORG_ID, 5, SAMPLE_DATE]


@pytest.mark.asyncio
async def test_get_revenue_comparison(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_revenue.get_revenue_comparison(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE, SAMPLE_DATE_B
    )
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_daily_trend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_revenue.get_daily_trend(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE_B, SAMPLE_DATE
    )
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_best_revenue_days_limit(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_revenue.get_best_revenue_days(mock_pool, SAMPLE_ORG_ID, limit=7)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[1] == 7


@pytest.mark.asyncio
async def test_get_location_revenue_summary(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_revenue.get_location_revenue_summary(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE)
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID
