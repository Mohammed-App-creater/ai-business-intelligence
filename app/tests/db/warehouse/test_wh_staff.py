"""Unit tests for warehouse.wh_staff."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_staff
from app.tests.db.warehouse.conftest import SAMPLE_DATE, SAMPLE_EMP_ID, SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_staff_monthly_performance",
        "get_staff_trend",
        "get_top_performers",
        "get_staff_rating_ranking",
        "get_staff_utilisation",
        "get_underperforming_staff",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_staff, name))


@pytest.mark.asyncio
async def test_get_staff_monthly_performance(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_staff.get_staff_monthly_performance(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE)
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_staff_trend_months(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_staff.get_staff_trend(mock_pool, SAMPLE_ORG_ID, SAMPLE_EMP_ID, months=4)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[2] == 4


@pytest.mark.asyncio
async def test_get_top_performers_limit(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_staff.get_top_performers(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE, limit=8)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[2] == 8


@pytest.mark.asyncio
async def test_get_staff_rating_ranking(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_staff.get_staff_rating_ranking(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert "avg_rating is not null" in sql.lower()


@pytest.mark.asyncio
async def test_get_staff_utilisation(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_staff.get_staff_utilisation(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "utilisation_rate" in sql.lower()


@pytest.mark.asyncio
async def test_get_underperforming_staff_min_visits(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_staff.get_underperforming_staff(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE, min_visits=2
    )
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[2] == 2
    assert "avg_rev" in sql.lower()
