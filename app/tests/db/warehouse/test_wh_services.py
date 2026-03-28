"""Unit tests for warehouse.wh_services."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_services
from app.tests.db.warehouse.conftest import SAMPLE_DATE, SAMPLE_DATE_B, SAMPLE_ORG_ID, SAMPLE_SVC_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_service_monthly_performance",
        "get_top_services",
        "get_service_trend",
        "get_service_revenue_ranking",
        "get_declining_services",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_services, name))


@pytest.mark.asyncio
async def test_get_service_monthly_performance(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_services.get_service_monthly_performance(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE
    )
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_top_services_limit(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_services.get_top_services(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE, limit=15)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[2] == 15


@pytest.mark.asyncio
async def test_get_service_trend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_services.get_service_trend(mock_pool, SAMPLE_ORG_ID, SAMPLE_SVC_ID, months=5)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[1] == SAMPLE_SVC_ID
    assert params[2] == 5


@pytest.mark.asyncio
async def test_get_declining_services_join(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_services.get_declining_services(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE, SAMPLE_DATE_B
    )
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert "join" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID
