"""Unit tests for warehouse.wh_clients."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_clients
from app.tests.db.warehouse.conftest import SAMPLE_CUST_ID, SAMPLE_DATE, SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_churned_clients",
        "get_top_clients_by_spend",
        "get_retention_summary",
        "get_new_clients",
        "get_client_detail",
        "get_high_value_clients",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_clients, name))


@pytest.mark.asyncio
async def test_get_churned_clients_limit(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_clients.get_churned_clients(mock_pool, SAMPLE_ORG_ID, limit=50)
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert params[1] == 50


@pytest.mark.asyncio
async def test_get_top_clients_by_spend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_clients.get_top_clients_by_spend(mock_pool, SAMPLE_ORG_ID)
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_retention_summary_fetchrow_none(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    assert await wh_clients.get_retention_summary(mock_pool, SAMPLE_ORG_ID) is None
    sql, params = _sql_params(mock_conn.fetchrow.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_new_clients(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_clients.get_new_clients(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE, limit=30)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[2] == 30


@pytest.mark.asyncio
async def test_get_client_detail(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    await wh_clients.get_client_detail(mock_pool, SAMPLE_ORG_ID, SAMPLE_CUST_ID)
    _sql, params = _sql_params(mock_conn.fetchrow.call_args)
    assert params == [SAMPLE_ORG_ID, SAMPLE_CUST_ID]


@pytest.mark.asyncio
async def test_get_high_value_clients(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_clients.get_high_value_clients(mock_pool, SAMPLE_ORG_ID, 500.0, limit=10)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[1] == 500.0
    assert params[2] == 10
