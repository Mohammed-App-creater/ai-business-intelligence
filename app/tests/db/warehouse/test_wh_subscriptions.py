"""Unit tests for warehouse.wh_subscriptions."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_subscriptions
from app.tests.db.warehouse.conftest import SAMPLE_DATE, SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_subscription_monthly_summary",
        "get_subscription_trend",
        "get_mrr_trend",
        "get_subscription_growth",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_subscriptions, name))


@pytest.mark.asyncio
async def test_get_subscription_monthly_summary(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    await wh_subscriptions.get_subscription_monthly_summary(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE
    )
    sql, params = _sql_params(mock_conn.fetchrow.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_subscription_trend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_subscriptions.get_subscription_trend(mock_pool, SAMPLE_ORG_ID, months=7)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[1] == 7


@pytest.mark.asyncio
async def test_get_mrr_trend_columns(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_subscriptions.get_mrr_trend(mock_pool, SAMPLE_ORG_ID)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "net_subscription_revenue" in sql.lower()


@pytest.mark.asyncio
async def test_get_subscription_growth(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_subscriptions.get_subscription_growth(mock_pool, SAMPLE_ORG_ID)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "new_subscriptions" in sql.lower()
