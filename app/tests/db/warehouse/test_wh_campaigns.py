"""Unit tests for warehouse.wh_campaigns."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_campaigns
from app.tests.db.warehouse.conftest import SAMPLE_CAMPAIGN_ID, SAMPLE_DATE, SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_campaign_history",
        "get_campaign_detail",
        "get_top_campaigns_by_open_rate",
        "get_campaign_monthly_summary",
        "get_recurring_campaigns",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_campaigns, name))


@pytest.mark.asyncio
async def test_get_campaign_history_limit(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_campaigns.get_campaign_history(mock_pool, SAMPLE_ORG_ID, limit=5)
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert params[1] == 5


@pytest.mark.asyncio
async def test_get_campaign_detail(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_campaigns.get_campaign_detail(mock_pool, SAMPLE_ORG_ID, SAMPLE_CAMPAIGN_ID)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[1] == SAMPLE_CAMPAIGN_ID


@pytest.mark.asyncio
async def test_get_top_campaigns_by_open_rate(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_campaigns.get_top_campaigns_by_open_rate(mock_pool, SAMPLE_ORG_ID)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "open_rate" in sql.lower()


@pytest.mark.asyncio
async def test_get_campaign_monthly_summary(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_campaigns.get_campaign_monthly_summary(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "execution_date" in sql.lower()


@pytest.mark.asyncio
async def test_get_recurring_campaigns(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_campaigns.get_recurring_campaigns(mock_pool, SAMPLE_ORG_ID)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "is_recurring" in sql.lower()
