"""Unit tests for warehouse.wh_reviews."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_reviews
from app.tests.db.warehouse.conftest import SAMPLE_DATE, SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_review_monthly_summary",
        "get_review_trend",
        "get_google_review_trend",
        "get_rating_decline_periods",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_reviews, name))


@pytest.mark.asyncio
async def test_get_review_monthly_summary_none(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    assert (
        await wh_reviews.get_review_monthly_summary(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE)
        is None
    )
    sql, params = _sql_params(mock_conn.fetchrow.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_review_trend_months(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_reviews.get_review_trend(mock_pool, SAMPLE_ORG_ID, months=2)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[1] == 2


@pytest.mark.asyncio
async def test_get_google_review_trend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_reviews.get_google_review_trend(mock_pool, SAMPLE_ORG_ID)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "google_avg_rating" in sql.lower()


@pytest.mark.asyncio
async def test_get_rating_decline_threshold(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_reviews.get_rating_decline_periods(mock_pool, SAMPLE_ORG_ID, threshold=3.0)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[1] == 3.0
