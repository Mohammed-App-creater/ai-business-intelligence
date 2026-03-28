"""Unit tests for warehouse.wh_payments."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_payments
from app.tests.db.warehouse.conftest import SAMPLE_DATE, SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_payment_monthly_breakdown",
        "get_payment_trend",
        "get_cash_vs_card_trend",
        "get_gift_card_usage_trend",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_payments, name))


@pytest.mark.asyncio
async def test_get_payment_monthly_breakdown(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    await wh_payments.get_payment_monthly_breakdown(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE)
    sql, params = _sql_params(mock_conn.fetchrow.call_args)
    assert "business_id" in sql.lower()
    assert "location_id" in sql.lower()


@pytest.mark.asyncio
async def test_get_payment_trend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_payments.get_payment_trend(mock_pool, SAMPLE_ORG_ID, months=8)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[1] == 8


@pytest.mark.asyncio
async def test_get_cash_vs_card_trend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_payments.get_cash_vs_card_trend(mock_pool, SAMPLE_ORG_ID)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "cash_amount" in sql.lower()


@pytest.mark.asyncio
async def test_get_gift_card_usage_trend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_payments.get_gift_card_usage_trend(mock_pool, SAMPLE_ORG_ID)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "gift_card_amount" in sql.lower()
