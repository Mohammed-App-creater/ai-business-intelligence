"""Unit tests for warehouse.wh_expenses."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_expenses
from app.tests.db.warehouse.conftest import SAMPLE_DATE, SAMPLE_DATE_B, SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_expense_monthly_summary",
        "get_expense_trend",
        "get_top_expense_categories",
        "get_expense_total",
        "get_expense_comparison",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_expenses, name))


@pytest.mark.asyncio
async def test_get_expense_monthly_summary(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_expenses.get_expense_monthly_summary(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE)
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_expense_trend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_expenses.get_expense_trend(mock_pool, SAMPLE_ORG_ID, category_id=9, months=3)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[1] == 9
    assert params[2] == 3


@pytest.mark.asyncio
async def test_get_expense_total_fetchrow(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    assert (
        await wh_expenses.get_expense_total(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE) is None
    )
    sql, _ = _sql_params(mock_conn.fetchrow.call_args)
    assert "location_id" in sql.lower()


@pytest.mark.asyncio
async def test_get_expense_comparison(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_expenses.get_expense_comparison(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE_B, SAMPLE_DATE
    )
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "period_a_amount" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID
