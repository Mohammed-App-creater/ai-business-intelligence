"""Unit tests for warehouse.wh_appointments."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_appointments
from app.tests.db.warehouse.conftest import SAMPLE_DATE, SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_appointment_monthly_summary",
        "get_appointment_trend",
        "get_cancellation_rate_trend",
        "get_walkin_vs_booked_trend",
        "get_location_appointment_comparison",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_appointments, name))


@pytest.mark.asyncio
async def test_get_appointment_monthly_summary_none(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    assert (
        await wh_appointments.get_appointment_monthly_summary(
            mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE
        )
        is None
    )
    sql, params = _sql_params(mock_conn.fetchrow.call_args)
    assert "business_id" in sql.lower()
    assert "location_id" in sql.lower()


@pytest.mark.asyncio
async def test_get_appointment_trend_months(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_appointments.get_appointment_trend(mock_pool, SAMPLE_ORG_ID, months=4)
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[1] == 4


@pytest.mark.asyncio
async def test_get_cancellation_rate_trend_columns(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_appointments.get_cancellation_rate_trend(mock_pool, SAMPLE_ORG_ID)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "cancellation_rate" in sql.lower()


@pytest.mark.asyncio
async def test_get_walkin_vs_booked_trend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_appointments.get_walkin_vs_booked_trend(mock_pool, SAMPLE_ORG_ID)
    sql, _ = _sql_params(mock_conn.fetch.call_args)
    assert "walkin_count" in sql.lower()


@pytest.mark.asyncio
async def test_get_location_appointment_comparison(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_appointments.get_location_appointment_comparison(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE
    )
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[0] == SAMPLE_ORG_ID
