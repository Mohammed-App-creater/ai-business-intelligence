"""Unit tests for warehouse.wh_attendance."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock

import pytest

from app.services.db.warehouse import wh_attendance
from app.tests.db.warehouse.conftest import SAMPLE_DATE, SAMPLE_EMP_ID, SAMPLE_ORG_ID


def _sql_params(call):
    args, _kwargs = call
    return args[0], list(args[1:])


@pytest.mark.asyncio
async def test_all_async():
    for name in (
        "get_staff_attendance_monthly",
        "get_staff_attendance_trend",
        "get_total_hours_summary",
        "get_low_attendance_staff",
    ):
        assert inspect.iscoroutinefunction(getattr(wh_attendance, name))


@pytest.mark.asyncio
async def test_get_staff_attendance_monthly(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_attendance.get_staff_attendance_monthly(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE
    )
    sql, params = _sql_params(mock_conn.fetch.call_args)
    assert "business_id" in sql.lower()
    assert params[0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_get_staff_attendance_trend(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_attendance.get_staff_attendance_trend(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_EMP_ID, months=3
    )
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[2] == 3


@pytest.mark.asyncio
async def test_get_total_hours_summary(mock_pool, mock_conn):
    mock_conn.fetchrow = AsyncMock(return_value=None)
    assert (
        await wh_attendance.get_total_hours_summary(mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE)
        is None
    )
    sql, _ = _sql_params(mock_conn.fetchrow.call_args)
    assert "sum(total_hours_worked)" in sql.lower()


@pytest.mark.asyncio
async def test_get_low_attendance_staff_min_days(mock_pool, mock_conn):
    mock_conn.fetch = AsyncMock(return_value=[])
    await wh_attendance.get_low_attendance_staff(
        mock_pool, SAMPLE_ORG_ID, SAMPLE_DATE, min_days=2
    )
    _sql, params = _sql_params(mock_conn.fetch.call_args)
    assert params[2] == 2
