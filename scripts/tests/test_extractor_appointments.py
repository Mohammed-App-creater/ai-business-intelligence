"""Tests for AppointmentsExtractor."""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from etl.extractors.appointments import AppointmentsExtractor
from scripts.tests.extractor_test_utils import SAMPLE_END, SAMPLE_ORG_ID, SAMPLE_START, make_mock_pool


@pytest.mark.asyncio
async def test_appointments_returns_list() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    out = await AppointmentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_appointments_empty_db_returns_empty_list() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    out = await AppointmentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out == []


@pytest.mark.asyncio
async def test_appointments_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    await AppointmentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for c in cursor.execute.await_args_list:
        assert SAMPLE_ORG_ID in c.args[1]


@pytest.mark.asyncio
async def test_appointments_passes_date_range_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    end_excl = SAMPLE_END + timedelta(days=1)
    await AppointmentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for c in cursor.execute.await_args_list:
        assert c.args[1][1] == SAMPLE_START
        assert c.args[1][2] == end_excl


@pytest.mark.asyncio
async def test_appointments_query_contains_org_filter() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    await AppointmentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert "OrganizationId" in cursor.execute.await_args_list[0].args[0]
    assert "OrgId" in cursor.execute.await_args_list[2].args[0]


@pytest.mark.asyncio
async def test_appointments_runs_three_queries() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    await AppointmentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert cursor.execute.await_count == 3


@pytest.mark.asyncio
async def test_appointments_merges_walkin_count() -> None:
    cal = [
        {
            "business_id": 1,
            "location_id": 2,
            "period_start": SAMPLE_START,
            "period_end": SAMPLE_END,
            "total_booked": 1,
            "confirmed_count": 1,
            "completed_count": 1,
            "cancelled_count": 0,
            "no_show_count": 0,
        }
    ]
    signin = [
        {
            "business_id": 1,
            "location_id": 2,
            "period_start": SAMPLE_START,
            "walkin_count": 3,
            "app_booking_count": 1,
        }
    ]
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[cal, [], signin])
    out = await AppointmentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    match = next(r for r in out if r["location_id"] == 2)
    assert match["walkin_count"] == 3
    assert match["app_booking_count"] == 1


@pytest.mark.asyncio
async def test_appointments_missing_walkin_gives_zero() -> None:
    cal = [
        {
            "business_id": 1,
            "location_id": 2,
            "period_start": SAMPLE_START,
            "period_end": SAMPLE_END,
            "total_booked": 1,
            "confirmed_count": 1,
            "completed_count": 1,
            "cancelled_count": 0,
            "no_show_count": 0,
        }
    ]
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[cal, [], []])
    out = await AppointmentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out[0]["walkin_count"] == 0


@pytest.mark.asyncio
async def test_appointments_output_has_required_keys() -> None:
    cal = [
        {
            "business_id": 1,
            "location_id": 0,
            "period_start": SAMPLE_START,
            "period_end": SAMPLE_END,
            "total_booked": 0,
            "confirmed_count": 0,
            "completed_count": 0,
            "cancelled_count": 0,
            "no_show_count": 0,
        }
    ]
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[cal, [], []])
    out = await AppointmentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for k in ("walkin_count", "app_booking_count", "total_booked"):
        assert k in out[0]
