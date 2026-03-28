"""Tests for StaffExtractor."""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from scripts.etl.extractors.staff import StaffExtractor
from scripts.tests.extractor_test_utils import SAMPLE_END, SAMPLE_ORG_ID, SAMPLE_START, make_mock_pool


@pytest.mark.asyncio
async def test_staff_returns_list() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], [], []])
    out = await StaffExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_staff_empty_db_returns_empty_list() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], [], []])
    out = await StaffExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out == []


@pytest.mark.asyncio
async def test_staff_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], [], []])
    await StaffExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for c in cursor.execute.await_args_list:
        assert SAMPLE_ORG_ID in c.args[1]


@pytest.mark.asyncio
async def test_staff_passes_date_range_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], [], []])
    end_excl = SAMPLE_END + timedelta(days=1)
    await StaffExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for c in cursor.execute.await_args_list:
        assert c.args[1][1] == SAMPLE_START
        assert c.args[1][2] == end_excl


@pytest.mark.asyncio
async def test_staff_query_contains_org_filter() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], [], []])
    await StaffExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    sql_a = cursor.execute.await_args_list[0].args[0]
    assert "OrganizationId" in sql_a


@pytest.mark.asyncio
async def test_staff_runs_four_queries() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], [], []])
    await StaffExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert cursor.execute.await_count == 4


@pytest.mark.asyncio
async def test_staff_merges_all_sources() -> None:
    q_a = [
        {
            "business_id": 1,
            "employee_id": 9,
            "employee_name": "John Doe",
            "period_start": SAMPLE_START,
            "period_end": SAMPLE_END,
            "total_visits": 2,
            "total_revenue": 50.0,
            "total_commission": 5.0,
        }
    ]
    q_tips = [{"employee_id": 9, "period_start": SAMPLE_START, "period_end": SAMPLE_END, "total_tips": 3.0}]
    q_appt = [
        {
            "business_id": 1,
            "employee_id": 9,
            "period_start": SAMPLE_START,
            "period_end": SAMPLE_END,
            "appointments_booked": 4,
            "appointments_completed": 3,
            "appointments_cancelled": 1,
        }
    ]
    q_rate = [
        {
            "business_id": 1,
            "employee_id": 9,
            "period_start": SAMPLE_START,
            "period_end": SAMPLE_END,
            "avg_rating": 4.5,
            "review_count": 2,
        }
    ]
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[q_a, q_tips, q_appt, q_rate])
    out = await StaffExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert len(out) == 1
    r = out[0]
    assert r["total_tips"] == 3.0
    assert r["appointments_booked"] == 4
    assert r["avg_rating"] == 4.5


@pytest.mark.asyncio
async def test_staff_missing_rating_gives_none() -> None:
    q_a = [
        {
            "business_id": 1,
            "employee_id": 9,
            "employee_name": "John Doe",
            "period_start": SAMPLE_START,
            "period_end": SAMPLE_END,
            "total_visits": 1,
            "total_revenue": 10.0,
            "total_commission": 1.0,
        }
    ]
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[q_a, [], [], []])
    out = await StaffExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out[0]["avg_rating"] is None
    assert out[0]["review_count"] == 0


@pytest.mark.asyncio
async def test_staff_missing_appointments_gives_zero() -> None:
    q_a = [
        {
            "business_id": 1,
            "employee_id": 9,
            "employee_name": "John Doe",
            "period_start": SAMPLE_START,
            "period_end": SAMPLE_END,
            "total_visits": 1,
            "total_revenue": 10.0,
            "total_commission": 1.0,
        }
    ]
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[q_a, [], [], []])
    out = await StaffExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out[0]["appointments_booked"] == 0


@pytest.mark.asyncio
async def test_staff_employee_name_concatenated() -> None:
    q_a = [
        {
            "business_id": 1,
            "employee_id": 9,
            "employee_name": "John Doe",
            "period_start": SAMPLE_START,
            "period_end": SAMPLE_END,
            "total_visits": 1,
            "total_revenue": 0.0,
            "total_commission": 0.0,
        }
    ]
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[q_a, [], [], []])
    out = await StaffExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out[0]["employee_name"] == "John Doe"
    assert "John" in out[0]["employee_name"]
