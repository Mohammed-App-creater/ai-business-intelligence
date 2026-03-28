"""Tests for StaffLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from scripts.etl.loaders.staff import StaffLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "employee_id": 42,
        "employee_name": "Alex",
        "period_start": date(2026, 2, 1),
        "period_end": date(2026, 2, 28),
        "total_visits": 30,
        "total_revenue": 3000.0,
        "total_tips": 100.0,
        "total_commission": 150.0,
        "appointments_booked": 35,
        "appointments_completed": 30,
        "appointments_cancelled": 5,
        "avg_rating": 4.5,
        "review_count": 12,
        "utilisation_rate": 85.0,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_staff_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = StaffLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_staff_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = StaffLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_staff_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = StaffLoader(pool)
    base = _sample_row()
    rows = [dict(base, employee_id=eid) for eid in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_staff_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = StaffLoader(pool)
    base = _sample_row()
    rows = [dict(base, employee_id=eid) for eid in (10, 11, 12)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_staff_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = StaffLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_staff_performance" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_staff_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = StaffLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_staff_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = StaffLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_staff_param_fn_returns_correct_tuple_length() -> None:
    assert len(StaffLoader._param_fn(_sample_row())) == 15


def test_staff_param_fn_length_is_15() -> None:
    assert len(StaffLoader._param_fn(_sample_row())) == 15


def test_staff_avg_rating_none_in_tuple() -> None:
    row = _sample_row()
    row["avg_rating"] = None
    t = StaffLoader._param_fn(row)
    assert t[12] is None


def test_staff_conflict_has_employee_id() -> None:
    assert "ON CONFLICT (business_id, employee_id, period_start)" in StaffLoader._SQL
