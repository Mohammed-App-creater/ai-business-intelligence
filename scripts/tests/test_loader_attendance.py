"""Tests for AttendanceLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from etl.loaders.attendance import AttendanceLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "employee_id": 5,
        "employee_name": "Sam",
        "location_id": 2,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "days_worked": 20,
        "total_hours_worked": 160.0,
        "avg_hours_per_day": 8.0,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_attendance_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AttendanceLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_attendance_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AttendanceLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_attendance_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AttendanceLoader(pool)
    base = _sample_row()
    rows = [dict(base, employee_id=eid) for eid in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_attendance_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AttendanceLoader(pool)
    base = _sample_row()
    rows = [dict(base, employee_id=eid) for eid in (10, 11, 12)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_attendance_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AttendanceLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_attendance_summary" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_attendance_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AttendanceLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_attendance_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AttendanceLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_attendance_param_fn_returns_correct_tuple_length() -> None:
    assert len(AttendanceLoader._param_fn(_sample_row())) == 9


def test_attendance_param_fn_length_is_9() -> None:
    assert len(AttendanceLoader._param_fn(_sample_row())) == 9


def test_attendance_conflict_has_4_columns() -> None:
    assert (
        "ON CONFLICT (business_id, employee_id, location_id, period_start)"
        in AttendanceLoader._SQL
    )
