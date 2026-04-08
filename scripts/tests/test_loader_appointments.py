"""Tests for AppointmentsLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from etl.loaders.appointments import AppointmentsLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "location_id": 3,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "total_booked": 200,
        "confirmed_count": 180,
        "completed_count": 150,
        "cancelled_count": 20,
        "no_show_count": 10,
        "walkin_count": 5,
        "app_booking_count": 100,
        "cancellation_rate": 10.0,
        "completion_rate": 83.33,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_appointments_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AppointmentsLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_appointments_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AppointmentsLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_appointments_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AppointmentsLoader(pool)
    base = _sample_row()
    rows = [dict(base, location_id=lid) for lid in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_appointments_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AppointmentsLoader(pool)
    base = _sample_row()
    rows = [dict(base, location_id=lid) for lid in (4, 5, 6)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_appointments_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AppointmentsLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_appointment_metrics" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_appointments_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AppointmentsLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_appointments_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = AppointmentsLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_appointments_param_fn_returns_correct_tuple_length() -> None:
    assert len(AppointmentsLoader._param_fn(_sample_row())) == 13


def test_appointments_param_fn_length_is_13() -> None:
    assert len(AppointmentsLoader._param_fn(_sample_row())) == 13


def test_appointments_conflict_has_location_id() -> None:
    assert "ON CONFLICT (business_id, location_id, period_start)" in AppointmentsLoader._SQL
