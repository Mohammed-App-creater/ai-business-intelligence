"""Tests for ServicesLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from scripts.etl.loaders.services import ServicesLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "service_id": 9,
        "service_name": "Cut",
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "booking_count": 100,
        "revenue": 5000.0,
        "avg_price": 50.0,
        "min_price": 40.0,
        "max_price": 80.0,
        "unique_customers": 60,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_services_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ServicesLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_services_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ServicesLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_services_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ServicesLoader(pool)
    base = _sample_row()
    rows = [dict(base, service_id=sid) for sid in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_services_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ServicesLoader(pool)
    base = _sample_row()
    rows = [dict(base, service_id=sid) for sid in (4, 5, 6)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_services_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ServicesLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_service_performance" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_services_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ServicesLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_services_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ServicesLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_services_param_fn_returns_correct_tuple_length() -> None:
    assert len(ServicesLoader._param_fn(_sample_row())) == 11


def test_services_param_fn_length_is_11() -> None:
    assert len(ServicesLoader._param_fn(_sample_row())) == 11


def test_services_conflict_has_service_id() -> None:
    assert "ON CONFLICT (business_id, service_id, period_start)" in ServicesLoader._SQL
