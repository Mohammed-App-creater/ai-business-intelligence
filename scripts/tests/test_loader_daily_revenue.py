"""Tests for DailyRevenueLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from scripts.etl.loaders.daily_revenue import DailyRevenueLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "location_id": 0,
        "revenue_date": date(2026, 3, 15),
        "total_revenue": 200.0,
        "total_tips": 20.0,
        "total_tax": 10.0,
        "total_discounts": 2.0,
        "gross_revenue": 228.0,
        "visit_count": 8,
        "successful_visit_count": 7,
        "avg_visit_value": 28.5,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_daily_revenue_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = DailyRevenueLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_revenue_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = DailyRevenueLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_daily_revenue_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = DailyRevenueLoader(pool)
    base = _sample_row()
    rows = [dict(base, revenue_date=date(2026, 3, d)) for d in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_daily_revenue_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = DailyRevenueLoader(pool)
    base = _sample_row()
    rows = [dict(base, revenue_date=date(2026, 3, d)) for d in (10, 11, 12)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_daily_revenue_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = DailyRevenueLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_daily_revenue" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_daily_revenue_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = DailyRevenueLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_daily_revenue_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = DailyRevenueLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_daily_revenue_param_fn_returns_correct_tuple_length() -> None:
    assert len(DailyRevenueLoader._param_fn(_sample_row())) == 11


def test_daily_revenue_param_fn_length_is_11() -> None:
    assert len(DailyRevenueLoader._param_fn(_sample_row())) == 11


def test_daily_revenue_conflict_target_has_revenue_date() -> None:
    assert "ON CONFLICT (business_id, location_id, revenue_date)" in DailyRevenueLoader._SQL
