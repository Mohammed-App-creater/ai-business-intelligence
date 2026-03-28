"""Tests for RevenueLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from scripts.etl.loaders.revenue import RevenueLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 7,
        "location_id": 2,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "total_revenue": 100.0,
        "total_tips": 10.0,
        "total_tax": 5.0,
        "total_discounts": 1.0,
        "total_gc_amount": 0.0,
        "gross_revenue": 114.0,
        "visit_count": 5,
        "successful_visit_count": 4,
        "refunded_visit_count": 0,
        "cancelled_visit_count": 1,
        "avg_visit_value": 22.8,
        "cash_revenue": 50.0,
        "card_revenue": 50.0,
        "other_revenue": 0.0,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_revenue_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = RevenueLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_revenue_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = RevenueLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_revenue_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = RevenueLoader(pool)
    r = _sample_row()
    rows = [dict(r, business_id=i) for i in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_revenue_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = RevenueLoader(pool)
    r = _sample_row()
    ins, upd = await loader.load([r, dict(r, business_id=99), dict(r, business_id=100)])
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_revenue_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = RevenueLoader(pool)
    await loader.load([_sample_row()])
    sql = _sql_from_first_execute(conn)
    assert "INSERT INTO wh_monthly_revenue" in sql


@pytest.mark.asyncio
async def test_revenue_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = RevenueLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_revenue_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = RevenueLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_revenue_param_fn_returns_correct_tuple_length() -> None:
    t = RevenueLoader._param_fn(_sample_row())
    assert len(t) == 18


def test_revenue_param_fn_length_is_18() -> None:
    assert len(RevenueLoader._param_fn(_sample_row())) == 18


def test_revenue_sql_conflict_target_correct() -> None:
    assert "ON CONFLICT (business_id, location_id, period_start)" in RevenueLoader._SQL


def test_revenue_none_values_passed_through() -> None:
    row = _sample_row()
    row["total_tax"] = None
    t = RevenueLoader._param_fn(row)
    assert t[6] is None
