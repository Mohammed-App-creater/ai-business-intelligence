"""Tests for ExpensesLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from etl.loaders.expenses import ExpensesLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "location_id": 0,
        "category_id": 7,
        "category_name": "Rent",
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "total_amount": 1200.0,
        "expense_count": 1,
        "avg_expense": 1200.0,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_expenses_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ExpensesLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_expenses_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ExpensesLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_expenses_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ExpensesLoader(pool)
    base = _sample_row()
    rows = [dict(base, category_id=cid) for cid in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_expenses_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ExpensesLoader(pool)
    base = _sample_row()
    rows = [dict(base, category_id=cid) for cid in (10, 11, 12)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_expenses_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ExpensesLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_expense_summary" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_expenses_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ExpensesLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_expenses_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ExpensesLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_expenses_param_fn_returns_correct_tuple_length() -> None:
    assert len(ExpensesLoader._param_fn(_sample_row())) == 9


def test_expenses_param_fn_length_is_9() -> None:
    assert len(ExpensesLoader._param_fn(_sample_row())) == 9


def test_expenses_conflict_has_category_id() -> None:
    assert (
        "ON CONFLICT (business_id, location_id, category_id, period_start)"
        in ExpensesLoader._SQL
    )
