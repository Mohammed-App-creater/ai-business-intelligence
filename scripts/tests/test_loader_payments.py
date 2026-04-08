"""Tests for PaymentsLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from etl.loaders.payments import PaymentsLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "location_id": 0,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "cash_amount": 100.0,
        "cash_count": 5,
        "card_amount": 200.0,
        "card_count": 8,
        "gift_card_amount": 10.0,
        "gift_card_count": 1,
        "other_amount": 5.0,
        "other_count": 2,
        "total_amount": 315.0,
        "total_count": 16,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_payments_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = PaymentsLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_payments_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = PaymentsLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_payments_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = PaymentsLoader(pool)
    base = _sample_row()
    rows = [dict(base, location_id=lid) for lid in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_payments_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = PaymentsLoader(pool)
    base = _sample_row()
    rows = [dict(base, location_id=lid) for lid in (4, 5, 6)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_payments_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = PaymentsLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_payment_breakdown" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_payments_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = PaymentsLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_payments_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = PaymentsLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_payments_param_fn_returns_correct_tuple_length() -> None:
    assert len(PaymentsLoader._param_fn(_sample_row())) == 14


def test_payments_param_fn_length_is_14() -> None:
    assert len(PaymentsLoader._param_fn(_sample_row())) == 14


def test_payments_conflict_has_location_id() -> None:
    assert "ON CONFLICT (business_id, location_id, period_start)" in PaymentsLoader._SQL
