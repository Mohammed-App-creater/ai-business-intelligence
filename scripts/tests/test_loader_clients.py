"""Tests for ClientsLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from scripts.etl.loaders.clients import ClientsLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "customer_id": 500,
        "first_visit_date": date(2025, 6, 1),
        "last_visit_date": date(2026, 2, 1),
        "total_visits": 10,
        "total_spend": 800.0,
        "avg_spend_per_visit": 80.0,
        "loyalty_points": 100,
        "days_since_last_visit": 5,
        "visit_frequency_days": 30.0,
        "is_churned": False,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_clients_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ClientsLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_clients_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ClientsLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_clients_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ClientsLoader(pool)
    base = _sample_row()
    rows = [dict(base, customer_id=cid) for cid in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_clients_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ClientsLoader(pool)
    base = _sample_row()
    rows = [dict(base, customer_id=cid) for cid in (10, 11, 12)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_clients_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ClientsLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_client_metrics" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_clients_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ClientsLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_clients_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ClientsLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_clients_param_fn_returns_correct_tuple_length() -> None:
    assert len(ClientsLoader._param_fn(_sample_row())) == 11


def test_clients_param_fn_length_is_11() -> None:
    assert len(ClientsLoader._param_fn(_sample_row())) == 11


def test_clients_nullable_dates_pass_through() -> None:
    row = _sample_row()
    row["first_visit_date"] = None
    row["last_visit_date"] = None
    row["days_since_last_visit"] = None
    row["visit_frequency_days"] = None
    t = ClientsLoader._param_fn(row)
    assert t[2] is None
    assert t[3] is None
    assert t[8] is None
    assert t[9] is None


def test_clients_conflict_target_has_customer_id() -> None:
    assert "ON CONFLICT (business_id, customer_id)" in ClientsLoader._SQL
