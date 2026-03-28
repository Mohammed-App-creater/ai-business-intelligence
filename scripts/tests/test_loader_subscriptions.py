"""Tests for SubscriptionsLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from scripts.etl.loaders.subscriptions import SubscriptionsLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "location_id": 0,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "active_subscriptions": 50,
        "new_subscriptions": 5,
        "cancelled_subscriptions": 2,
        "gross_subscription_revenue": 5000.0,
        "net_subscription_revenue": 4500.0,
        "avg_subscription_value": 100.0,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_subscriptions_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = SubscriptionsLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_subscriptions_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = SubscriptionsLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_subscriptions_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = SubscriptionsLoader(pool)
    base = _sample_row()
    rows = [dict(base, location_id=lid) for lid in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_subscriptions_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = SubscriptionsLoader(pool)
    base = _sample_row()
    rows = [dict(base, location_id=lid) for lid in (4, 5, 6)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_subscriptions_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = SubscriptionsLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_subscription_revenue" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_subscriptions_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = SubscriptionsLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_subscriptions_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = SubscriptionsLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_subscriptions_param_fn_returns_correct_tuple_length() -> None:
    assert len(SubscriptionsLoader._param_fn(_sample_row())) == 10


def test_subscriptions_param_fn_length_is_10() -> None:
    assert len(SubscriptionsLoader._param_fn(_sample_row())) == 10


def test_subscriptions_conflict_has_location_id() -> None:
    assert "ON CONFLICT (business_id, location_id, period_start)" in SubscriptionsLoader._SQL
