"""Tests for CampaignsLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from scripts.etl.loaders.campaigns import CampaignsLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "campaign_id": 100,
        "campaign_name": "Spring",
        "execution_date": date(2026, 3, 1),
        "is_recurring": False,
        "total_sent": 1000,
        "successful_sent": 990,
        "failed_count": 10,
        "opened_count": 400,
        "clicked_count": 50,
        "open_rate": 40.0,
        "click_rate": 5.0,
        "fail_rate": 1.0,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_campaigns_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = CampaignsLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_campaigns_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = CampaignsLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_campaigns_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = CampaignsLoader(pool)
    base = _sample_row()
    rows = [dict(base, execution_date=date(2026, 3, d)) for d in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_campaigns_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = CampaignsLoader(pool)
    base = _sample_row()
    rows = [dict(base, execution_date=date(2026, 4, d)) for d in (1, 2, 3)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_campaigns_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = CampaignsLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_campaign_performance" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_campaigns_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = CampaignsLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_campaigns_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = CampaignsLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_campaigns_param_fn_returns_correct_tuple_length() -> None:
    assert len(CampaignsLoader._param_fn(_sample_row())) == 13


def test_campaigns_param_fn_length_is_13() -> None:
    assert len(CampaignsLoader._param_fn(_sample_row())) == 13


def test_campaigns_bool_is_recurring_preserved() -> None:
    row = _sample_row()
    row["is_recurring"] = True
    t = CampaignsLoader._param_fn(row)
    assert t[4] is True


def test_campaigns_conflict_has_campaign_id() -> None:
    assert "ON CONFLICT (business_id, campaign_id, execution_date)" in CampaignsLoader._SQL
