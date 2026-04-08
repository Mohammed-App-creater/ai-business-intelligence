"""Tests for ReviewsLoader — mocked warehouse pool only."""
from __future__ import annotations

from datetime import date

import pytest

from etl.loaders.reviews import ReviewsLoader
from scripts.tests.conftest import make_mock_wh_pool


def _sample_row() -> dict:
    return {
        "business_id": 1,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "emp_review_count": 5,
        "emp_avg_rating": 4.2,
        "visit_review_count": 10,
        "visit_avg_rating": 4.0,
        "google_review_count": 3,
        "google_avg_rating": 4.5,
        "google_bad_review_count": 0,
        "total_review_count": 18,
        "overall_avg_rating": 4.22,
    }


def _sql_from_first_execute(conn) -> str:
    return conn.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_reviews_load_empty_rows_returns_zero_counts() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ReviewsLoader(pool)
    ins, upd = await loader.load([])
    assert (ins, upd) == (0, 0)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_reviews_load_single_row_calls_execute_once() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ReviewsLoader(pool)
    await loader.load([_sample_row()])
    assert conn.execute.await_count == 1


@pytest.mark.asyncio
async def test_reviews_load_multiple_rows_calls_execute_per_row() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ReviewsLoader(pool)
    base = _sample_row()
    rows = [dict(base, business_id=bid) for bid in (1, 2, 3)]
    await loader.load(rows)
    assert conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_reviews_returns_inserted_count_matching_row_count() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ReviewsLoader(pool)
    base = _sample_row()
    rows = [dict(base, business_id=bid) for bid in (10, 11, 12)]
    ins, upd = await loader.load(rows)
    assert ins == 3
    assert upd == 0


@pytest.mark.asyncio
async def test_reviews_sql_contains_insert_into_correct_table() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ReviewsLoader(pool)
    await loader.load([_sample_row()])
    assert "INSERT INTO wh_review_summary" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_reviews_sql_contains_on_conflict() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ReviewsLoader(pool)
    await loader.load([_sample_row()])
    assert "ON CONFLICT" in _sql_from_first_execute(conn)


@pytest.mark.asyncio
async def test_reviews_sql_contains_do_update() -> None:
    pool, conn = make_mock_wh_pool()
    loader = ReviewsLoader(pool)
    await loader.load([_sample_row()])
    assert "DO UPDATE SET" in _sql_from_first_execute(conn)


def test_reviews_param_fn_returns_correct_tuple_length() -> None:
    assert len(ReviewsLoader._param_fn(_sample_row())) == 12


def test_reviews_param_fn_length_is_12() -> None:
    assert len(ReviewsLoader._param_fn(_sample_row())) == 12


def test_reviews_null_ratings_pass_through() -> None:
    row = _sample_row()
    row["emp_avg_rating"] = None
    t = ReviewsLoader._param_fn(row)
    assert t[4] is None


def test_reviews_conflict_has_period_start_only() -> None:
    assert "ON CONFLICT (business_id, period_start)" in ReviewsLoader._SQL
