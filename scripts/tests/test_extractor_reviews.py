"""Tests for ReviewsExtractor."""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from scripts.etl.extractors.reviews import ReviewsExtractor
from scripts.tests.extractor_test_utils import SAMPLE_END, SAMPLE_ORG_ID, SAMPLE_START, make_mock_pool


@pytest.mark.asyncio
async def test_reviews_returns_tuple_of_three_lists() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    emp, visit, google = await ReviewsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert isinstance(emp, list)
    assert isinstance(visit, list)
    assert isinstance(google, list)


@pytest.mark.asyncio
async def test_reviews_empty_db_returns_three_empty_lists() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    emp, visit, google = await ReviewsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert emp == [] and visit == [] and google == []


@pytest.mark.asyncio
async def test_reviews_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    await ReviewsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for c in cursor.execute.await_args_list:
        assert SAMPLE_ORG_ID in c.args[1]


@pytest.mark.asyncio
async def test_reviews_passes_date_range_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    end_excl = SAMPLE_END + timedelta(days=1)
    await ReviewsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for c in cursor.execute.await_args_list:
        assert c.args[1][1] == SAMPLE_START
        assert c.args[1][2] == end_excl


@pytest.mark.asyncio
async def test_reviews_runs_three_queries() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    await ReviewsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert cursor.execute.await_count == 3


@pytest.mark.asyncio
async def test_reviews_emp_query_joins_tbl_emp() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    await ReviewsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    sql_emp = cursor.execute.await_args_list[0].args[0]
    assert "JOIN tbl_emp" in sql_emp


@pytest.mark.asyncio
async def test_reviews_google_query_uses_lowercase_col() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], [], []])
    await ReviewsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    sql_g = cursor.execute.await_args_list[2].args[0]
    assert "organization_id" in sql_g


@pytest.mark.asyncio
async def test_reviews_output_has_required_keys_per_list() -> None:
    emp_row = {
        "business_id": 1,
        "period_start": SAMPLE_START,
        "period_end": SAMPLE_END,
        "emp_review_count": 1,
        "emp_avg_rating": 5.0,
    }
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[emp_row], [], []])
    emp, _, _ = await ReviewsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for k in emp_row:
        assert k in emp[0]
