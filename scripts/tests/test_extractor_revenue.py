"""Tests for RevenueExtractor."""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from scripts.etl.extractors.revenue import RevenueExtractor
from scripts.tests.extractor_test_utils import SAMPLE_END, SAMPLE_ORG_ID, SAMPLE_START, make_mock_pool

EXPECTED_KEYS = {
    "business_id",
    "location_id",
    "period_start",
    "period_end",
    "total_revenue",
    "total_tips",
    "total_tax",
    "total_discounts",
    "total_gc_amount",
    "gross_revenue",
    "visit_count",
    "successful_visit_count",
    "refunded_visit_count",
    "cancelled_visit_count",
    "cash_revenue",
    "card_revenue",
    "other_revenue",
}


@pytest.mark.asyncio
async def test_revenue_returns_list() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    out = await RevenueExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_revenue_empty_db_returns_empty_list() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    out = await RevenueExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out == []


@pytest.mark.asyncio
async def test_revenue_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    await RevenueExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for call in cursor.execute.await_args_list:
        assert SAMPLE_ORG_ID in call.args[1]


@pytest.mark.asyncio
async def test_revenue_passes_date_range_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    end_excl = SAMPLE_END + timedelta(days=1)
    await RevenueExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for call in cursor.execute.await_args_list:
        params = call.args[1]
        assert params[1] == SAMPLE_START
        assert params[2] == end_excl


@pytest.mark.asyncio
async def test_revenue_query_contains_org_filter() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    await RevenueExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    sql0 = cursor.execute.await_args_list[0].args[0]
    assert "WHERE" in sql0.upper()
    assert "OrganizationId" in sql0


@pytest.mark.asyncio
async def test_revenue_runs_two_queries() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[{"business_id": 1}], [{"location_id": 0}]])
    await RevenueExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert cursor.execute.await_count == 2


@pytest.mark.asyncio
async def test_revenue_rollup_has_location_id_zero() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(
        side_effect=[[{"business_id": 1, "location_id": 3}], [{"business_id": 1, "location_id": 0}]]
    )
    out = await RevenueExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert any(r.get("location_id") == 0 for r in out)


@pytest.mark.asyncio
async def test_revenue_output_keys_correct() -> None:
    row = {k: 0 for k in EXPECTED_KEYS}
    row["business_id"] = 1
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[row], []])
    out = await RevenueExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert set(out[0].keys()) >= EXPECTED_KEYS


@pytest.mark.asyncio
async def test_revenue_output_has_required_keys_single_row() -> None:
    pool, cursor = make_mock_pool()
    one = dict.fromkeys(EXPECTED_KEYS, 0)
    one["business_id"] = SAMPLE_ORG_ID
    cursor.fetchall = AsyncMock(side_effect=[[one], []])
    out = await RevenueExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for k in EXPECTED_KEYS:
        assert k in out[0]
