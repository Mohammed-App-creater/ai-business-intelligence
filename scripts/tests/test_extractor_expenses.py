"""Tests for ExpensesExtractor."""
from __future__ import annotations

from datetime import timedelta

import pytest

from etl.extractors.expenses import ExpensesExtractor
from scripts.tests.extractor_test_utils import SAMPLE_END, SAMPLE_ORG_ID, SAMPLE_START, make_mock_pool


@pytest.mark.asyncio
async def test_expenses_returns_list() -> None:
    pool, cursor = make_mock_pool()
    out = await ExpensesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_expenses_empty_db_returns_empty_list() -> None:
    pool, cursor = make_mock_pool()
    out = await ExpensesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out == []


@pytest.mark.asyncio
async def test_expenses_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    await ExpensesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert cursor.execute.await_args.args[1][0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_expenses_passes_date_range_to_query() -> None:
    pool, cursor = make_mock_pool()
    end_excl = SAMPLE_END + timedelta(days=1)
    await ExpensesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    p = cursor.execute.await_args.args[1]
    assert p[1] == SAMPLE_START
    assert p[2] == end_excl


@pytest.mark.asyncio
async def test_expenses_query_contains_org_filter() -> None:
    pool, cursor = make_mock_pool()
    await ExpensesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert "OrganizationId" in cursor.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_expenses_output_has_required_keys() -> None:
    row = {
        "business_id": 1,
        "location_id": 0,
        "category_id": 1,
        "category_name": "X",
        "period_start": SAMPLE_START,
        "period_end": SAMPLE_END,
        "total_amount": 1.0,
        "expense_count": 1,
    }
    pool, cursor = make_mock_pool(rows=[row])
    out = await ExpensesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for k in row:
        assert k in out[0]


@pytest.mark.asyncio
async def test_expenses_filters_deleted() -> None:
    import etl.extractors.expenses as mod

    assert "isDeleted = 0" in mod._SQL


@pytest.mark.asyncio
async def test_expenses_null_category_defaulted() -> None:
    import etl.extractors.expenses as mod

    assert "Uncategorised" in mod._SQL
