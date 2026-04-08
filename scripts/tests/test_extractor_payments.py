"""Tests for PaymentsExtractor."""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from etl.extractors.payments import PaymentsExtractor
from scripts.tests.extractor_test_utils import SAMPLE_END, SAMPLE_ORG_ID, SAMPLE_START, make_mock_pool


@pytest.mark.asyncio
async def test_payments_returns_list() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    out = await PaymentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_payments_empty_db_returns_empty_list() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    out = await PaymentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out == []


@pytest.mark.asyncio
async def test_payments_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    await PaymentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for c in cursor.execute.await_args_list:
        assert SAMPLE_ORG_ID in c.args[1]


@pytest.mark.asyncio
async def test_payments_passes_date_range_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    end_excl = SAMPLE_END + timedelta(days=1)
    await PaymentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for c in cursor.execute.await_args_list:
        assert c.args[1][1] == SAMPLE_START
        assert c.args[1][2] == end_excl


@pytest.mark.asyncio
async def test_payments_query_contains_org_filter() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    await PaymentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert "OrganizationId" in cursor.execute.await_args_list[0].args[0]


@pytest.mark.asyncio
async def test_payments_runs_two_queries() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    await PaymentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert cursor.execute.await_count == 2


@pytest.mark.asyncio
async def test_payments_output_has_payment_type_key() -> None:
    row = {
        "business_id": 1,
        "location_id": 0,
        "period_start": SAMPLE_START,
        "period_end": SAMPLE_END,
        "payment_type": "Cash",
        "amount": 10.0,
        "count": 2,
    }
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[row], []])
    out = await PaymentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert "payment_type" in out[0]


@pytest.mark.asyncio
async def test_payments_rollup_has_location_zero() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[{"location_id": 1}], [{"location_id": 0}]])
    out = await PaymentsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert any(r.get("location_id") == 0 for r in out)
