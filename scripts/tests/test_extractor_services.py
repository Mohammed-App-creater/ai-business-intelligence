"""Tests for ServicesExtractor."""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from etl.extractors.services import ServicesExtractor
from scripts.tests.extractor_test_utils import SAMPLE_END, SAMPLE_ORG_ID, SAMPLE_START, make_mock_pool


@pytest.mark.asyncio
async def test_services_returns_list() -> None:
    pool, cursor = make_mock_pool()
    out = await ServicesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_services_empty_db_returns_empty_list() -> None:
    pool, cursor = make_mock_pool()
    out = await ServicesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out == []


@pytest.mark.asyncio
async def test_services_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    await ServicesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert cursor.execute.await_args.args[1][0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_services_passes_date_range_to_query() -> None:
    pool, cursor = make_mock_pool()
    end_excl = SAMPLE_END + timedelta(days=1)
    await ServicesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    p = cursor.execute.await_args.args[1]
    assert p[1] == SAMPLE_START
    assert p[2] == end_excl


@pytest.mark.asyncio
async def test_services_query_contains_org_filter() -> None:
    pool, cursor = make_mock_pool()
    await ServicesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert "OrganizationId" in cursor.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_services_output_has_required_keys() -> None:
    row = {
        "business_id": 1,
        "service_id": 2,
        "service_name": "X",
        "period_start": SAMPLE_START,
        "period_end": SAMPLE_END,
        "booking_count": 1,
        "revenue": 1.0,
        "min_price": 1.0,
        "max_price": 2.0,
        "unique_customers": 1,
    }
    pool, cursor = make_mock_pool(rows=[row])
    out = await ServicesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for k in row:
        assert k in out[0]


@pytest.mark.asyncio
async def test_services_output_has_service_name() -> None:
    pool, cursor = make_mock_pool(rows=[{"service_name": "Cut"}])
    out = await ServicesExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert "service_name" in out[0]


@pytest.mark.asyncio
async def test_services_null_service_name_defaulted() -> None:
    import etl.extractors.services as services_mod

    assert "Unknown Service" in services_mod._SQL
