"""Tests for ClientsExtractor."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from etl.extractors.clients import ClientsExtractor
from scripts.tests.extractor_test_utils import SAMPLE_ORG_ID, make_mock_pool


@pytest.mark.asyncio
async def test_clients_returns_list() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    out = await ClientsExtractor(pool).extract(SAMPLE_ORG_ID)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_clients_empty_db_returns_empty_list() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    out = await ClientsExtractor(pool).extract(SAMPLE_ORG_ID)
    assert out == []


@pytest.mark.asyncio
async def test_clients_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    await ClientsExtractor(pool).extract(SAMPLE_ORG_ID)
    assert cursor.execute.await_args_list[0].args[1] == (SAMPLE_ORG_ID,)
    assert cursor.execute.await_args_list[1].args[1] == (SAMPLE_ORG_ID,)


@pytest.mark.asyncio
async def test_clients_query_contains_org_filter() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    await ClientsExtractor(pool).extract(SAMPLE_ORG_ID)
    sql0 = cursor.execute.await_args_list[0].args[0]
    sql1 = cursor.execute.await_args_list[1].args[0]
    assert "OrganizationId" in sql0
    assert "OrgID" in sql1


@pytest.mark.asyncio
async def test_clients_runs_two_queries() -> None:
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[[], []])
    await ClientsExtractor(pool).extract(SAMPLE_ORG_ID)
    assert cursor.execute.await_count == 2


@pytest.mark.asyncio
async def test_clients_merges_loyalty_points() -> None:
    visits = [
        {
            "business_id": 1,
            "customer_id": 7,
            "first_visit_date": None,
            "last_visit_date": None,
            "total_visits": 2,
            "total_spend": 20.0,
        }
    ]
    loyalty = [{"customer_id": 7, "loyalty_points": 100}]
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[visits, loyalty])
    out = await ClientsExtractor(pool).extract(SAMPLE_ORG_ID)
    assert out[0]["loyalty_points"] == 100


@pytest.mark.asyncio
async def test_clients_missing_loyalty_gives_zero() -> None:
    visits = [
        {
            "business_id": 1,
            "customer_id": 7,
            "first_visit_date": None,
            "last_visit_date": None,
            "total_visits": 1,
            "total_spend": 10.0,
        }
    ]
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[visits, []])
    out = await ClientsExtractor(pool).extract(SAMPLE_ORG_ID)
    assert out[0]["loyalty_points"] == 0


@pytest.mark.asyncio
async def test_clients_output_has_required_keys() -> None:
    visits = [
        {
            "business_id": 1,
            "customer_id": 1,
            "first_visit_date": None,
            "last_visit_date": None,
            "total_visits": 0,
            "total_spend": 0.0,
        }
    ]
    pool, cursor = make_mock_pool()
    cursor.fetchall = AsyncMock(side_effect=[visits, []])
    out = await ClientsExtractor(pool).extract(SAMPLE_ORG_ID)
    for k in ("business_id", "customer_id", "loyalty_points"):
        assert k in out[0]
