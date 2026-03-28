"""Tests for SubscriptionsExtractor."""
from __future__ import annotations

from datetime import timedelta

import pytest

from scripts.etl.extractors.subscriptions import SubscriptionsExtractor
from scripts.tests.extractor_test_utils import SAMPLE_END, SAMPLE_ORG_ID, SAMPLE_START, make_mock_pool


@pytest.mark.asyncio
async def test_subscriptions_returns_list() -> None:
    pool, cursor = make_mock_pool()
    out = await SubscriptionsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_subscriptions_empty_db_returns_empty_list() -> None:
    pool, cursor = make_mock_pool()
    out = await SubscriptionsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out == []


@pytest.mark.asyncio
async def test_subscriptions_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    await SubscriptionsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    params = cursor.execute.await_args.args[1]
    assert params[2] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_subscriptions_passes_date_range_to_query() -> None:
    pool, cursor = make_mock_pool()
    end_excl = SAMPLE_END + timedelta(days=1)
    await SubscriptionsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    params = cursor.execute.await_args.args[1]
    assert SAMPLE_START in params
    assert end_excl in params


@pytest.mark.asyncio
async def test_subscriptions_uses_org_id_col() -> None:
    import scripts.etl.extractors.subscriptions as mod

    assert "OrgId" in mod._SQL
    assert "OrganizationId" not in mod._SQL


@pytest.mark.asyncio
async def test_subscriptions_includes_both_active_and_new() -> None:
    import scripts.etl.extractors.subscriptions as mod

    assert "Active = 1" in mod._SQL
    assert "SubCreateDate" in mod._SQL


@pytest.mark.asyncio
async def test_subscriptions_output_has_required_keys() -> None:
    row = {
        "business_id": 1,
        "location_id": 0,
        "period_start": SAMPLE_START,
        "period_end": SAMPLE_END,
        "customer_id": 1,
        "amount": 10.0,
        "discount": 0.0,
        "is_active": 1,
        "sub_create_date": SAMPLE_START,
    }
    pool, cursor = make_mock_pool(rows=[row])
    out = await SubscriptionsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for k in row:
        assert k in out[0]


@pytest.mark.asyncio
async def test_subscriptions_query_contains_where_org() -> None:
    pool, cursor = make_mock_pool()
    await SubscriptionsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    sql = cursor.execute.await_args.args[0]
    assert "WHERE" in sql.upper()
    assert "OrgId" in sql
