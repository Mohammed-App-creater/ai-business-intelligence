"""Tests for CampaignsExtractor."""
from __future__ import annotations

from datetime import timedelta

import pytest

from etl.extractors.campaigns import CampaignsExtractor
from scripts.tests.extractor_test_utils import SAMPLE_END, SAMPLE_ORG_ID, SAMPLE_START, make_mock_pool


@pytest.mark.asyncio
async def test_campaigns_returns_list() -> None:
    pool, cursor = make_mock_pool()
    out = await CampaignsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_campaigns_empty_db_returns_empty_list() -> None:
    pool, cursor = make_mock_pool()
    out = await CampaignsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out == []


@pytest.mark.asyncio
async def test_campaigns_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    await CampaignsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert cursor.execute.await_args.args[1][0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_campaigns_passes_date_range_to_query() -> None:
    pool, cursor = make_mock_pool()
    end_excl = SAMPLE_END + timedelta(days=1)
    await CampaignsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    p = cursor.execute.await_args.args[1]
    assert p[1] == SAMPLE_START
    assert p[2] == end_excl


@pytest.mark.asyncio
async def test_campaigns_output_has_required_keys() -> None:
    row = {
        "business_id": 1,
        "campaign_id": 1,
        "campaign_name": "N",
        "execution_date": SAMPLE_START,
        "is_recurring": 0,
        "total_sent": 1,
        "successful_sent": 1,
        "failed_count": 0,
        "opened_count": 0,
        "clicked_count": 0,
    }
    pool, cursor = make_mock_pool(rows=[row])
    out = await CampaignsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for k in row:
        assert k in out[0]


@pytest.mark.asyncio
async def test_campaigns_uses_tenant_id() -> None:
    import etl.extractors.campaigns as mod

    assert "TenantID" in mod._SQL
    assert "OrganizationId" not in mod._SQL


@pytest.mark.asyncio
async def test_campaigns_uses_successed_column() -> None:
    import etl.extractors.campaigns as mod

    assert "Successed" in mod._SQL


@pytest.mark.asyncio
async def test_campaigns_excludes_deleted_status() -> None:
    import etl.extractors.campaigns as mod

    assert "Status != 'Delete'" in mod._SQL


@pytest.mark.asyncio
async def test_campaigns_query_contains_where_tenant() -> None:
    pool, cursor = make_mock_pool()
    await CampaignsExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    sql = cursor.execute.await_args.args[0]
    assert "WHERE" in sql.upper()
    assert "TenantID" in sql
