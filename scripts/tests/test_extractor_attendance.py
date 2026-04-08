"""Tests for AttendanceExtractor."""
from __future__ import annotations

from datetime import timedelta

import pytest

from etl.extractors.attendance import AttendanceExtractor
from scripts.tests.extractor_test_utils import SAMPLE_END, SAMPLE_ORG_ID, SAMPLE_START, make_mock_pool


@pytest.mark.asyncio
async def test_attendance_returns_list() -> None:
    pool, cursor = make_mock_pool()
    out = await AttendanceExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_attendance_empty_db_returns_empty_list() -> None:
    pool, cursor = make_mock_pool()
    out = await AttendanceExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert out == []


@pytest.mark.asyncio
async def test_attendance_passes_org_id_to_query() -> None:
    pool, cursor = make_mock_pool()
    await AttendanceExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    assert cursor.execute.await_args.args[1][0] == SAMPLE_ORG_ID


@pytest.mark.asyncio
async def test_attendance_passes_date_range_to_query() -> None:
    pool, cursor = make_mock_pool()
    end_excl = SAMPLE_END + timedelta(days=1)
    await AttendanceExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    p = cursor.execute.await_args.args[1]
    assert p[1] == SAMPLE_START
    assert p[2] == end_excl


@pytest.mark.asyncio
async def test_attendance_returns_raw_records() -> None:
    import etl.extractors.attendance as mod

    assert "GROUP BY" not in mod._SQL.upper()


@pytest.mark.asyncio
async def test_attendance_joins_tbl_emp() -> None:
    import etl.extractors.attendance as mod

    assert "JOIN tbl_emp" in mod._SQL


@pytest.mark.asyncio
async def test_attendance_includes_time_columns() -> None:
    import etl.extractors.attendance as mod

    assert "time_sign_in" in mod._SQL
    assert "time_sign_out" in mod._SQL


@pytest.mark.asyncio
async def test_attendance_output_has_required_keys() -> None:
    row = {
        "business_id": 1,
        "employee_id": 2,
        "employee_name": "A",
        "location_id": 0,
        "period_start": SAMPLE_START,
        "period_end": SAMPLE_END,
        "record_date": SAMPLE_START,
        "time_sign_in": "09:00",
        "time_sign_out": "17:00",
    }
    pool, cursor = make_mock_pool(rows=[row])
    out = await AttendanceExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    for k in ("time_sign_in", "time_sign_out", "record_date"):
        assert k in out[0]


@pytest.mark.asyncio
async def test_attendance_query_filters_by_org_via_emp() -> None:
    pool, cursor = make_mock_pool()
    await AttendanceExtractor(pool).extract(SAMPLE_ORG_ID, SAMPLE_START, SAMPLE_END)
    sql = cursor.execute.await_args.args[0]
    assert "OrganizationId" in sql
