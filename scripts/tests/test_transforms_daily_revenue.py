"""Tests for transform_daily_revenue."""
from __future__ import annotations

from datetime import date, datetime

from etl.transforms.daily_revenue import (
    transform_daily_revenue,
    warehouse_keys_daily_revenue,
)


def _base_row(**overrides):
    r = {
        "business_id": 1,
        "location_id": 0,
        "revenue_date": date(2026, 1, 15),
        "total_revenue": 100.0,
        "total_tips": 5.0,
        "total_tax": 3.0,
        "total_discounts": 2.0,
        "gross_revenue": 120.0,
        "visit_count": 4,
        "successful_visit_count": 4,
    }
    r.update(overrides)
    return r


def test_daily_revenue_empty_input_returns_empty_list() -> None:
    assert transform_daily_revenue([]) == []


def test_daily_revenue_returns_correct_row_count() -> None:
    assert len(transform_daily_revenue([_base_row(), _base_row(business_id=2)])) == 2


def test_daily_revenue_all_output_keys_present() -> None:
    out = transform_daily_revenue([_base_row()])[0]
    for k in warehouse_keys_daily_revenue():
        assert k in out


def test_daily_revenue_money_rounded_to_2dp() -> None:
    out = transform_daily_revenue([_base_row(total_revenue=1.111, gross_revenue=2.222)])[0]
    assert out["total_revenue"] == 1.11
    assert out["gross_revenue"] == 2.22


def test_daily_revenue_handles_none_values_gracefully() -> None:
    out = transform_daily_revenue(
        [_base_row(total_revenue=None, gross_revenue=None, visit_count=None)]
    )[0]
    assert out["total_revenue"] == 0.0
    assert out["gross_revenue"] == 0.0
    assert out["visit_count"] == 0
    assert out["avg_visit_value"] == 0.0


def test_daily_revenue_counts_are_non_negative() -> None:
    out = transform_daily_revenue([_base_row(visit_count=-3, successful_visit_count=-1)])[0]
    assert out["visit_count"] == 0
    assert out["successful_visit_count"] == 0


def test_daily_revenue_avg_visit_value_correct() -> None:
    out = transform_daily_revenue([_base_row(gross_revenue=50.0, visit_count=5)])[0]
    assert out["avg_visit_value"] == 10.0


def test_daily_revenue_date_normalization() -> None:
    out = transform_daily_revenue(
        [_base_row(revenue_date=datetime(2026, 6, 10, 8, 0))]
    )[0]
    assert out["revenue_date"] == date(2026, 6, 10)


def test_daily_revenue_skips_row_without_revenue_date() -> None:
    assert transform_daily_revenue([_base_row(revenue_date=None)]) == []


def test_daily_revenue_avg_zero_when_no_visits() -> None:
    out = transform_daily_revenue([_base_row(gross_revenue=99.0, visit_count=0)])[0]
    assert out["avg_visit_value"] == 0.0
