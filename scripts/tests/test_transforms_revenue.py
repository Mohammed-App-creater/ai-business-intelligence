"""Tests for transform_revenue."""
from __future__ import annotations

from datetime import date, datetime

from scripts.etl.transforms.revenue import transform_revenue, warehouse_keys_revenue


def test_revenue_empty_input_returns_empty_list() -> None:
    assert transform_revenue([]) == []


def test_revenue_returns_correct_row_count() -> None:
    rows = [
        {
            "business_id": 1,
            "location_id": 0,
            "period_start": date(2026, 1, 1),
            "period_end": date(2026, 1, 31),
            "total_revenue": 100.0,
            "total_tips": 10.0,
            "total_tax": 5.0,
            "total_discounts": 2.0,
            "total_gc_amount": 1.0,
            "gross_revenue": 200.0,
            "visit_count": 4,
            "successful_visit_count": 3,
            "refunded_visit_count": 0,
            "cancelled_visit_count": 1,
            "cash_revenue": 50.0,
            "card_revenue": 100.0,
            "other_revenue": 50.0,
        }
    ]
    assert len(transform_revenue(rows)) == 1


def test_revenue_all_output_keys_present() -> None:
    rows = [
        {
            "business_id": 1,
            "location_id": 2,
            "period_start": date(2026, 2, 1),
            "period_end": date(2026, 2, 28),
            "total_revenue": 1.0,
            "total_tips": 1.0,
            "total_tax": 1.0,
            "total_discounts": 1.0,
            "total_gc_amount": 1.0,
            "gross_revenue": 10.0,
            "visit_count": 2,
            "successful_visit_count": 2,
            "refunded_visit_count": 0,
            "cancelled_visit_count": 0,
            "cash_revenue": 1.0,
            "card_revenue": 1.0,
            "other_revenue": 1.0,
        }
    ]
    out = transform_revenue(rows)[0]
    for k in warehouse_keys_revenue():
        assert k in out


def test_revenue_money_rounded_to_2dp() -> None:
    rows = [
        {
            "business_id": 1,
            "location_id": 0,
            "period_start": date(2026, 1, 1),
            "period_end": date(2026, 1, 31),
            "total_revenue": 10.126,
            "total_tips": 0,
            "total_tax": 0,
            "total_discounts": 0,
            "total_gc_amount": 0,
            "gross_revenue": 10.126,
            "visit_count": 3,
            "successful_visit_count": 3,
            "refunded_visit_count": 0,
            "cancelled_visit_count": 0,
            "cash_revenue": 0,
            "card_revenue": 0,
            "other_revenue": 0,
        }
    ]
    o = transform_revenue(rows)[0]
    assert o["total_revenue"] == 10.13
    assert o["gross_revenue"] == 10.13


def test_revenue_handles_none_values_gracefully() -> None:
    rows = [
        {
            "business_id": 1,
            "location_id": 0,
            "period_start": date(2026, 1, 1),
            "period_end": date(2026, 1, 31),
            "total_revenue": None,
            "total_tips": None,
            "total_tax": None,
            "total_discounts": None,
            "total_gc_amount": None,
            "gross_revenue": None,
            "visit_count": None,
            "successful_visit_count": None,
            "refunded_visit_count": None,
            "cancelled_visit_count": None,
            "cash_revenue": None,
            "card_revenue": None,
            "other_revenue": None,
        }
    ]
    o = transform_revenue(rows)[0]
    assert o["total_revenue"] == 0.0
    assert o["visit_count"] == 0
    assert o["avg_visit_value"] == 0.0


def test_revenue_counts_are_non_negative() -> None:
    rows = [
        {
            "business_id": 1,
            "location_id": 0,
            "period_start": date(2026, 1, 1),
            "period_end": date(2026, 1, 31),
            "total_revenue": 0,
            "total_tips": 0,
            "total_tax": 0,
            "total_discounts": 0,
            "total_gc_amount": 0,
            "gross_revenue": 0,
            "visit_count": -5,
            "successful_visit_count": -1,
            "refunded_visit_count": -2,
            "cancelled_visit_count": -3,
            "cash_revenue": 0,
            "card_revenue": 0,
            "other_revenue": 0,
        }
    ]
    o = transform_revenue(rows)[0]
    assert o["visit_count"] == 0
    assert o["successful_visit_count"] == 0
    assert o["refunded_visit_count"] == 0
    assert o["cancelled_visit_count"] == 0


def test_revenue_avg_visit_value_computed_correctly() -> None:
    rows = [
        {
            "business_id": 1,
            "location_id": 0,
            "period_start": date(2026, 1, 1),
            "period_end": date(2026, 1, 31),
            "total_revenue": 0,
            "total_tips": 0,
            "total_tax": 0,
            "total_discounts": 0,
            "total_gc_amount": 0,
            "gross_revenue": 100.0,
            "visit_count": 4,
            "successful_visit_count": 4,
            "refunded_visit_count": 0,
            "cancelled_visit_count": 0,
            "cash_revenue": 0,
            "card_revenue": 0,
            "other_revenue": 0,
        }
    ]
    assert transform_revenue(rows)[0]["avg_visit_value"] == 25.0


def test_revenue_avg_visit_value_zero_when_no_visits() -> None:
    rows = [
        {
            "business_id": 1,
            "location_id": 0,
            "period_start": date(2026, 1, 1),
            "period_end": date(2026, 1, 31),
            "total_revenue": 0,
            "total_tips": 0,
            "total_tax": 0,
            "total_discounts": 0,
            "total_gc_amount": 0,
            "gross_revenue": 999.0,
            "visit_count": 0,
            "successful_visit_count": 0,
            "refunded_visit_count": 0,
            "cancelled_visit_count": 0,
            "cash_revenue": 0,
            "card_revenue": 0,
            "other_revenue": 0,
        }
    ]
    assert transform_revenue(rows)[0]["avg_visit_value"] == 0.0


def test_revenue_datetime_period_normalized_to_date() -> None:
    rows = [
        {
            "business_id": 1,
            "location_id": 0,
            "period_start": datetime(2026, 3, 1, 15, 30),
            "period_end": datetime(2026, 3, 31, 23, 59),
            "total_revenue": 0,
            "total_tips": 0,
            "total_tax": 0,
            "total_discounts": 0,
            "total_gc_amount": 0,
            "gross_revenue": 0,
            "visit_count": 1,
            "successful_visit_count": 1,
            "refunded_visit_count": 0,
            "cancelled_visit_count": 0,
            "cash_revenue": 0,
            "card_revenue": 0,
            "other_revenue": 0,
        }
    ]
    o = transform_revenue(rows)[0]
    assert o["period_start"] == date(2026, 3, 1)
    assert o["period_end"] == date(2026, 3, 31)
