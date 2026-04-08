"""Tests for transform_services."""
from __future__ import annotations

from datetime import date

from etl.transforms.services import transform_services, warehouse_keys_services


def _row(**kw):
    r = {
        "business_id": 1,
        "service_id": 99,
        "service_name": "Cut",
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "booking_count": 8,
        "revenue": 400.0,
        "min_price": 40.0,
        "max_price": 60.0,
        "unique_customers": 5,
    }
    r.update(kw)
    return r


def test_services_empty_input_returns_empty_list() -> None:
    assert transform_services([]) == []


def test_services_returns_correct_row_count() -> None:
    assert len(transform_services([_row(), _row(service_id=100)])) == 2


def test_services_all_output_keys_present() -> None:
    out = transform_services([_row()])[0]
    for k in warehouse_keys_services():
        assert k in out


def test_services_money_rounded_to_2dp() -> None:
    out = transform_services([_row(revenue=10.556, min_price=1.111)])[0]
    assert out["revenue"] == 10.56
    assert out["min_price"] == 1.11


def test_services_handles_none_values_gracefully() -> None:
    out = transform_services([_row(revenue=None, booking_count=None, min_price=None)])[0]
    assert out["revenue"] == 0.0
    assert out["booking_count"] == 0
    assert out["avg_price"] == 0.0


def test_services_counts_are_non_negative() -> None:
    out = transform_services([_row(booking_count=-2, unique_customers=-1)])[0]
    assert out["booking_count"] == 0
    assert out["unique_customers"] == 0


def test_services_avg_price_computed_correctly() -> None:
    out = transform_services([_row(revenue=100.0, booking_count=4)])[0]
    assert out["avg_price"] == 25.0


def test_services_avg_price_zero_when_no_bookings() -> None:
    out = transform_services([_row(revenue=50.0, booking_count=0)])[0]
    assert out["avg_price"] == 0.0


def test_services_blank_name_defaults_to_unknown_service() -> None:
    out = transform_services([_row(service_name="")])[0]
    assert out["service_name"] == "Unknown Service"


def test_services_skips_missing_period() -> None:
    assert transform_services([_row(period_start=None)]) == []
