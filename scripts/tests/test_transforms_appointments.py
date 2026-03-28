"""Tests for transform_appointments."""
from __future__ import annotations

from datetime import date

from scripts.etl.transforms.appointments import (
    transform_appointments,
    warehouse_keys_appointments,
)


def _row(**kw):
    r = {
        "business_id": 1,
        "location_id": 0,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "total_booked": 100,
        "confirmed_count": 90,
        "completed_count": 80,
        "cancelled_count": 10,
        "no_show_count": 5,
        "walkin_count": 2,
        "app_booking_count": 10,
    }
    r.update(kw)
    return r


def test_appointments_empty_input_returns_empty_list() -> None:
    assert transform_appointments([]) == []


def test_appointments_returns_correct_row_count() -> None:
    assert len(transform_appointments([_row(), _row(location_id=1)])) == 2


def test_appointments_all_output_keys_present() -> None:
    out = transform_appointments([_row()])[0]
    for k in warehouse_keys_appointments():
        assert k in out


def test_appointments_money_rounded_to_2dp() -> None:
    # rates are percentages; no money column — assert rates rounded
    out = transform_appointments([_row()])[0]
    assert isinstance(out["cancellation_rate"], float)
    assert out["cancellation_rate"] == round(out["cancellation_rate"], 2)


def test_appointments_handles_none_values_gracefully() -> None:
    out = transform_appointments(
        [_row(total_booked=None, cancelled_count=None, completed_count=None)]
    )[0]
    assert out["total_booked"] == 0
    assert out["cancellation_rate"] == 0.0
    assert out["completion_rate"] == 0.0


def test_appointments_counts_are_non_negative() -> None:
    out = transform_appointments([_row(total_booked=-10, cancelled_count=-2)])[0]
    assert out["total_booked"] == 0
    assert out["cancelled_count"] == 0


def test_appointments_cancellation_rate_computed() -> None:
    out = transform_appointments([_row(total_booked=100, cancelled_count=10)])[0]
    assert out["cancellation_rate"] == 10.0


def test_appointments_cancellation_rate_zero_when_none() -> None:
    out = transform_appointments([_row(total_booked=0, cancelled_count=5)])[0]
    assert out["cancellation_rate"] == 0.0


def test_appointments_completion_rate_computed() -> None:
    out = transform_appointments([_row(total_booked=50, completed_count=25)])[0]
    assert out["completion_rate"] == 50.0


def test_appointments_rates_clamped_to_100() -> None:
    out = transform_appointments([_row(total_booked=10, cancelled_count=50, completed_count=50)])[0]
    assert out["cancellation_rate"] == 100.0
    assert out["completion_rate"] == 100.0
