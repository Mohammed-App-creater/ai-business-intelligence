"""Tests for transform_staff."""
from __future__ import annotations

from datetime import date

from etl.transforms.staff import transform_staff, warehouse_keys_staff


def _row(**kw):
    r = {
        "business_id": 1,
        "employee_id": 10,
        "employee_name": "Jane Doe",
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "total_visits": 5,
        "total_revenue": 100.0,
        "total_tips": 20.0,
        "total_commission": 15.0,
        "appointments_booked": 10,
        "appointments_completed": 8,
        "appointments_cancelled": 1,
        "avg_rating": 4.5,
        "review_count": 3,
    }
    r.update(kw)
    return r


def test_staff_empty_input_returns_empty_list() -> None:
    assert transform_staff([]) == []


def test_staff_returns_correct_row_count() -> None:
    assert len(transform_staff([_row(), _row(employee_id=11)])) == 2


def test_staff_all_output_keys_present() -> None:
    out = transform_staff([_row()])[0]
    for k in warehouse_keys_staff():
        assert k in out


def test_staff_money_rounded_to_2dp() -> None:
    out = transform_staff([_row(total_revenue=10.999, total_tips=1.001)])[0]
    assert out["total_revenue"] == 11.0
    assert out["total_tips"] == 1.0


def test_staff_handles_none_values_gracefully() -> None:
    out = transform_staff(
        [_row(total_revenue=None, avg_rating=None, appointments_booked=None)]
    )[0]
    assert out["total_revenue"] == 0.0
    assert out["avg_rating"] is None
    assert out["appointments_booked"] == 0
    assert out["utilisation_rate"] == 0.0


def test_staff_counts_are_non_negative() -> None:
    out = transform_staff([_row(appointments_booked=-5, appointments_completed=-1)])[0]
    assert out["appointments_booked"] == 0
    assert out["appointments_completed"] == 0


def test_staff_utilisation_rate_computed_correctly() -> None:
    out = transform_staff([_row(appointments_booked=10, appointments_completed=5)])[0]
    assert out["utilisation_rate"] == 50.0


def test_staff_utilisation_zero_when_no_bookings() -> None:
    out = transform_staff([_row(appointments_booked=0, appointments_completed=3)])[0]
    assert out["utilisation_rate"] == 0.0


def test_staff_utilisation_clamped_to_100() -> None:
    out = transform_staff([_row(appointments_booked=4, appointments_completed=10)])[0]
    assert out["utilisation_rate"] == 100.0


def test_staff_avg_rating_none_preserved() -> None:
    out = transform_staff([_row(avg_rating=None)])[0]
    assert out["avg_rating"] is None


def test_staff_avg_rating_rounded() -> None:
    out = transform_staff([_row(avg_rating=4.666)])[0]
    assert out["avg_rating"] == 4.67


def test_staff_empty_name_defaults_to_unknown() -> None:
    out = transform_staff([_row(employee_name="   ")])[0]
    assert out["employee_name"] == "Unknown"
