"""Tests for transform_attendance."""
from __future__ import annotations

from datetime import date

from etl.transforms.attendance import transform_attendance, warehouse_keys_attendance


def _row(**kw):
    r = {
        "business_id": 1,
        "employee_id": 5,
        "employee_name": "Alex",
        "location_id": 0,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "record_date": date(2026, 1, 10),
        "time_sign_in": "09:00",
        "time_sign_out": "17:00",
    }
    r.update(kw)
    return r


def test_attendance_empty_input_returns_empty_list() -> None:
    assert transform_attendance([]) == []


def test_attendance_returns_correct_row_count() -> None:
    rows = [
        _row(record_date=date(2026, 1, 2)),
        _row(record_date=date(2026, 1, 3), employee_id=6),
    ]
    assert len(transform_attendance(rows)) == 2


def test_attendance_all_output_keys_present() -> None:
    out = transform_attendance([_row()])[0]
    for k in warehouse_keys_attendance():
        assert k in out


def test_attendance_money_rounded_to_2dp() -> None:
    out = transform_attendance([_row(time_sign_in="09:00", time_sign_out="17:30")])[0]
    assert out["total_hours_worked"] == round(out["total_hours_worked"], 2)


def test_attendance_handles_none_values_gracefully() -> None:
    out = transform_attendance([_row(time_sign_in=None, time_sign_out=None)])[0]
    assert out["total_hours_worked"] == 0.0
    assert out["days_worked"] == 1


def test_attendance_counts_are_non_negative() -> None:
    out = transform_attendance([_row()])[0]
    assert out["days_worked"] >= 0


def test_attendance_days_worked_counts_unique_dates() -> None:
    rows = [
        _row(record_date=date(2026, 1, 2)),
        _row(record_date=date(2026, 1, 2)),
        _row(record_date=date(2026, 1, 3)),
    ]
    out = transform_attendance(rows)[0]
    assert out["days_worked"] == 2


def test_attendance_hours_computed_correctly() -> None:
    out = transform_attendance([_row(time_sign_in="9:00", time_sign_out="17:00")])[0]
    assert out["total_hours_worked"] == 8.0


def test_attendance_skips_invalid_time_records() -> None:
    rows = [
        _row(time_sign_in="17:00", time_sign_out="09:00"),
        _row(record_date=date(2026, 1, 11), time_sign_in="09:00", time_sign_out="10:00"),
    ]
    out = transform_attendance(rows)[0]
    assert out["total_hours_worked"] == 1.0


def test_attendance_total_hours_summed_across_days() -> None:
    rows = [
        _row(record_date=date(2026, 1, 2), time_sign_in="09:00", time_sign_out="13:00"),
        _row(record_date=date(2026, 1, 3), time_sign_in="09:00", time_sign_out="13:00"),
    ]
    out = transform_attendance(rows)[0]
    assert out["total_hours_worked"] == 8.0


def test_attendance_avg_hours_per_day_computed() -> None:
    rows = [
        _row(record_date=date(2026, 1, 2), time_sign_in="09:00", time_sign_out="17:00"),
        _row(record_date=date(2026, 1, 3), time_sign_in="09:00", time_sign_out="17:00"),
    ]
    out = transform_attendance(rows)[0]
    assert out["avg_hours_per_day"] == 8.0


def test_attendance_invalid_time_string_skipped_gracefully() -> None:
    out = transform_attendance([_row(time_sign_in="abc", time_sign_out="def")])[0]
    assert out["total_hours_worked"] == 0.0


def test_attendance_groups_by_employee_and_period() -> None:
    rows = [
        _row(employee_id=1, period_start=date(2026, 1, 1)),
        _row(employee_id=2, period_start=date(2026, 1, 1)),
    ]
    assert len(transform_attendance(rows)) == 2
