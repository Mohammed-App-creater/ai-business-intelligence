"""Unit tests for etl.transforms.appointments_field_derivations."""

from __future__ import annotations

from etl.transforms.appointments_field_derivations import (
    derive_completion_rate,
    derive_period_end,
    derive_peak_slot,
)


def test_derive_period_end_feb_non_leap() -> None:
    rows = [{"period": "2026-02"}]
    out = derive_period_end(rows)
    assert out[0]["period_end"] == "2026-02-28"


def test_derive_period_end_feb_leap() -> None:
    rows = [{"period": "2024-02"}]
    out = derive_period_end(rows)
    assert out[0]["period_end"] == "2024-02-29"


def test_derive_period_end_december() -> None:
    rows = [{"period": "2025-12"}]
    out = derive_period_end(rows)
    assert out[0]["period_end"] == "2025-12-31"


def test_derive_period_end_preserves_existing() -> None:
    rows = [{"period": "2026-01", "period_end": "2099-01-01"}]
    out = derive_period_end(rows)
    assert out[0]["period_end"] == "2099-01-01"


def test_derive_peak_slot_max_morning() -> None:
    rows = [
        {"morning_count": 10, "afternoon_count": 5, "evening_count": 3},
    ]
    out = derive_peak_slot(rows)
    assert out[0]["peak_slot"] == "morning"


def test_derive_peak_slot_all_zero_none() -> None:
    rows = [{"morning_count": 0, "afternoon_count": 0, "evening_count": 0}]
    out = derive_peak_slot(rows)
    assert out[0]["peak_slot"] is None


def test_derive_peak_slot_tie_prefers_morning_first() -> None:
    """max(..., key=counts.get) breaks ties by dict key order: morning, afternoon, evening."""
    rows = [{"morning_count": 5, "afternoon_count": 5, "evening_count": 0}]
    out = derive_peak_slot(rows)
    assert out[0]["peak_slot"] == "morning"


def test_derive_peak_slot_preserves_existing() -> None:
    rows = [{"morning_count": 1, "peak_slot": "evening"}]
    out = derive_peak_slot(rows)
    assert out[0]["peak_slot"] == "evening"


def test_derive_completion_rate_five_over_twentyfour() -> None:
    rows = [{"total_booked": 24, "completed_count": 5}]
    out = derive_completion_rate(rows)
    assert out[0]["completion_rate_pct"] == 20.83


def test_derive_completion_rate_zero_total() -> None:
    rows = [{"total_booked": 0, "completed_count": 0}]
    out = derive_completion_rate(rows)
    assert out[0]["completion_rate_pct"] == 0.0


def test_derive_completion_rate_preserves_existing() -> None:
    rows = [{"total_booked": 10, "completed_count": 5, "completion_rate_pct": 99.99}]
    out = derive_completion_rate(rows)
    assert out[0]["completion_rate_pct"] == 99.99
