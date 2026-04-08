"""Tests for transform_expenses."""
from __future__ import annotations

from datetime import date

from etl.transforms.expenses import transform_expenses, warehouse_keys_expenses


def _row(**kw):
    r = {
        "business_id": 1,
        "location_id": 0,
        "category_id": 3,
        "category_name": "Rent",
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "total_amount": 500.0,
        "expense_count": 4,
    }
    r.update(kw)
    return r


def test_expenses_empty_input_returns_empty_list() -> None:
    assert transform_expenses([]) == []


def test_expenses_returns_correct_row_count() -> None:
    assert len(transform_expenses([_row(), _row(category_id=4)])) == 2


def test_expenses_all_output_keys_present() -> None:
    out = transform_expenses([_row()])[0]
    for k in warehouse_keys_expenses():
        assert k in out


def test_expenses_money_rounded_to_2dp() -> None:
    out = transform_expenses([_row(total_amount=12.345)])[0]
    assert out["total_amount"] == 12.35
    assert out["avg_expense"] == 3.09


def test_expenses_handles_none_values_gracefully() -> None:
    out = transform_expenses([_row(total_amount=None, expense_count=None)])[0]
    assert out["total_amount"] == 0.0
    assert out["expense_count"] == 0
    assert out["avg_expense"] == 0.0


def test_expenses_counts_are_non_negative() -> None:
    out = transform_expenses([_row(expense_count=-2)])[0]
    assert out["expense_count"] == 0


def test_expenses_avg_expense_computed() -> None:
    out = transform_expenses([_row(total_amount=100.0, expense_count=4)])[0]
    assert out["avg_expense"] == 25.0


def test_expenses_avg_expense_zero_when_no_expenses() -> None:
    out = transform_expenses([_row(total_amount=50.0, expense_count=0)])[0]
    assert out["avg_expense"] == 0.0


def test_expenses_blank_category_defaults_to_uncategorised() -> None:
    out = transform_expenses([_row(category_name="  ")])[0]
    assert out["category_name"] == "Uncategorised"


def test_expenses_skips_missing_period() -> None:
    assert transform_expenses([_row(period_start=None)]) == []
