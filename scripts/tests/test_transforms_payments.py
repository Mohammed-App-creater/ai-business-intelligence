"""Tests for transform_payments."""
from __future__ import annotations

from datetime import date

from scripts.etl.transforms.payments import transform_payments, warehouse_keys_payments


def _row(ptype: str, amount: float, count: int, **kw):
    r = {
        "business_id": 1,
        "location_id": 0,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "payment_type": ptype,
        "amount": amount,
        "count": count,
    }
    r.update(kw)
    return r


def test_payments_empty_input_returns_empty_list() -> None:
    assert transform_payments([]) == []


def test_payments_returns_correct_row_count() -> None:
    rows = [_row("Cash", 10, 1), _row("Credit", 20, 2)]
    assert len(transform_payments(rows)) == 1


def test_payments_all_output_keys_present() -> None:
    out = transform_payments([_row("Cash", 1, 1)])[0]
    for k in warehouse_keys_payments():
        assert k in out


def test_payments_money_rounded_to_2dp() -> None:
    out = transform_payments([_row("Cash", 10.126, 1)])[0]
    assert out["cash_amount"] == 10.13


def test_payments_handles_none_values_gracefully() -> None:
    out = transform_payments([_row("Cash", None, None)])[0]
    assert out["cash_amount"] == 0.0
    assert out["cash_count"] == 0


def test_payments_counts_are_non_negative() -> None:
    out = transform_payments([_row("Cash", 10, -3)])[0]
    assert out["cash_count"] == 0


def test_payments_cash_classified_correctly() -> None:
    out = transform_payments([_row("Cash", 25.0, 2)])[0]
    assert out["cash_amount"] == 25.0
    assert out["cash_count"] == 2


def test_payments_credit_classified_as_card() -> None:
    out = transform_payments([_row("Credit", 40.0, 1)])[0]
    assert out["card_amount"] == 40.0
    assert out["card_count"] == 1


def test_payments_check_classified_as_card() -> None:
    out = transform_payments([_row("Check", 15.0, 3)])[0]
    assert out["card_amount"] == 15.0
    assert out["card_count"] == 3


def test_payments_giftcard_classified_correctly() -> None:
    out = transform_payments([_row("GiftCard", 5.0, 1)])[0]
    assert out["gift_card_amount"] == 5.0
    assert out["gift_card_count"] == 1


def test_payments_unknown_type_goes_to_other() -> None:
    out = transform_payments([_row("PayPal", 9.0, 4)])[0]
    assert out["other_amount"] == 9.0
    assert out["other_count"] == 4


def test_payments_totals_sum_correctly() -> None:
    rows = [
        _row("Cash", 10.0, 1),
        _row("Credit", 20.0, 2),
        _row("GiftCard", 5.0, 1),
        _row("Other", 3.0, 1),
    ]
    out = transform_payments(rows)[0]
    assert out["total_amount"] == 38.0
    assert out["total_count"] == 5


def test_payments_groups_by_period() -> None:
    rows = [
        _row("Cash", 1, 1, period_start=date(2026, 1, 1)),
        _row("Cash", 2, 1, period_start=date(2026, 2, 1)),
    ]
    out = transform_payments(rows)
    assert len(out) == 2
