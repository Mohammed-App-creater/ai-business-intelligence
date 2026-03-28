"""Tests for transform_subscriptions."""
from __future__ import annotations

from datetime import date

from scripts.etl.transforms.subscriptions import (
    transform_subscriptions,
    warehouse_keys_subscriptions,
)


def _sub(**kw):
    r = {
        "business_id": 1,
        "location_id": 0,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "customer_id": 1,
        "amount": 50.0,
        "discount": 5.0,
        "is_active": 1,
        "sub_create_date": date(2026, 1, 15),
    }
    r.update(kw)
    return r


def test_subscriptions_empty_lists_returns_empty() -> None:
    assert transform_subscriptions([], []) == []


def test_subscriptions_returns_correct_row_count() -> None:
    active = [_sub(location_id=0), _sub(location_id=1, customer_id=2)]
    assert len(transform_subscriptions(active, [])) == 2


def test_subscriptions_all_output_keys_present() -> None:
    out = transform_subscriptions([_sub()], [_sub()])[0]
    for k in warehouse_keys_subscriptions():
        assert k in out


def test_subscriptions_money_rounded_to_2dp() -> None:
    out = transform_subscriptions([_sub(amount=10.126, discount=1.111)], [_sub()])[0]
    assert out["gross_subscription_revenue"] == 10.13
    assert out["net_subscription_revenue"] == 9.02


def test_subscriptions_handles_none_values_gracefully() -> None:
    out = transform_subscriptions(
        [_sub(amount=None, discount=None, is_active=1)],
        [_sub(sub_create_date=None)],
    )[0]
    assert out["gross_subscription_revenue"] == 0.0


def test_subscriptions_counts_are_non_negative() -> None:
    out = transform_subscriptions([_sub()], [_sub(is_active=-1)])[0]
    assert out["active_subscriptions"] >= 0


def test_subscriptions_active_count_correct() -> None:
    active = [_sub(is_active=1), _sub(customer_id=2, is_active=1)]
    out = transform_subscriptions(active, [])[0]
    assert out["active_subscriptions"] == 2


def test_subscriptions_new_count_correct() -> None:
    all_rows = [_sub(sub_create_date=date(2026, 1, 10), is_active=1)]
    out = transform_subscriptions([], all_rows)[0]
    assert out["new_subscriptions"] == 1


def test_subscriptions_cancelled_count_correct() -> None:
    all_rows = [_sub(is_active=0, sub_create_date=date(2025, 6, 1))]
    out = transform_subscriptions([], all_rows)[0]
    assert out["cancelled_subscriptions"] == 1


def test_subscriptions_gross_revenue_sum_correct() -> None:
    active = [_sub(amount=30.0, customer_id=1), _sub(amount=20.0, customer_id=2)]
    out = transform_subscriptions(active, active)[0]
    assert out["gross_subscription_revenue"] == 50.0


def test_subscriptions_net_revenue_correct() -> None:
    active = [_sub(amount=100.0, discount=25.0)]
    out = transform_subscriptions(active, active)[0]
    assert out["net_subscription_revenue"] == 75.0


def test_subscriptions_avg_value_zero_when_none_active() -> None:
    out = transform_subscriptions([], [_sub(is_active=0, sub_create_date=date(2025, 1, 1))])[0]
    assert out["active_subscriptions"] == 0
    assert out["avg_subscription_value"] == 0.0


def test_subscriptions_groups_by_location() -> None:
    a1 = _sub(location_id=1, customer_id=1)
    a2 = _sub(location_id=2, customer_id=2)
    out = transform_subscriptions([a1, a2], [a1, a2])
    assert len(out) == 2
