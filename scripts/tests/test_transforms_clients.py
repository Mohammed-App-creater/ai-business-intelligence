"""Tests for transform_clients."""
from __future__ import annotations

from datetime import date, timedelta

from scripts.etl.transforms.clients import transform_clients, warehouse_keys_clients


def _row(**kw):
    r = {
        "business_id": 1,
        "customer_id": 50,
        "first_visit_date": date(2025, 1, 1),
        "last_visit_date": date(2026, 1, 10),
        "total_visits": 5,
        "total_spend": 250.0,
        "loyalty_points": 100,
    }
    r.update(kw)
    return r


def test_clients_empty_input_returns_empty_list() -> None:
    assert transform_clients([]) == []


def test_clients_returns_correct_row_count() -> None:
    assert len(transform_clients([_row(), _row(customer_id=51)])) == 2


def test_clients_all_output_keys_present() -> None:
    out = transform_clients([_row()])[0]
    for k in warehouse_keys_clients():
        assert k in out


def test_clients_money_rounded_to_2dp() -> None:
    out = transform_clients([_row(total_spend=10.999)])[0]
    assert out["total_spend"] == 11.0


def test_clients_handles_none_values_gracefully() -> None:
    out = transform_clients(
        [_row(last_visit_date=None, first_visit_date=None, total_spend=None, total_visits=None)]
    )[0]
    assert out["total_spend"] == 0.0
    assert out["total_visits"] == 0
    assert out["days_since_last_visit"] is None
    assert out["is_churned"] is False


def test_clients_counts_are_non_negative() -> None:
    out = transform_clients([_row(total_visits=-3, loyalty_points=-5)])[0]
    assert out["total_visits"] == 0
    assert out["loyalty_points"] == 0


def test_clients_days_since_last_visit_computed() -> None:
    last = date.today() - timedelta(days=5)
    out = transform_clients([_row(last_visit_date=last)])[0]
    assert out["days_since_last_visit"] == 5


def test_clients_days_since_none_when_no_visits() -> None:
    out = transform_clients([_row(last_visit_date=None)])[0]
    assert out["days_since_last_visit"] is None


def test_clients_churned_when_over_90_days() -> None:
    last = date.today() - timedelta(days=91)
    out = transform_clients([_row(last_visit_date=last)])[0]
    assert out["is_churned"] is True


def test_clients_not_churned_when_under_90_days() -> None:
    last = date.today() - timedelta(days=89)
    out = transform_clients([_row(last_visit_date=last)])[0]
    assert out["is_churned"] is False


def test_clients_visit_frequency_none_for_single_visit() -> None:
    out = transform_clients([_row(total_visits=1, first_visit_date=date(2025, 1, 1))])[0]
    assert out["visit_frequency_days"] is None


def test_clients_visit_frequency_computed_correctly() -> None:
    out = transform_clients(
        [
            _row(
                first_visit_date=date(2025, 1, 1),
                last_visit_date=date(2025, 1, 31),
                total_visits=3,
            )
        ]
    )[0]
    assert out["visit_frequency_days"] == 15.0


def test_clients_avg_spend_per_visit_computed() -> None:
    out = transform_clients([_row(total_spend=200.0, total_visits=8)])[0]
    assert out["avg_spend_per_visit"] == 25.0
