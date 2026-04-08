"""Tests for transform_campaigns."""
from __future__ import annotations

from datetime import date

from etl.transforms.campaigns import transform_campaigns, warehouse_keys_campaigns


def _row(**kw):
    r = {
        "business_id": 1,
        "campaign_id": 7,
        "campaign_name": "Spring",
        "execution_date": date(2026, 3, 1),
        "is_recurring": 0,
        "total_sent": 100,
        "successful_sent": 95,
        "failed_count": 5,
        "opened_count": 50,
        "clicked_count": 10,
    }
    r.update(kw)
    return r


def test_campaigns_empty_input_returns_empty_list() -> None:
    assert transform_campaigns([]) == []


def test_campaigns_returns_correct_row_count() -> None:
    assert len(transform_campaigns([_row(), _row(campaign_id=8)])) == 2


def test_campaigns_all_output_keys_present() -> None:
    out = transform_campaigns([_row()])[0]
    for k in warehouse_keys_campaigns():
        assert k in out


def test_campaigns_money_rounded_to_2dp() -> None:
    out = transform_campaigns([_row(opened_count=1, total_sent=3)])[0]
    assert out["open_rate"] == round(100.0 / 3.0, 2)


def test_campaigns_handles_none_values_gracefully() -> None:
    out = transform_campaigns([_row(total_sent=None, opened_count=None)])[0]
    assert out["total_sent"] == 0
    assert out["open_rate"] == 0.0


def test_campaigns_counts_are_non_negative() -> None:
    out = transform_campaigns([_row(total_sent=-5, opened_count=-1)])[0]
    assert out["total_sent"] == 0
    assert out["opened_count"] == 0


def test_campaigns_open_rate_computed() -> None:
    out = transform_campaigns([_row(total_sent=100, opened_count=50)])[0]
    assert out["open_rate"] == 50.0


def test_campaigns_rates_zero_when_nothing_sent() -> None:
    out = transform_campaigns([_row(total_sent=0, opened_count=10, clicked_count=5, failed_count=2)])[0]
    assert out["open_rate"] == 0.0
    assert out["click_rate"] == 0.0
    assert out["fail_rate"] == 0.0


def test_campaigns_rates_clamped_to_100() -> None:
    out = transform_campaigns([_row(total_sent=10, opened_count=50, clicked_count=50, failed_count=50)])[0]
    assert out["open_rate"] == 100.0
    assert out["click_rate"] == 100.0
    assert out["fail_rate"] == 100.0


def test_campaigns_is_recurring_bool_normalized() -> None:
    assert transform_campaigns([_row(is_recurring=1)])[0]["is_recurring"] is True
    assert transform_campaigns([_row(is_recurring=0)])[0]["is_recurring"] is False


def test_campaigns_blank_name_defaults_to_unnamed() -> None:
    out = transform_campaigns([_row(campaign_name="")])[0]
    assert out["campaign_name"] == "Unnamed Campaign"
