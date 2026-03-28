"""Tests for transform_reviews."""
from __future__ import annotations

from datetime import date

from scripts.etl.transforms.reviews import transform_reviews, warehouse_keys_reviews


def _emp(**kw):
    r = {
        "business_id": 1,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "emp_review_count": 0,
        "emp_avg_rating": None,
    }
    r.update(kw)
    return r


def _visit(**kw):
    r = {
        "business_id": 1,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "visit_review_count": 0,
        "visit_avg_rating": None,
    }
    r.update(kw)
    return r


def _google(**kw):
    r = {
        "business_id": 1,
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 1, 31),
        "google_review_count": 0,
        "google_avg_rating": None,
        "google_bad_review_count": 0,
    }
    r.update(kw)
    return r


def test_reviews_empty_input_returns_empty_list() -> None:
    assert transform_reviews([], [], []) == []


def test_reviews_returns_correct_row_count() -> None:
    out = transform_reviews([_emp()], [], [])
    assert len(out) == 1


def test_reviews_all_output_keys_present() -> None:
    out = transform_reviews([_emp(emp_review_count=1, emp_avg_rating=5.0)], [], [])[0]
    for k in warehouse_keys_reviews():
        assert k in out


def test_reviews_money_rounded_to_2dp() -> None:
    # ratings use same rounding as money scale
    out = transform_reviews([_emp(emp_review_count=1, emp_avg_rating=4.666)], [], [])[0]
    assert out["emp_avg_rating"] == 4.67


def test_reviews_handles_none_values_gracefully() -> None:
    out = transform_reviews([_emp(emp_avg_rating=None)], [], [])[0]
    assert out["emp_avg_rating"] is None
    assert out["overall_avg_rating"] is None


def test_reviews_counts_are_non_negative() -> None:
    out = transform_reviews(
        [_emp(emp_review_count=-2, emp_avg_rating=3.0)], [], []
    )[0]
    assert out["emp_review_count"] == 0


def test_reviews_merges_three_sources_into_one_row() -> None:
    out = transform_reviews(
        [_emp(emp_review_count=1, emp_avg_rating=5.0)],
        [_visit(visit_review_count=2, visit_avg_rating=4.0)],
        [_google(google_review_count=1, google_avg_rating=3.0, google_bad_review_count=0)],
    )
    assert len(out) == 1
    r = out[0]
    assert r["emp_review_count"] == 1
    assert r["visit_review_count"] == 2
    assert r["google_review_count"] == 1


def test_reviews_total_count_is_sum_of_all_sources() -> None:
    out = transform_reviews(
        [_emp(emp_review_count=2, emp_avg_rating=5.0)],
        [_visit(visit_review_count=3, visit_avg_rating=4.0)],
        [_google(google_review_count=5, google_avg_rating=3.0, google_bad_review_count=1)],
    )[0]
    assert out["total_review_count"] == 10


def test_reviews_overall_avg_is_weighted_correctly() -> None:
    out = transform_reviews(
        [_emp(emp_review_count=10, emp_avg_rating=4.0)],
        [_visit(visit_review_count=10, visit_avg_rating=5.0)],
        [],
    )[0]
    assert out["overall_avg_rating"] == 4.5


def test_reviews_handles_missing_source() -> None:
    out = transform_reviews([_emp(emp_review_count=3, emp_avg_rating=4.0)], [], [])
    assert len(out) == 1
    assert out[0]["visit_review_count"] == 0


def test_reviews_overall_avg_none_when_no_reviews() -> None:
    out = transform_reviews(
        [_emp(emp_review_count=0, emp_avg_rating=None)],
        [_visit(visit_review_count=0, visit_avg_rating=None)],
        [_google(google_review_count=0, google_avg_rating=None, google_bad_review_count=0)],
    )[0]
    assert out["overall_avg_rating"] is None


def test_reviews_multiple_orgs_produce_separate_rows() -> None:
    out = transform_reviews(
        [
            _emp(business_id=1, emp_review_count=1, emp_avg_rating=5.0),
            _emp(business_id=2, emp_review_count=1, emp_avg_rating=4.0),
        ],
        [],
        [],
    )
    assert len(out) == 2
    bids = {r["business_id"] for r in out}
    assert bids == {1, 2}
