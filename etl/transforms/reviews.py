"""Review summary transform → wh_review_summary shape (merge three sources)."""
from __future__ import annotations

import logging
from typing import Any

from etl.transforms._common import icount, rating_or_none, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "period_start",
    "period_end",
    "emp_review_count",
    "emp_avg_rating",
    "visit_review_count",
    "visit_avg_rating",
    "google_review_count",
    "google_avg_rating",
    "google_bad_review_count",
    "total_review_count",
    "overall_avg_rating",
)


def transform_reviews(
    emp_rows: list[dict],
    visit_rows: list[dict],
    google_rows: list[dict],
    **kwargs: Any,
) -> list[dict]:
    merged: dict[tuple[int, Any], dict[str, Any]] = {}

    def _ensure(bid: int, ps: Any, pe: Any) -> dict[str, Any]:
        key = (bid, ps)
        if key not in merged:
            merged[key] = {
                "business_id": bid,
                "period_start": ps,
                "period_end": pe,
                "emp_review_count": 0,
                "emp_avg_rating": None,
                "visit_review_count": 0,
                "visit_avg_rating": None,
                "google_review_count": 0,
                "google_avg_rating": None,
                "google_bad_review_count": 0,
            }
        return merged[key]

    for row in emp_rows:
        try:
            ps = to_date(row.get("period_start"))
            pe = to_date(row.get("period_end"))
            bid = icount(row.get("business_id"))
            if ps is None or pe is None:
                continue
            cell = _ensure(bid, ps, pe)
            cell["period_end"] = pe
            cell["emp_review_count"] = icount(row.get("emp_review_count"))
            cell["emp_avg_rating"] = rating_or_none(row.get("emp_avg_rating"))
        except Exception as exc:
            _log.warning("transform_reviews emp_rows: skip: %s", exc)

    for row in visit_rows:
        try:
            ps = to_date(row.get("period_start"))
            pe = to_date(row.get("period_end"))
            bid = icount(row.get("business_id"))
            if ps is None or pe is None:
                continue
            cell = _ensure(bid, ps, pe)
            cell["period_end"] = pe
            cell["visit_review_count"] = icount(row.get("visit_review_count"))
            cell["visit_avg_rating"] = rating_or_none(row.get("visit_avg_rating"))
        except Exception as exc:
            _log.warning("transform_reviews visit_rows: skip: %s", exc)

    for row in google_rows:
        try:
            ps = to_date(row.get("period_start"))
            pe = to_date(row.get("period_end"))
            bid = icount(row.get("business_id"))
            if ps is None or pe is None:
                continue
            cell = _ensure(bid, ps, pe)
            cell["period_end"] = pe
            cell["google_review_count"] = icount(row.get("google_review_count"))
            cell["google_avg_rating"] = rating_or_none(row.get("google_avg_rating"))
            cell["google_bad_review_count"] = icount(row.get("google_bad_review_count"))
        except Exception as exc:
            _log.warning("transform_reviews google_rows: skip: %s", exc)

    out: list[dict] = []
    for cell in merged.values():
        try:
            ec = cell["emp_review_count"]
            vc = cell["visit_review_count"]
            gc = cell["google_review_count"]
            total_c = ec + vc + gc
            weighted = 0.0
            wden = 0
            ea = cell["emp_avg_rating"]
            va = cell["visit_avg_rating"]
            ga = cell["google_avg_rating"]
            if ea is not None:
                weighted += ea * ec
                wden += ec
            if va is not None:
                weighted += va * vc
                wden += vc
            if ga is not None:
                weighted += ga * gc
                wden += gc
            overall = round(safe_div(weighted, float(wden)), 2) if wden > 0 else None
            out.append(
                {
                    "business_id": cell["business_id"],
                    "period_start": cell["period_start"],
                    "period_end": cell["period_end"],
                    "emp_review_count": ec,
                    "emp_avg_rating": ea,
                    "visit_review_count": vc,
                    "visit_avg_rating": va,
                    "google_review_count": gc,
                    "google_avg_rating": ga,
                    "google_bad_review_count": cell["google_bad_review_count"],
                    "total_review_count": total_c,
                    "overall_avg_rating": overall,
                }
            )
        except Exception as exc:
            _log.warning("transform_reviews: output row skip: %s", exc)
    return out


def warehouse_keys_reviews() -> tuple[str, ...]:
    return _WH_KEYS
