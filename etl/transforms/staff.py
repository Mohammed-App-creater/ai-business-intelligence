"""Staff performance transform → wh_staff_performance shape."""
from __future__ import annotations

import logging
from typing import Any

from etl.transforms._common import clamp_rate, icount, money, rating_or_none, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "employee_id",
    "employee_name",
    "period_start",
    "period_end",
    "total_visits",
    "total_revenue",
    "total_tips",
    "total_commission",
    "appointments_booked",
    "appointments_completed",
    "appointments_cancelled",
    "avg_rating",
    "review_count",
    "utilisation_rate",
)


def transform_staff(rows: list[dict], **kwargs: Any) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        try:
            ps = to_date(row.get("period_start"))
            pe = to_date(row.get("period_end"))
            if ps is None or pe is None:
                _log.warning("transform_staff: skipping row missing period %s", row)
                continue
            booked = icount(row.get("appointments_booked"))
            completed = icount(row.get("appointments_completed"))
            raw_util = safe_div(float(completed), float(booked)) * 100.0 if booked > 0 else 0.0
            util = clamp_rate(raw_util)
            name = (row.get("employee_name") or "").strip() or "Unknown"
            avg_r = rating_or_none(row.get("avg_rating"))
            out.append(
                {
                    "business_id": icount(row.get("business_id")),
                    "employee_id": icount(row.get("employee_id")),
                    "employee_name": name,
                    "period_start": ps,
                    "period_end": pe,
                    "total_visits": icount(row.get("total_visits")),
                    "total_revenue": money(row.get("total_revenue")),
                    "total_tips": money(row.get("total_tips")),
                    "total_commission": money(row.get("total_commission")),
                    "appointments_booked": booked,
                    "appointments_completed": completed,
                    "appointments_cancelled": icount(row.get("appointments_cancelled")),
                    "avg_rating": avg_r,
                    "review_count": icount(row.get("review_count")),
                    "utilisation_rate": util,
                }
            )
        except Exception as exc:
            _log.warning("transform_staff: skipping bad row: %s", exc)
    return out


def warehouse_keys_staff() -> tuple[str, ...]:
    return _WH_KEYS
