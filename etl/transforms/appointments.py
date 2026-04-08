"""Appointment metrics transform → wh_appointment_metrics shape."""
from __future__ import annotations

import logging
from typing import Any

from etl.transforms._common import clamp_rate, icount, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "location_id",
    "period_start",
    "period_end",
    "total_booked",
    "confirmed_count",
    "completed_count",
    "cancelled_count",
    "no_show_count",
    "walkin_count",
    "app_booking_count",
    "cancellation_rate",
    "completion_rate",
)


def transform_appointments(rows: list[dict], **kwargs: Any) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        try:
            ps = to_date(row.get("period_start"))
            pe = to_date(row.get("period_end"))
            if ps is None or pe is None:
                _log.warning("transform_appointments: skipping row missing period %s", row)
                continue
            total = icount(row.get("total_booked"))
            cancelled = icount(row.get("cancelled_count"))
            completed = icount(row.get("completed_count"))
            cancel_rate = clamp_rate(safe_div(float(cancelled), float(total)) * 100.0)
            complete_rate = clamp_rate(safe_div(float(completed), float(total)) * 100.0)
            out.append(
                {
                    "business_id": icount(row.get("business_id")),
                    "location_id": icount(row.get("location_id")),
                    "period_start": ps,
                    "period_end": pe,
                    "total_booked": total,
                    "confirmed_count": icount(row.get("confirmed_count")),
                    "completed_count": completed,
                    "cancelled_count": cancelled,
                    "no_show_count": icount(row.get("no_show_count")),
                    "walkin_count": icount(row.get("walkin_count")),
                    "app_booking_count": icount(row.get("app_booking_count")),
                    "cancellation_rate": cancel_rate,
                    "completion_rate": complete_rate,
                }
            )
        except Exception as exc:
            _log.warning("transform_appointments: skipping bad row: %s", exc)
    return out


def warehouse_keys_appointments() -> tuple[str, ...]:
    return _WH_KEYS
