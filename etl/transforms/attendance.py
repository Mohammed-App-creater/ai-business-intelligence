"""Attendance summary transform → wh_attendance_summary shape."""
from __future__ import annotations

import logging
from typing import Any

from scripts.etl.base import parse_time_str
from scripts.etl.transforms._common import icount, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "employee_id",
    "employee_name",
    "location_id",
    "period_start",
    "period_end",
    "days_worked",
    "total_hours_worked",
    "avg_hours_per_day",
)


def transform_attendance(rows: list[dict], **kwargs: Any) -> list[dict]:
    groups: dict[tuple[int, int, int, Any], dict[str, Any]] = {}
    for row in rows:
        try:
            ps = to_date(row.get("period_start"))
            pe = to_date(row.get("period_end"))
            rd = to_date(row.get("record_date"))
            if ps is None or pe is None or rd is None:
                _log.warning("transform_attendance: skip missing date %s", row)
                continue
            bid = icount(row.get("business_id"))
            eid = icount(row.get("employee_id"))
            lid = icount(row.get("location_id"))
            key = (bid, eid, lid, ps)
            if key not in groups:
                groups[key] = {
                    "business_id": bid,
                    "employee_id": eid,
                    "location_id": lid,
                    "period_start": ps,
                    "period_end": pe,
                    "dates": set(),
                    "hours_sum": 0.0,
                    "name": (row.get("employee_name") or "").strip() or "Unknown",
                }
            g = groups[key]
            g["period_end"] = pe
            nm = (row.get("employee_name") or "").strip()
            if nm:
                g["name"] = nm
            g["dates"].add(rd)
            sign_in = parse_time_str(row.get("time_sign_in"))
            sign_out = parse_time_str(row.get("time_sign_out"))
            if sign_in is not None and sign_out is not None and sign_out > sign_in:
                g["hours_sum"] += sign_out - sign_in
            else:
                _log.debug(
                    "transform_attendance: skip invalid times bid=%s eid=%s day=%s",
                    bid,
                    eid,
                    rd,
                )
        except Exception as exc:
            _log.warning("transform_attendance: skip bad row: %s", exc)

    out: list[dict] = []
    for g in groups.values():
        days = len(g["dates"])
        total_h = round(g["hours_sum"], 2)
        avg_d = round(safe_div(total_h, float(days)), 2) if days > 0 else 0.0
        out.append(
            {
                "business_id": g["business_id"],
                "employee_id": g["employee_id"],
                "employee_name": g["name"],
                "location_id": g["location_id"],
                "period_start": g["period_start"],
                "period_end": g["period_end"],
                "days_worked": days,
                "total_hours_worked": total_h,
                "avg_hours_per_day": avg_d,
            }
        )
    return out


def warehouse_keys_attendance() -> tuple[str, ...]:
    return _WH_KEYS
