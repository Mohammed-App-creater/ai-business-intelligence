"""Service performance transform → wh_service_performance shape."""
from __future__ import annotations

import logging
from typing import Any

from scripts.etl.transforms._common import icount, money, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "service_id",
    "service_name",
    "period_start",
    "period_end",
    "booking_count",
    "revenue",
    "avg_price",
    "min_price",
    "max_price",
    "unique_customers",
)


def transform_services(rows: list[dict], **kwargs: Any) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        try:
            ps = to_date(row.get("period_start"))
            pe = to_date(row.get("period_end"))
            if ps is None or pe is None:
                _log.warning("transform_services: skipping row missing period %s", row)
                continue
            bookings = icount(row.get("booking_count"))
            rev = money(row.get("revenue"))
            avg_price = round(safe_div(rev, float(bookings)), 2) if bookings > 0 else 0.0
            svc = (row.get("service_name") or "").strip() or "Unknown Service"
            out.append(
                {
                    "business_id": icount(row.get("business_id")),
                    "service_id": icount(row.get("service_id")),
                    "service_name": svc,
                    "period_start": ps,
                    "period_end": pe,
                    "booking_count": bookings,
                    "revenue": rev,
                    "avg_price": avg_price,
                    "min_price": money(row.get("min_price")),
                    "max_price": money(row.get("max_price")),
                    "unique_customers": icount(row.get("unique_customers")),
                }
            )
        except Exception as exc:
            _log.warning("transform_services: skipping bad row: %s", exc)
    return out


def warehouse_keys_services() -> tuple[str, ...]:
    return _WH_KEYS
