"""Daily revenue transform → wh_daily_revenue shape."""
from __future__ import annotations

import logging
from typing import Any

from scripts.etl.transforms._common import icount, money, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "location_id",
    "revenue_date",
    "total_revenue",
    "total_tips",
    "total_tax",
    "total_discounts",
    "gross_revenue",
    "visit_count",
    "successful_visit_count",
    "avg_visit_value",
)


def transform_daily_revenue(rows: list[dict], **kwargs: Any) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        try:
            rd = to_date(row.get("revenue_date"))
            if rd is None:
                _log.warning("transform_daily_revenue: skipping row missing revenue_date %s", row)
                continue
            visit_count = icount(row.get("visit_count"))
            gross = money(row.get("gross_revenue"))
            avg_visit = round(safe_div(gross, float(visit_count)), 2) if visit_count > 0 else 0.0
            out.append(
                {
                    "business_id": icount(row.get("business_id")),
                    "location_id": icount(row.get("location_id")),
                    "revenue_date": rd,
                    "total_revenue": money(row.get("total_revenue")),
                    "total_tips": money(row.get("total_tips")),
                    "total_tax": money(row.get("total_tax")),
                    "total_discounts": money(row.get("total_discounts")),
                    "gross_revenue": gross,
                    "visit_count": visit_count,
                    "successful_visit_count": icount(row.get("successful_visit_count")),
                    "avg_visit_value": avg_visit,
                }
            )
        except Exception as exc:
            _log.warning("transform_daily_revenue: skipping bad row: %s", exc)
    return out


def warehouse_keys_daily_revenue() -> tuple[str, ...]:
    return _WH_KEYS
