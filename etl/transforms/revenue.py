"""Monthly revenue transform → wh_monthly_revenue shape."""
from __future__ import annotations

import logging
from typing import Any

from etl.transforms._common import icount, money, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "location_id",
    "period_start",
    "period_end",
    "total_revenue",
    "total_tips",
    "total_tax",
    "total_discounts",
    "total_gc_amount",
    "gross_revenue",
    "visit_count",
    "successful_visit_count",
    "refunded_visit_count",
    "cancelled_visit_count",
    "avg_visit_value",
    "cash_revenue",
    "card_revenue",
    "other_revenue",
)


def transform_revenue(rows: list[dict], **kwargs: Any) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        try:
            ps = to_date(row.get("period_start"))
            pe = to_date(row.get("period_end"))
            if ps is None or pe is None:
                _log.warning("transform_revenue: skipping row missing period dates %s", row)
                continue
            visit_count = icount(row.get("visit_count"))
            gross = money(row.get("gross_revenue"))
            avg_visit = round(safe_div(gross, float(visit_count)), 2) if visit_count > 0 else 0.0
            out.append(
                {
                    "business_id": icount(row.get("business_id")),
                    "location_id": icount(row.get("location_id")),
                    "period_start": ps,
                    "period_end": pe,
                    "total_revenue": money(row.get("total_revenue")),
                    "total_tips": money(row.get("total_tips")),
                    "total_tax": money(row.get("total_tax")),
                    "total_discounts": money(row.get("total_discounts")),
                    "total_gc_amount": money(row.get("total_gc_amount")),
                    "gross_revenue": gross,
                    "visit_count": visit_count,
                    "successful_visit_count": icount(row.get("successful_visit_count")),
                    "refunded_visit_count": icount(row.get("refunded_visit_count")),
                    "cancelled_visit_count": icount(row.get("cancelled_visit_count")),
                    "avg_visit_value": avg_visit,
                    "cash_revenue": money(row.get("cash_revenue")),
                    "card_revenue": money(row.get("card_revenue")),
                    "other_revenue": money(row.get("other_revenue")),
                }
            )
        except Exception as exc:
            _log.warning("transform_revenue: skipping bad row: %s", exc)
    return out


def warehouse_keys_revenue() -> tuple[str, ...]:
    return _WH_KEYS
