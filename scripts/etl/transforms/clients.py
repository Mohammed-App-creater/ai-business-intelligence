"""Client metrics transform → wh_client_metrics shape."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from scripts.etl.transforms._common import icount, money, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "customer_id",
    "first_visit_date",
    "last_visit_date",
    "total_visits",
    "total_spend",
    "avg_spend_per_visit",
    "loyalty_points",
    "days_since_last_visit",
    "visit_frequency_days",
    "is_churned",
)


def transform_clients(rows: list[dict], **kwargs: Any) -> list[dict]:
    out: list[dict] = []
    today = date.today()
    for row in rows:
        try:
            first = to_date(row.get("first_visit_date"))
            last = to_date(row.get("last_visit_date"))
            visits = icount(row.get("total_visits"))
            spend = money(row.get("total_spend"))
            avg_spend = round(safe_div(spend, float(visits)), 2) if visits > 0 else 0.0
            if last is None:
                days_since: int | None = None
                is_churned = False
            else:
                days_since = max(0, (today - last).days)
                is_churned = days_since > 90
            if visits < 2 or first is None:
                freq: float | None = None
            elif last is None:
                freq = None
            else:
                span = (last - first).days
                freq = round(safe_div(float(span), float(visits - 1)), 2)
            out.append(
                {
                    "business_id": icount(row.get("business_id")),
                    "customer_id": icount(row.get("customer_id")),
                    "first_visit_date": first,
                    "last_visit_date": last,
                    "total_visits": visits,
                    "total_spend": spend,
                    "avg_spend_per_visit": avg_spend,
                    "loyalty_points": icount(row.get("loyalty_points")),
                    "days_since_last_visit": days_since,
                    "visit_frequency_days": freq,
                    "is_churned": is_churned,
                }
            )
        except Exception as exc:
            _log.warning("transform_clients: skipping bad row: %s", exc)
    return out


def warehouse_keys_clients() -> tuple[str, ...]:
    return _WH_KEYS
