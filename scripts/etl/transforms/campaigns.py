"""Campaign performance transform → wh_campaign_performance shape."""
from __future__ import annotations

import logging
from typing import Any

from scripts.etl.transforms._common import as_bool, clamp_rate, icount, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "campaign_id",
    "campaign_name",
    "execution_date",
    "is_recurring",
    "total_sent",
    "successful_sent",
    "failed_count",
    "opened_count",
    "clicked_count",
    "open_rate",
    "click_rate",
    "fail_rate",
)


def transform_campaigns(rows: list[dict], **kwargs: Any) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        try:
            ed = to_date(row.get("execution_date"))
            if ed is None:
                _log.warning("transform_campaigns: skip missing execution_date %s", row)
                continue
            total = icount(row.get("total_sent"))
            opened = icount(row.get("opened_count"))
            clicked = icount(row.get("clicked_count"))
            failed = icount(row.get("failed_count"))
            open_r = clamp_rate(safe_div(float(opened), float(total)) * 100.0)
            click_r = clamp_rate(safe_div(float(clicked), float(total)) * 100.0)
            fail_r = clamp_rate(safe_div(float(failed), float(total)) * 100.0)
            name = (row.get("campaign_name") or "").strip() or "Unnamed Campaign"
            out.append(
                {
                    "business_id": icount(row.get("business_id")),
                    "campaign_id": icount(row.get("campaign_id")),
                    "campaign_name": name,
                    "execution_date": ed,
                    "is_recurring": as_bool(row.get("is_recurring")),
                    "total_sent": total,
                    "successful_sent": icount(row.get("successful_sent")),
                    "failed_count": failed,
                    "opened_count": opened,
                    "clicked_count": clicked,
                    "open_rate": open_r,
                    "click_rate": click_r,
                    "fail_rate": fail_r,
                }
            )
        except Exception as exc:
            _log.warning("transform_campaigns: skip bad row: %s", exc)
    return out


def warehouse_keys_campaigns() -> tuple[str, ...]:
    return _WH_KEYS
