"""Subscription revenue transform → wh_subscription_revenue shape."""
from __future__ import annotations

import logging
from typing import Any

from etl.transforms._common import as_bool, icount, money, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "location_id",
    "period_start",
    "period_end",
    "active_subscriptions",
    "new_subscriptions",
    "cancelled_subscriptions",
    "gross_subscription_revenue",
    "net_subscription_revenue",
    "avg_subscription_value",
)


def _row_key(row: dict) -> tuple[int, int, Any] | None:
    ps = to_date(row.get("period_start"))
    pe = to_date(row.get("period_end"))
    if ps is None or pe is None:
        return None
    return (icount(row.get("business_id")), icount(row.get("location_id")), ps)


def transform_subscriptions(
    active_rows: list[dict],
    all_rows: list[dict],
    **kwargs: Any,
) -> list[dict]:
    groups: dict[tuple[int, int, Any], dict[str, Any]] = {}

    def _ensure(key: tuple[int, int, Any], pe: Any) -> dict[str, Any]:
        if key not in groups:
            groups[key] = {
                "business_id": key[0],
                "location_id": key[1],
                "period_start": key[2],
                "period_end": pe,
                "gross": 0.0,
                "net": 0.0,
                "active_n": 0,
                "new_n": 0,
                "cancel_n": 0,
            }
        return groups[key]

    for row in active_rows:
        try:
            key = _row_key(row)
            if key is None:
                continue
            pe = to_date(row.get("period_end"))
            if pe is None:
                continue
            g = _ensure(key, pe)
            g["period_end"] = pe
            if as_bool(row.get("is_active")):
                g["active_n"] += 1
                amt = money(row.get("amount"))
                disc = money(row.get("discount"))
                g["gross"] += amt
                g["net"] += money(amt - disc)
        except Exception as exc:
            _log.warning("transform_subscriptions active_rows: skip: %s", exc)

    for row in all_rows:
        try:
            key = _row_key(row)
            if key is None:
                continue
            pe = to_date(row.get("period_end"))
            if pe is None:
                continue
            g = _ensure(key, pe)
            g["period_end"] = pe
            ps = key[2]
            sub_create = to_date(row.get("sub_create_date"))
            if sub_create is None:
                continue
            if ps <= sub_create <= pe:
                g["new_n"] += 1
            if not as_bool(row.get("is_active")) and sub_create < ps:
                g["cancel_n"] += 1
        except Exception as exc:
            _log.warning("transform_subscriptions all_rows: skip: %s", exc)

    out: list[dict] = []
    for g in groups.values():
        act = g["active_n"]
        gross = money(g["gross"])
        net = money(g["net"])
        avg = round(safe_div(gross, float(act)), 2) if act > 0 else 0.0
        out.append(
            {
                "business_id": g["business_id"],
                "location_id": g["location_id"],
                "period_start": g["period_start"],
                "period_end": g["period_end"],
                "active_subscriptions": act,
                "new_subscriptions": g["new_n"],
                "cancelled_subscriptions": g["cancel_n"],
                "gross_subscription_revenue": gross,
                "net_subscription_revenue": net,
                "avg_subscription_value": avg,
            }
        )
    return out


def warehouse_keys_subscriptions() -> tuple[str, ...]:
    return _WH_KEYS
