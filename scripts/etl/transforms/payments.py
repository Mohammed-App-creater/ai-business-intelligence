"""Payment breakdown transform → wh_payment_breakdown shape."""
from __future__ import annotations

import logging
from typing import Any

from scripts.etl.transforms._common import icount, money, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "location_id",
    "period_start",
    "period_end",
    "cash_amount",
    "cash_count",
    "card_amount",
    "card_count",
    "gift_card_amount",
    "gift_card_count",
    "other_amount",
    "other_count",
    "total_amount",
    "total_count",
)


def _bucket(payment_type: str | None) -> str:
    pt = (payment_type or "").strip()
    if pt == "Cash":
        return "cash"
    if pt in ("Credit", "Check"):
        return "card"
    if pt == "GiftCard":
        return "gift_card"
    return "other"


def transform_payments(rows: list[dict], **kwargs: Any) -> list[dict]:
    groups: dict[tuple[int, int, Any], dict[str, Any]] = {}
    for row in rows:
        try:
            ps = to_date(row.get("period_start"))
            pe = to_date(row.get("period_end"))
            if ps is None or pe is None:
                _log.warning("transform_payments: skip missing period %s", row)
                continue
            bid = icount(row.get("business_id"))
            lid = icount(row.get("location_id"))
            key = (bid, lid, ps)
            if key not in groups:
                groups[key] = {
                    "business_id": bid,
                    "location_id": lid,
                    "period_start": ps,
                    "period_end": pe,
                    "cash_amount": 0.0,
                    "cash_count": 0,
                    "card_amount": 0.0,
                    "card_count": 0,
                    "gift_card_amount": 0.0,
                    "gift_card_count": 0,
                    "other_amount": 0.0,
                    "other_count": 0,
                }
            g = groups[key]
            g["period_end"] = pe
            b = _bucket(row.get("payment_type"))
            amt = money(row.get("amount"))
            cnt = icount(row.get("count"))
            g[f"{b}_amount"] = money(g[f"{b}_amount"] + amt)
            g[f"{b}_count"] = g[f"{b}_count"] + cnt
        except Exception as exc:
            _log.warning("transform_payments: skip bad row: %s", exc)

    out: list[dict] = []
    for g in groups.values():
        cash_a = money(g["cash_amount"])
        card_a = money(g["card_amount"])
        gc_a = money(g["gift_card_amount"])
        oth_a = money(g["other_amount"])
        tot_a = money(cash_a + card_a + gc_a + oth_a)
        tot_c = g["cash_count"] + g["card_count"] + g["gift_card_count"] + g["other_count"]
        out.append(
            {
                "business_id": g["business_id"],
                "location_id": g["location_id"],
                "period_start": g["period_start"],
                "period_end": g["period_end"],
                "cash_amount": cash_a,
                "cash_count": g["cash_count"],
                "card_amount": card_a,
                "card_count": g["card_count"],
                "gift_card_amount": gc_a,
                "gift_card_count": g["gift_card_count"],
                "other_amount": oth_a,
                "other_count": g["other_count"],
                "total_amount": tot_a,
                "total_count": tot_c,
            }
        )
    return out


def warehouse_keys_payments() -> tuple[str, ...]:
    return _WH_KEYS
