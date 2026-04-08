"""Expense summary transform → wh_expense_summary shape."""
from __future__ import annotations

import logging
from typing import Any

from etl.transforms._common import icount, money, safe_div, to_date

_log = logging.getLogger(__name__)

_WH_KEYS = (
    "business_id",
    "location_id",
    "category_id",
    "category_name",
    "period_start",
    "period_end",
    "total_amount",
    "expense_count",
    "avg_expense",
)


def transform_expenses(rows: list[dict], **kwargs: Any) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        try:
            ps = to_date(row.get("period_start"))
            pe = to_date(row.get("period_end"))
            if ps is None or pe is None:
                _log.warning("transform_expenses: skipping row missing period %s", row)
                continue
            count = icount(row.get("expense_count"))
            total = money(row.get("total_amount"))
            avg = round(safe_div(total, float(count)), 2) if count > 0 else 0.0
            cat = (row.get("category_name") or "").strip() or "Uncategorised"
            out.append(
                {
                    "business_id": icount(row.get("business_id")),
                    "location_id": icount(row.get("location_id")),
                    "category_id": icount(row.get("category_id")),
                    "category_name": cat,
                    "period_start": ps,
                    "period_end": pe,
                    "total_amount": total,
                    "expense_count": count,
                    "avg_expense": avg,
                }
            )
        except Exception as exc:
            _log.warning("transform_expenses: skipping bad row: %s", exc)
    return out


def warehouse_keys_expenses() -> tuple[str, ...]:
    return _WH_KEYS
