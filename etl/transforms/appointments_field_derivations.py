"""
Field-level workarounds for fields the analytics backend doesn't return
but the v1.1 spec lists as required. All functions are SELF-HEALING:
if the API response already includes the field, the function leaves it
untouched; only when missing does the function compute it.

Once backend ships the missing fields, these functions become no-ops.
No removal needed.

Tracked: docs/integration/APPOINTMENTS_KNOWN_ISSUES.md §3.11.
"""

from __future__ import annotations

import calendar
from typing import Iterable


def derive_period_end(rows: Iterable[dict]) -> list[dict]:
    """Add period_end (YYYY-MM-DD) computed from period (YYYY-MM) — last day of month."""
    result = []
    for r in rows:
        if "period_end" not in r and r.get("period"):
            year, month = map(int, str(r["period"]).split("-"))
            last_day = calendar.monthrange(year, month)[1]
            r["period_end"] = f"{year:04d}-{month:02d}-{last_day:02d}"
        result.append(r)
    return result


def derive_peak_slot(rows: Iterable[dict]) -> list[dict]:
    """Add peak_slot ('morning'|'afternoon'|'evening'|None) from time-bucket counts.

    On ties (equal counts), ``max(..., key=counts.get)`` picks the first slot among
    ties in dict iteration order: morning, then afternoon, then evening.
    """
    result = []
    for r in rows:
        if "peak_slot" not in r:
            counts = {
                "morning": int(r.get("morning_count") or 0),
                "afternoon": int(r.get("afternoon_count") or 0),
                "evening": int(r.get("evening_count") or 0),
            }
            r["peak_slot"] = (
                max(counts, key=counts.get) if max(counts.values()) > 0 else None
            )
        result.append(r)
    return result


def derive_completion_rate(rows: Iterable[dict]) -> list[dict]:
    """Add completion_rate_pct = completed/total*100 (0.0 when total is 0)."""
    result = []
    for r in rows:
        if "completion_rate_pct" not in r:
            total = int(r.get("total_booked") or 0)
            completed = int(r.get("completed_count") or 0)
            r["completion_rate_pct"] = round(completed / total * 100, 2) if total else 0.0
        result.append(r)
    return result
