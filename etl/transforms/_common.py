"""Shared helpers for ETL transforms (stdlib only)."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

_log = logging.getLogger(__name__)


def to_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        try:
            return date.fromisoformat(val[:10])
        except ValueError:
            _log.warning("to_date: unparseable date string %r", val)
            return None
    _log.warning("to_date: unsupported type %r", type(val).__name__)
    return None


def money(val: Any) -> float:
    return round(float(val or 0), 2)


def icount(val: Any) -> int:
    try:
        return max(0, int(val or 0))
    except (TypeError, ValueError):
        _log.warning("icount: bad integer value %r", val)
        return 0


def clamp_rate(val: float) -> float:
    return round(max(0.0, min(100.0, val)), 2)


def safe_div(numerator: float, denominator: float) -> float:
    return (numerator / denominator) if denominator else 0.0


def rating_or_none(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return round(float(val), 2)
    except (TypeError, ValueError):
        _log.warning("rating_or_none: bad rating %r", val)
        return None


def as_bool(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    return bool(icount(val))
