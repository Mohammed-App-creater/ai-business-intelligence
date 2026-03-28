"""Small helpers for ETL extractors."""
from __future__ import annotations

from datetime import date, timedelta


def period_end_exclusive(period_end: date) -> date:
    return period_end + timedelta(days=1)
