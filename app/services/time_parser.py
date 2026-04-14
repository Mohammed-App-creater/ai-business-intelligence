"""
time_parser.py
==============
Extract a ``since_date`` filter from natural-language questions.

Used by ChatService before calling the retriever, so the vector store
can pre-filter by ``period_start >= since_date`` before cosine ranking.

Returns ``None`` when no time reference is found — the retriever then
searches the full history.
"""
from __future__ import annotations

import re
from datetime import date


_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _first_of_month(y: int, m: int) -> date:
    """Return the first day of (y, m), handling month underflow/overflow."""
    while m < 1:
        m += 12
        y -= 1
    while m > 12:
        m -= 12
        y += 1
    return date(y, m, 1)


def _quarter_start(y: int, q: int) -> date:
    """Return the first day of quarter q (1-4) in year y."""
    return date(y, (q - 1) * 3 + 1, 1)


def parse_since_date(question: str, today: date | None = None) -> date | None:
    """
    Parse a natural-language time reference from ``question``.

    Returns the earliest date that should be included in results,
    or ``None`` if no time reference is found.

    Examples (assuming today = 2026-04-14):
        "last month"          → 2026-03-01
        "this month"          → 2026-04-01
        "last quarter"        → 2026-01-01
        "this quarter"        → 2026-04-01
        "last year"           → 2025-01-01
        "this year"           → 2026-01-01
        "last 3 months"       → 2026-01-01
        "past 6 months"       → 2025-10-01
        "year to date" / "ytd"→ 2026-01-01
        "in Q1"               → 2026-01-01
        "in March 2026"       → 2026-03-01
        "since January"       → 2026-01-01
        "in 2025"             → 2025-01-01
        "what's my revenue"   → None  (no time reference)
    """
    if today is None:
        today = date.today()

    q = question.lower()
    y, m = today.year, today.month

    # --- relative: "last N months/quarters/years" ---------------------
    match = re.search(
        r"\b(?:last|past|previous|prior)\s+(\d{1,2})\s+(month|months|quarter|quarters|year|years)\b",
        q,
    )
    if match:
        n = int(match.group(1))
        unit = match.group(2)
        if unit.startswith("month"):
            return _first_of_month(y, m - (n - 1))
        if unit.startswith("quarter"):
            return _first_of_month(y, m - (n * 3 - 2))
        if unit.startswith("year"):
            return date(y - (n - 1), 1, 1)

    # --- "last month" / "this month" ---------------------------------
    if re.search(r"\blast\s+month\b", q):
        return _first_of_month(y, m - 1)
    if re.search(r"\bthis\s+month\b", q):
        return _first_of_month(y, m)

    # --- "last quarter" / "this quarter" -----------------------------
    current_q = (m - 1) // 3 + 1
    if re.search(r"\blast\s+quarter\b", q):
        if current_q == 1:
            return _quarter_start(y - 1, 4)
        return _quarter_start(y, current_q - 1)
    if re.search(r"\bthis\s+quarter\b", q):
        return _quarter_start(y, current_q)

    # --- explicit Q1..Q4 ---------------------------------------------
    match = re.search(r"\bq([1-4])\b(?:\s+(\d{4}))?", q)
    if match:
        qn = int(match.group(1))
        qy = int(match.group(2)) if match.group(2) else y
        return _quarter_start(qy, qn)

    # --- "last year" / "this year" / "ytd" ---------------------------
    if re.search(r"\blast\s+year\b", q):
        return date(y - 1, 1, 1)
    if re.search(r"\bthis\s+year\b|\byear\s+to\s+date\b|\bytd\b", q):
        return date(y, 1, 1)

    # --- explicit year: "in 2025" ------------------------------------
    match = re.search(r"\bin\s+(20\d{2})\b", q)
    if match:
        return date(int(match.group(1)), 1, 1)

    # --- "in March" / "in March 2026" / "since January" --------------
    match = re.search(
        r"\b(?:in|since|during|for)\s+"
        r"(january|february|march|april|may|june|july|august|"
        r"september|october|november|december|"
        r"jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)"
        r"(?:\s+(20\d{2}))?\b",
        q,
    )
    if match:
        month_num = _MONTHS[match.group(1)]
        year_num = int(match.group(2)) if match.group(2) else y
        return date(year_num, month_num, 1)

    return None