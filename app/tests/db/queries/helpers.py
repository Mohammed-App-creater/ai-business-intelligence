"""
Shared test helpers for DB query tests.
Importable as a regular module (not a pytest conftest fixture).
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock


ORG_ID    = 42
FROM_DATE = "2026-01-01"
TO_DATE   = "2026-03-31"


def make_mock_pool(rows=None, raises=None):
    rows = rows or []

    cursor = MagicMock()
    cursor.execute  = AsyncMock(side_effect=raises if raises else None)
    cursor.fetchall = AsyncMock(return_value=rows)
    cursor.__aenter__ = AsyncMock(return_value=cursor)
    cursor.__aexit__  = AsyncMock(return_value=False)

    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)

    pool = MagicMock()

    @asynccontextmanager
    async def acquire():
        yield conn

    pool.acquire = acquire
    return pool, cursor


def revenue_rows(n=2):
    return [
        {
            "month": f"2026-0{i + 1}",
            "payment_type": "Card",
            "visit_count": 100 + i * 10,
            "total_revenue": 9000.0 + i * 1000,
            "service_revenue": 8000.0 + i * 900,
            "tips": 600.0 + i * 50,
            "discounts": 200.0,
            "gift_card_redeemed": 100.0,
            "tax": 140.0,
        }
        for i in range(n)
    ]


def expense_rows(n=2):
    return [
        {
            "month": f"2026-0{i + 1}",
            "category": "Supplies",
            "subcategory": "Hair Products",
            "expense_count": 8 + i,
            "total": 1200.0 + i * 100,
        }
        for i in range(n)
    ]


def service_rows(n=3):
    names = ["Balayage", "Haircut", "Color Treatment"]
    return [
        {
            "service_id": i + 1,
            "service_name": names[i % len(names)],
            "unit_price": 80.0 + i * 20,
            "booking_count": 140 - i * 20,
            "revenue": 11200.0 - i * 1600,
            "avg_price_charged": 80.0 + i * 20,
        }
        for i in range(n)
    ]


def net_profit_rows():
    return [
        {"month": "2026-01", "revenue": 12000.0, "expenses": 3200.0, "net_profit": 8800.0},
        {"month": "2026-02", "revenue": 13100.0, "expenses": 3400.0, "net_profit": 9700.0},
        {"month": "2026-03", "revenue": 9200.0,  "expenses": 3000.0, "net_profit": 6200.0},
    ]
