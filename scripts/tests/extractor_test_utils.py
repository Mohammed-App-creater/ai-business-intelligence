"""Mock pool helpers for extractor unit tests."""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date
from unittest.mock import AsyncMock, MagicMock

SAMPLE_ORG_ID = 42
SAMPLE_START = date(2026, 1, 1)
SAMPLE_END = date(2026, 1, 31)


def make_mock_pool(rows=None, side_effect=None):
    """
    Returns a mock aiomysql-style pool and its cursor.
    Default: cursor.fetchall() returns `rows` for every execute.
    """
    rows = rows or []
    cursor = MagicMock()
    cursor.execute = AsyncMock(side_effect=side_effect)
    cursor.fetchall = AsyncMock(return_value=rows)

    @asynccontextmanager
    async def cursor_ctx():
        yield cursor

    conn = MagicMock()
    conn.cursor = MagicMock(side_effect=lambda *a, **kw: cursor_ctx())

    @asynccontextmanager
    async def acquire():
        yield conn

    pool = MagicMock()
    pool.acquire = acquire
    return pool, cursor
