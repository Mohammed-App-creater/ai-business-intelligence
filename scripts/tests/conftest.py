"""Shared fixtures for scripts/tests (pytest auto-loads this file)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from scripts.tests.extractor_test_utils import (
    SAMPLE_END,
    SAMPLE_ORG_ID,
    SAMPLE_START,
    make_mock_pool,
)

__all__ = [
    "SAMPLE_END",
    "SAMPLE_ORG_ID",
    "SAMPLE_START",
    "make_mock_pool",
    "make_mock_wh_pool",
]


def make_mock_wh_pool(execute_return: str = "INSERT 0 1"):
    """Mock asyncpg-style PGPool for warehouse writes."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=execute_return)

    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool, conn


@pytest.fixture
def sample_org_id() -> int:
    return SAMPLE_ORG_ID


@pytest.fixture
def sample_period() -> tuple:
    return SAMPLE_START, SAMPLE_END
