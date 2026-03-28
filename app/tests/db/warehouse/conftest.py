import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock

SAMPLE_ORG_ID = 42
SAMPLE_EMP_ID = 7
SAMPLE_CUST_ID = 99
SAMPLE_SVC_ID = 3
SAMPLE_CAMPAIGN_ID = 12
SAMPLE_DATE = date(2026, 1, 1)
SAMPLE_DATE_B = date(2025, 12, 1)


@pytest.fixture
def mock_conn():
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=None)
    return conn


@pytest.fixture
def mock_pool(mock_conn):
    pool = MagicMock()
    pool.acquire = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        )
    )
    return pool
