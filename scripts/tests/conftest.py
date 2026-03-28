"""Shared fixtures for scripts/tests (pytest auto-loads this file)."""
from __future__ import annotations

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
]


@pytest.fixture
def sample_org_id() -> int:
    return SAMPLE_ORG_ID


@pytest.fixture
def sample_period() -> tuple:
    return SAMPLE_START, SAMPLE_END
