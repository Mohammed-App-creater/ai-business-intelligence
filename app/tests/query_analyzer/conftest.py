"""
Shared pytest fixtures for Query Analyzer tests.
"""
import json
from unittest.mock import MagicMock
import pytest
from app.services.query_analyzer import QueryAnalyzer


class _MockGateway:
    """Wraps a bare async callable into the gateway interface expected by QueryAnalyzer."""
    def __init__(self, fn):
        self._fn = fn

    async def call_with_data(self, use_case, data, business_id):
        result = MagicMock()
        result.content = await self._fn(use_case, business_id)
        return result


# ---------------------------------------------------------------------------
# Analyzer fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def analyzer():
    """Rules-only analyzer — no LLM client."""
    return QueryAnalyzer()


@pytest.fixture
def analyzer_with_threshold():
    """Factory: create analyzer with a custom confidence threshold."""
    def _make(threshold: float):
        return QueryAnalyzer(confidence_threshold=threshold)
    return _make


# ---------------------------------------------------------------------------
# Mock LLM client helpers
# ---------------------------------------------------------------------------

def make_llm_client(route: str, confidence: float = 0.85, reasoning: str = "mocked"):
    """
    Returns an async callable that mimics an LLM classifier response,
    always returning the specified route.
    """
    async def _client(system: str, user: str) -> str:
        return json.dumps({
            "route": route,
            "confidence": confidence,
            "reasoning": reasoning,
        })
    return _client


def make_failing_llm_client():
    """Returns an async callable that always raises an exception."""
    async def _client(system: str, user: str) -> str:
        raise RuntimeError("LLM service unavailable")
    return _client


def make_malformed_llm_client():
    """Returns an async callable that returns invalid JSON."""
    async def _client(system: str, user: str) -> str:
        return "not valid json {"
    return _client


@pytest.fixture
def analyzer_with_rag_classifier():
    """Analyzer wired to a classifier that always returns RAG."""
    return QueryAnalyzer(gateway=_MockGateway(make_llm_client("RAG")))


@pytest.fixture
def analyzer_with_direct_classifier():
    """Analyzer wired to a classifier that always returns DIRECT."""
    return QueryAnalyzer(gateway=_MockGateway(make_llm_client("DIRECT")))


@pytest.fixture
def analyzer_with_failing_classifier():
    """Analyzer wired to a classifier that always fails."""
    return QueryAnalyzer(gateway=_MockGateway(make_failing_llm_client()))


@pytest.fixture
def analyzer_with_malformed_classifier():
    """Analyzer wired to a classifier that returns malformed JSON."""
    return QueryAnalyzer(gateway=_MockGateway(make_malformed_llm_client()))
