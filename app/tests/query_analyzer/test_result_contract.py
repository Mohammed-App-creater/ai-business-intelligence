"""
test_result_contract.py
=======================
Tests for the AnalysisResult dataclass contract.

Ensures every code path returns a valid, fully-populated AnalysisResult
that downstream components (cache, RAG pipeline, LLM client) can safely
consume without defensive None-checks.

Covers:
  - Required fields always present and correctly typed
  - Route is always a valid Route enum member
  - Confidence always in [0.0, 1.0]
  - method string always one of the known values
  - latency_ms always non-negative
  - matched_keywords always a list (never None)
"""
import json
import pytest
from app.services.query_analyzer import QueryAnalyzer, Route, AnalysisResult

pytestmark = pytest.mark.asyncio

KNOWN_METHODS = {"rules", "rules_fallback", "classifier", "classifier_error"}

REPRESENTATIVE_QUESTIONS = [
    # High-confidence RAG via possessive
    "Why did my revenue decrease this month?",
    # High-confidence DIRECT via advice pattern
    "How can salons reduce no-shows?",
    # Multi-keyword RAG
    "Which staff has the best cancellation rate and revenue?",
    # Ambiguous single keyword
    "Tell me about the drop.",
    # Empty
    "",
    # Very long
    "revenue " * 200,
]


async def _analyze(question: str, llm_client=None) -> AnalysisResult:
    return await QueryAnalyzer(llm_client=llm_client).analyze(question, "test")


# ---------------------------------------------------------------------------
# Field presence and types
# ---------------------------------------------------------------------------

class TestFieldContract:

    @pytest.mark.parametrize("question", REPRESENTATIVE_QUESTIONS)
    async def test_route_is_route_enum(self, question):
        r = await _analyze(question)
        assert isinstance(r.route, Route)

    @pytest.mark.parametrize("question", REPRESENTATIVE_QUESTIONS)
    async def test_confidence_in_range(self, question):
        r = await _analyze(question)
        assert 0.0 <= r.confidence <= 1.0, f"confidence={r.confidence} out of range"

    @pytest.mark.parametrize("question", REPRESENTATIVE_QUESTIONS)
    async def test_method_is_known_string(self, question):
        r = await _analyze(question)
        assert r.method in KNOWN_METHODS, f"Unknown method: {r.method!r}"

    @pytest.mark.parametrize("question", REPRESENTATIVE_QUESTIONS)
    async def test_latency_non_negative(self, question):
        r = await _analyze(question)
        assert r.latency_ms >= 0.0

    @pytest.mark.parametrize("question", REPRESENTATIVE_QUESTIONS)
    async def test_matched_keywords_is_list(self, question):
        r = await _analyze(question)
        assert isinstance(r.matched_keywords, list)

    @pytest.mark.parametrize("question", REPRESENTATIVE_QUESTIONS)
    async def test_reasoning_is_str_or_none(self, question):
        r = await _analyze(question)
        assert r.reasoning is None or isinstance(r.reasoning, str)


# ---------------------------------------------------------------------------
# Contract holds through classifier path
# ---------------------------------------------------------------------------

class TestClassifierPathContract:

    async def test_classifier_rag_result_valid(self):
        async def _client(s, u):
            return json.dumps({"route": "RAG", "confidence": 0.9, "reasoning": "ok"})
        r = await _analyze("Tell me about the drop.", llm_client=_client)
        assert isinstance(r.route, Route)
        assert 0.0 <= r.confidence <= 1.0
        assert r.method in KNOWN_METHODS

    async def test_classifier_direct_result_valid(self):
        async def _client(s, u):
            return json.dumps({"route": "DIRECT", "confidence": 0.85, "reasoning": "ok"})
        r = await _analyze("Tell me about the drop.", llm_client=_client)
        assert isinstance(r.route, Route)
        assert 0.0 <= r.confidence <= 1.0

    async def test_classifier_error_result_valid(self):
        async def _client(s, u):
            raise RuntimeError("boom")
        r = await _analyze("Tell me about the drop.", llm_client=_client)
        assert isinstance(r.route, Route)
        assert 0.0 <= r.confidence <= 1.0
        assert r.method == "classifier_error"

    async def test_classifier_bad_confidence_clamped_or_valid(self):
        """If classifier returns an out-of-range confidence, result must still parse."""
        async def _client(s, u):
            # Deliberately weird values
            return json.dumps({"route": "RAG", "confidence": 999, "reasoning": ""})
        # Should not raise — module must handle gracefully
        r = await _analyze("Tell me about the drop.", llm_client=_client)
        assert isinstance(r.route, Route)


# ---------------------------------------------------------------------------
# Route enum completeness
# ---------------------------------------------------------------------------

class TestRouteEnum:

    async def test_route_values(self):
        assert Route.DIRECT == "DIRECT"
        assert Route.RAG == "RAG"
        assert Route.AGENT == "AGENT"

    async def test_route_is_str(self):
        assert isinstance(Route.RAG, str)
        assert isinstance(Route.DIRECT, str)
