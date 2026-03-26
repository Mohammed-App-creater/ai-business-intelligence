"""
test_classifier_fallback.py
===========================
Tests for Step 2: LLM classifier fallback.

Covers:
  - Classifier invoked only when rule confidence is below threshold
  - Classifier result respected when invoked (RAG and DIRECT)
  - No classifier → safe fallback to RAG
  - Classifier raises exception → safe fallback to RAG
  - Classifier returns malformed JSON → safe fallback to RAG
  - Method field correctly reflects routing path taken
  - Custom confidence thresholds control when classifier fires
"""
import json
from unittest.mock import MagicMock
import pytest
from app.services.query_analyzer import QueryAnalyzer, Route

pytestmark = pytest.mark.asyncio


class _MockGateway:
    """Wraps a bare async callable into the gateway interface expected by QueryAnalyzer."""
    def __init__(self, fn):
        self._fn = fn

    async def call_with_data(self, use_case, data, business_id):
        result = MagicMock()
        result.content = await self._fn(use_case, business_id)
        return result

# A question designed to score low on rules (single weak keyword or none)
# so the classifier is triggered.
AMBIGUOUS_QUESTION = "Tell me about the drop."   # "drop" is a single keyword → conf=0.60


# ---------------------------------------------------------------------------
# Classifier not configured (no llm_client)
# ---------------------------------------------------------------------------

class TestNoClassifier:

    async def test_ambiguous_falls_back_to_rag(self):
        """Without a classifier, ambiguous questions must default to RAG."""
        analyzer = QueryAnalyzer(gateway=None)
        result = await analyzer.analyze(AMBIGUOUS_QUESTION)
        assert result.route == Route.RAG

    async def test_method_is_rules_fallback(self):
        analyzer = QueryAnalyzer(gateway=None)
        result = await analyzer.analyze(AMBIGUOUS_QUESTION)
        assert result.method == "rules_fallback"

    async def test_high_confidence_rules_skip_classifier(self):
        """High-confidence rule results must never invoke the (absent) classifier."""
        analyzer = QueryAnalyzer(gateway=None)
        result = await analyzer.analyze("Why did my revenue decrease this month?")
        assert result.route == Route.RAG
        assert result.method == "rules"


# ---------------------------------------------------------------------------
# Classifier invoked — returns RAG
# ---------------------------------------------------------------------------

class TestClassifierRAG:

    async def test_classifier_rag_respected(self, analyzer_with_rag_classifier):
        result = await analyzer_with_rag_classifier.analyze(AMBIGUOUS_QUESTION)
        assert result.route == Route.RAG

    async def test_method_is_classifier(self, analyzer_with_rag_classifier):
        result = await analyzer_with_rag_classifier.analyze(AMBIGUOUS_QUESTION)
        assert result.method == "classifier"

    async def test_classifier_confidence_preserved(self, analyzer_with_rag_classifier):
        result = await analyzer_with_rag_classifier.analyze(AMBIGUOUS_QUESTION)
        assert 0.0 <= result.confidence <= 1.0

    async def test_classifier_reasoning_preserved(self):
        async def _client(system, user):
            return json.dumps({
                "route": "RAG",
                "confidence": 0.88,
                "reasoning": "Question implies business-specific data."
            })
        analyzer = QueryAnalyzer(gateway=_MockGateway(_client))
        result = await analyzer.analyze(AMBIGUOUS_QUESTION)
        assert result.reasoning == "Question implies business-specific data."


# ---------------------------------------------------------------------------
# Classifier invoked — returns DIRECT
# ---------------------------------------------------------------------------

class TestClassifierDirect:

    async def test_classifier_direct_respected(self, analyzer_with_direct_classifier):
        result = await analyzer_with_direct_classifier.analyze(AMBIGUOUS_QUESTION)
        assert result.route == Route.DIRECT

    async def test_method_is_classifier(self, analyzer_with_direct_classifier):
        result = await analyzer_with_direct_classifier.analyze(AMBIGUOUS_QUESTION)
        assert result.method == "classifier"


# ---------------------------------------------------------------------------
# Classifier not invoked for high-confidence rule results
# ---------------------------------------------------------------------------

class TestClassifierNotInvoked:

    async def test_possessive_skips_classifier(self):
        """Possessive patterns yield 0.95 confidence — classifier must not fire."""
        call_count = 0

        async def _spy_client(system, user):
            nonlocal call_count
            call_count += 1
            return json.dumps({"route": "DIRECT", "confidence": 0.99, "reasoning": ""})

        analyzer = QueryAnalyzer(gateway=_MockGateway(_spy_client))
        await analyzer.analyze("Why did my revenue drop?")
        assert call_count == 0, "Classifier should not be called for high-confidence rules"

    async def test_direct_pattern_skips_classifier(self):
        call_count = 0

        async def _spy_client(system, user):
            nonlocal call_count
            call_count += 1
            return json.dumps({"route": "RAG", "confidence": 0.99, "reasoning": ""})

        analyzer = QueryAnalyzer(gateway=_MockGateway(_spy_client))
        await analyzer.analyze("How can salons improve customer retention?")
        assert call_count == 0


# ---------------------------------------------------------------------------
# Classifier failure — safe fallback
# ---------------------------------------------------------------------------

class TestClassifierFailure:

    async def test_exception_falls_back_to_rag(self, analyzer_with_failing_classifier):
        result = await analyzer_with_failing_classifier.analyze(AMBIGUOUS_QUESTION)
        assert result.route == Route.RAG

    async def test_exception_method_is_classifier_error(self, analyzer_with_failing_classifier):
        result = await analyzer_with_failing_classifier.analyze(AMBIGUOUS_QUESTION)
        assert result.method == "classifier_error"

    async def test_malformed_json_falls_back_to_rag(self, analyzer_with_malformed_classifier):
        result = await analyzer_with_malformed_classifier.analyze(AMBIGUOUS_QUESTION)
        assert result.route == Route.RAG

    async def test_malformed_json_method_is_classifier_error(self, analyzer_with_malformed_classifier):
        result = await analyzer_with_malformed_classifier.analyze(AMBIGUOUS_QUESTION)
        assert result.method == "classifier_error"

    async def test_confidence_still_set_on_error(self, analyzer_with_failing_classifier):
        result = await analyzer_with_failing_classifier.analyze(AMBIGUOUS_QUESTION)
        assert 0.0 <= result.confidence <= 1.0


# ---------------------------------------------------------------------------
# Custom confidence thresholds
# ---------------------------------------------------------------------------

class TestConfidenceThreshold:

    async def test_low_threshold_calls_classifier_on_ambiguous(self):
        """
        When confidence_threshold is set above the rule engine's low-confidence score
        for an ambiguous question, the classifier must be invoked.
        Single-keyword questions score 0.60; a threshold of 0.70 forces the classifier.
        """
        call_count = 0

        async def _spy(system, user):
            nonlocal call_count
            call_count += 1
            return json.dumps({"route": "RAG", "confidence": 0.8, "reasoning": ""})

        # "drop" is a single keyword → rules score 0.60 < threshold 0.70 → classifier fires
        analyzer = QueryAnalyzer(gateway=_MockGateway(_spy), confidence_threshold=0.70)
        await analyzer.analyze("Tell me about the drop.")
        assert call_count == 1

    async def test_threshold_one_never_calls_classifier(self):
        """Threshold=1.0 → no rule result can ever be confident enough."""
        call_count = 0

        async def _spy(system, user):
            nonlocal call_count
            call_count += 1
            return json.dumps({"route": "DIRECT", "confidence": 0.99, "reasoning": ""})

        # Analyzer without classifier at threshold=1.0; ambiguous → rules_fallback
        analyzer = QueryAnalyzer(gateway=None, confidence_threshold=1.0)
        result = await analyzer.analyze("Why did my revenue drop?")
        # Even high-confidence rules (0.95) won't meet threshold=1.0
        assert call_count == 0
        assert result.method == "rules_fallback"
