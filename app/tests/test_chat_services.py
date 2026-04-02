"""
Unit tests for app/services/chat_service.py

All tests mock the analyzer, retriever, and gateway — no real I/O.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.api.v1.schemas import ChatRequest, ChatResponse
from app.services.chat_service import (
    ChatService,
    DIRECT_SYSTEM_PROMPT,
    LIVE_DATA_REDIRECT,
)
from app.services.query_analyzer import Route
from app.services.retriever import RetrievalContext


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class FakeAnalysis:
    route: Route = Route.RAG
    confidence: float = 0.90
    method: str = "rules"
    matched_keywords: list[str] = field(default_factory=list)
    latency_ms: float = 1.0
    reasoning: str | None = None


@dataclass
class FakeLLMResponse:
    content: str = '{"summary": "Revenue dropped 30%."}'
    model: str = "test-model"
    provider: str = "test"
    latency_ms: float = 500.0


def _make_request(**overrides) -> ChatRequest:
    defaults = {
        "question": "Why did my revenue drop?",
        "business_id": "salon_42",
        "org_id": 42,
        "conversation_id": "conv_123",
    }
    defaults.update(overrides)
    return ChatRequest(**defaults)


def _make_service(
    analysis: FakeAnalysis | None = None,
    retrieval: RetrievalContext | None = None,
    llm_content: str | None = None,
) -> ChatService:
    """Build a ChatService with mocked dependencies."""
    analyzer = AsyncMock()
    analyzer.analyze = AsyncMock(
        return_value=analysis or FakeAnalysis(),
    )

    retriever = AsyncMock()
    retriever.retrieve = AsyncMock(
        return_value=retrieval or RetrievalContext(
            documents=["Revenue was $9,200 in March."],
            doc_ids=["42_revenue_monthly_2026_03"],
            domains_searched=["revenue"],
            total_results=1,
            latency_ms=50.0,
        ),
    )

    gateway = AsyncMock()
    content = llm_content or '{"summary": "Revenue dropped 30%.", "root_causes": ["cancellations increased"], "supporting_data": "$9,200 vs $13,100", "recommendations": ["reduce no-shows"], "confidence": "high", "data_gaps": null}'
    gateway.call = AsyncMock(return_value=FakeLLMResponse(content=content))
    gateway.call_with_data = AsyncMock(return_value=FakeLLMResponse(content=content))

    return ChatService(analyzer, retriever, gateway)


# ═══════════════════════════════════════════════════════════════════════════
# 1. Basic routing
# ═══════════════════════════════════════════════════════════════════════════

class TestRouting:

    @pytest.mark.asyncio
    async def test_direct_route(self):
        service = _make_service(
            analysis=FakeAnalysis(route=Route.DIRECT, confidence=0.90),
        )
        request = _make_request(question="How can salons improve retention?")

        response = await service.handle(request)

        assert response.route == "DIRECT"
        assert response.sources == []
        assert response.confidence == 0.90

    @pytest.mark.asyncio
    async def test_rag_route(self):
        service = _make_service(
            analysis=FakeAnalysis(route=Route.RAG, confidence=0.85),
        )
        request = _make_request()

        response = await service.handle(request)

        assert response.route == "RAG"
        assert "42_revenue_monthly_2026_03" in response.sources

    @pytest.mark.asyncio
    async def test_conversation_id_passed_through(self):
        service = _make_service()
        request = _make_request(conversation_id="conv_abc")

        response = await service.handle(request)

        assert response.conversation_id == "conv_abc"

    @pytest.mark.asyncio
    async def test_latency_recorded(self):
        service = _make_service()
        request = _make_request()

        response = await service.handle(request)

        assert response.latency_ms > 0


# ═══════════════════════════════════════════════════════════════════════════
# 2. DIRECT route details
# ═══════════════════════════════════════════════════════════════════════════

class TestDirectRoute:

    @pytest.mark.asyncio
    async def test_calls_gateway_call(self):
        service = _make_service(
            analysis=FakeAnalysis(route=Route.DIRECT),
            llm_content="Loyalty programs help retain clients.",
        )
        request = _make_request(question="Tips for retention?")

        response = await service.handle(request)

        service._gateway.call.assert_awaited_once()
        call_args = service._gateway.call.call_args[0]
        # UseCase, system, user, business_id
        assert call_args[1] == DIRECT_SYSTEM_PROMPT
        assert call_args[2] == "Tips for retention?"
        assert call_args[3] == "salon_42"

    @pytest.mark.asyncio
    async def test_does_not_call_retriever(self):
        service = _make_service(
            analysis=FakeAnalysis(route=Route.DIRECT),
        )
        request = _make_request(question="General advice?")

        await service.handle(request)

        service._retriever.retrieve.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_raw_content(self):
        service = _make_service(
            analysis=FakeAnalysis(route=Route.DIRECT),
            llm_content="Focus on client experience.",
        )
        request = _make_request(question="How to improve?")

        response = await service.handle(request)

        assert response.answer == "Focus on client experience."


# ═══════════════════════════════════════════════════════════════════════════
# 3. RAG route details
# ═══════════════════════════════════════════════════════════════════════════

class TestRagRoute:

    @pytest.mark.asyncio
    async def test_calls_retriever(self):
        service = _make_service()
        request = _make_request()

        await service.handle(request)

        service._retriever.retrieve.assert_awaited_once()
        call_kwargs = service._retriever.retrieve.call_args[1]
        assert call_kwargs["question"] == "Why did my revenue drop?"
        assert call_kwargs["tenant_id"] == "salon_42"

    @pytest.mark.asyncio
    async def test_calls_gateway_call_with_data(self):
        service = _make_service()
        request = _make_request()

        await service.handle(request)

        service._gateway.call_with_data.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rag_data_has_documents(self):
        ctx = RetrievalContext(
            documents=["Doc 1", "Doc 2"],
            doc_ids=["id_1", "id_2"],
            domains_searched=["revenue"],
            total_results=2,
        )
        service = _make_service(retrieval=ctx)
        request = _make_request()

        await service.handle(request)

        call_args = service._gateway.call_with_data.call_args[0]
        rag_data = call_args[1]
        assert rag_data.documents == ["Doc 1", "Doc 2"]
        assert rag_data.question == "Why did my revenue drop?"
        assert rag_data.business_id == "salon_42"

    @pytest.mark.asyncio
    async def test_sources_from_retrieval(self):
        ctx = RetrievalContext(
            documents=["text"],
            doc_ids=["doc_a", "doc_b"],
            domains_searched=["revenue"],
            total_results=2,
        )
        service = _make_service(retrieval=ctx)
        request = _make_request()

        response = await service.handle(request)

        assert response.sources == ["doc_a", "doc_b"]

    @pytest.mark.asyncio
    async def test_empty_retrieval_still_calls_llm(self):
        """Even with no docs, we still call the LLM — it may say 'insufficient data'."""
        ctx = RetrievalContext(documents=[], doc_ids=[], total_results=0)
        service = _make_service(retrieval=ctx)
        request = _make_request()

        response = await service.handle(request)

        service._gateway.call_with_data.assert_awaited_once()
        assert response.sources == []


# ═══════════════════════════════════════════════════════════════════════════
# 4. Answer extraction
# ═══════════════════════════════════════════════════════════════════════════

class TestAnswerExtraction:

    @pytest.mark.asyncio
    async def test_extracts_summary_from_json(self):
        content = json.dumps({
            "summary": "Revenue dropped 30%.",
            "root_causes": ["cancellations"],
            "supporting_data": "$9,200 vs $13,100",
            "recommendations": ["reduce no-shows"],
            "confidence": "high",
            "data_gaps": None,
        })
        service = _make_service(llm_content=content)
        request = _make_request()

        response = await service.handle(request)

        assert "Revenue dropped 30%" in response.answer

    @pytest.mark.asyncio
    async def test_includes_supporting_data(self):
        content = json.dumps({
            "summary": "Revenue dropped.",
            "supporting_data": "$9,200 in March vs $13,100 in February",
            "root_causes": [],
            "recommendations": [],
            "confidence": "high",
            "data_gaps": None,
        })
        service = _make_service(llm_content=content)
        request = _make_request()

        response = await service.handle(request)

        assert "$9,200 in March" in response.answer

    @pytest.mark.asyncio
    async def test_includes_recommendations(self):
        content = json.dumps({
            "summary": "Revenue dropped.",
            "supporting_data": None,
            "root_causes": [],
            "recommendations": ["reduce no-shows", "add promotions"],
            "confidence": "high",
            "data_gaps": None,
        })
        service = _make_service(llm_content=content)
        request = _make_request()

        response = await service.handle(request)

        assert "reduce no-shows" in response.answer

    @pytest.mark.asyncio
    async def test_fallback_to_raw_content(self):
        """If LLM returns non-JSON, use raw content."""
        service = _make_service(llm_content="Here's my analysis: revenue dropped.")
        request = _make_request()

        response = await service.handle(request)

        assert response.answer == "Here's my analysis: revenue dropped."

    @pytest.mark.asyncio
    async def test_malformed_json_falls_back(self):
        service = _make_service(llm_content='{"summary": incomplete')
        request = _make_request()

        response = await service.handle(request)

        assert '{"summary": incomplete' in response.answer


# ═══════════════════════════════════════════════════════════════════════════
# 5. Live-data detection
# ═══════════════════════════════════════════════════════════════════════════

class TestLiveDataDetection:

    @pytest.mark.asyncio
    async def test_today_triggers_redirect(self):
        service = _make_service()
        request = _make_request(question="How many appointments do I have today?")

        response = await service.handle(request)

        assert LIVE_DATA_REDIRECT in response.answer

    @pytest.mark.asyncio
    async def test_right_now_triggers_redirect(self):
        service = _make_service()
        request = _make_request(question="What's happening right now?")

        response = await service.handle(request)

        assert LIVE_DATA_REDIRECT in response.answer

    @pytest.mark.asyncio
    async def test_currently_triggers_redirect(self):
        service = _make_service()
        request = _make_request(question="Who is currently working?")

        response = await service.handle(request)

        assert LIVE_DATA_REDIRECT in response.answer

    @pytest.mark.asyncio
    async def test_upcoming_triggers_redirect(self):
        service = _make_service()
        request = _make_request(question="Show me upcoming appointments")

        response = await service.handle(request)

        assert LIVE_DATA_REDIRECT in response.answer

    @pytest.mark.asyncio
    async def test_this_morning_triggers_redirect(self):
        service = _make_service()
        request = _make_request(question="How was business this morning?")

        response = await service.handle(request)

        assert LIVE_DATA_REDIRECT in response.answer

    @pytest.mark.asyncio
    async def test_historical_does_not_trigger(self):
        service = _make_service()
        request = _make_request(question="How was revenue last month?")

        response = await service.handle(request)

        assert LIVE_DATA_REDIRECT not in response.answer

    @pytest.mark.asyncio
    async def test_redirect_preserves_conversation_id(self):
        service = _make_service()
        request = _make_request(
            question="What's happening today?",
            conversation_id="conv_xyz",
        )

        response = await service.handle(request)

        assert response.conversation_id == "conv_xyz"

    @pytest.mark.asyncio
    async def test_redirect_does_not_call_retriever(self):
        service = _make_service()
        request = _make_request(question="Show me today's revenue")

        await service.handle(request)

        service._retriever.retrieve.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_redirect_does_not_call_gateway(self):
        service = _make_service()
        request = _make_request(question="Who's working right now?")

        await service.handle(request)

        service._gateway.call.assert_not_awaited()
        service._gateway.call_with_data.assert_not_awaited()


# ═══════════════════════════════════════════════════════════════════════════
# 6. Error handling
# ═══════════════════════════════════════════════════════════════════════════

class TestErrorHandling:

    @pytest.mark.asyncio
    async def test_analyzer_failure_returns_error_response(self):
        service = _make_service()
        service._analyzer.analyze.side_effect = RuntimeError("Analyzer down")
        request = _make_request()

        response = await service.handle(request)

        assert "something went wrong" in response.answer.lower()
        assert response.route == "ERROR"

    @pytest.mark.asyncio
    async def test_retriever_failure_returns_error_response(self):
        service = _make_service()
        service._retriever.retrieve.side_effect = RuntimeError("DB down")
        request = _make_request()

        response = await service.handle(request)

        assert "something went wrong" in response.answer.lower()

    @pytest.mark.asyncio
    async def test_gateway_failure_returns_error_response(self):
        service = _make_service()
        service._gateway.call_with_data.side_effect = RuntimeError("LLM timeout")
        request = _make_request()

        response = await service.handle(request)

        assert "something went wrong" in response.answer.lower()

    @pytest.mark.asyncio
    async def test_error_response_has_latency(self):
        service = _make_service()
        service._analyzer.analyze.side_effect = RuntimeError("fail")
        request = _make_request()

        response = await service.handle(request)

        assert response.latency_ms > 0

    @pytest.mark.asyncio
    async def test_never_raises(self):
        service = _make_service()
        service._analyzer.analyze.side_effect = Exception("Unexpected")
        request = _make_request()

        # Should NOT raise
        response = await service.handle(request)
        assert isinstance(response, ChatResponse)