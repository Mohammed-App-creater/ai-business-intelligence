"""
Unit tests for app/api/v1/schemas.py

Tests Pydantic v2 validation for ChatRequest, ChatResponse, ErrorResponse.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.api.v1.schemas import ChatRequest, ChatResponse, ErrorResponse


# ═══════════════════════════════════════════════════════════════════════════
# ChatRequest
# ═══════════════════════════════════════════════════════════════════════════

class TestChatRequest:

    def test_valid_minimal(self):
        req = ChatRequest(
            question="How was revenue last month?",
            business_id="salon_123",
            org_id=7,
        )
        assert req.question == "How was revenue last month?"
        assert req.business_id == "salon_123"
        assert req.org_id == 7
        assert req.conversation_id is None

    def test_valid_with_conversation_id(self):
        req = ChatRequest(
            question="Follow up question",
            business_id="salon_123",
            org_id=7,
            conversation_id="conv_abc123",
        )
        assert req.conversation_id == "conv_abc123"

    def test_question_required(self):
        with pytest.raises(ValidationError) as exc_info:
            ChatRequest(business_id="salon_123", org_id=7)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("question",) for e in errors)

    def test_question_empty_string_rejected(self):
        with pytest.raises(ValidationError):
            ChatRequest(question="", business_id="salon_123", org_id=7)

    def test_question_too_long_rejected(self):
        with pytest.raises(ValidationError):
            ChatRequest(
                question="x" * 2001,
                business_id="salon_123",
                org_id=7,
            )

    def test_question_max_length_accepted(self):
        req = ChatRequest(
            question="x" * 2000,
            business_id="salon_123",
            org_id=7,
        )
        assert len(req.question) == 2000

    def test_business_id_required(self):
        with pytest.raises(ValidationError) as exc_info:
            ChatRequest(question="test", org_id=7)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("business_id",) for e in errors)

    def test_business_id_empty_rejected(self):
        with pytest.raises(ValidationError):
            ChatRequest(question="test", business_id="", org_id=7)

    def test_org_id_required(self):
        with pytest.raises(ValidationError) as exc_info:
            ChatRequest(question="test", business_id="salon_123")
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("org_id",) for e in errors)

    def test_org_id_must_be_positive(self):
        with pytest.raises(ValidationError):
            ChatRequest(question="test", business_id="salon_123", org_id=0)

    def test_org_id_negative_rejected(self):
        with pytest.raises(ValidationError):
            ChatRequest(question="test", business_id="salon_123", org_id=-1)

    def test_serialization(self):
        req = ChatRequest(
            question="Revenue?",
            business_id="salon_123",
            org_id=7,
            conversation_id="conv_1",
        )
        data = req.model_dump()
        assert data == {
            "question": "Revenue?",
            "business_id": "salon_123",
            "org_id": 7,
            "conversation_id": "conv_1",
        }

    def test_deserialization_from_dict(self):
        data = {
            "question": "Revenue?",
            "business_id": "salon_123",
            "org_id": 7,
        }
        req = ChatRequest.model_validate(data)
        assert req.question == "Revenue?"
        assert req.conversation_id is None


# ═══════════════════════════════════════════════════════════════════════════
# ChatResponse
# ═══════════════════════════════════════════════════════════════════════════

class TestChatResponse:

    def test_valid_direct_response(self):
        resp = ChatResponse(
            answer="Retention improves with loyalty programs.",
            route="DIRECT",
            confidence=0.95,
            sources=[],
            latency_ms=1200.5,
        )
        assert resp.route == "DIRECT"
        assert resp.sources == []
        assert resp.conversation_id is None

    def test_valid_rag_response(self):
        resp = ChatResponse(
            answer="Revenue dropped 30% in March.",
            route="RAG",
            confidence=0.88,
            sources=["42_revenue_monthly_2026_03", "42_staff_monthly_2026_03"],
            conversation_id="conv_abc",
            latency_ms=3450.0,
        )
        assert resp.route == "RAG"
        assert len(resp.sources) == 2
        assert resp.conversation_id == "conv_abc"

    def test_answer_required(self):
        with pytest.raises(ValidationError) as exc_info:
            ChatResponse(route="DIRECT", confidence=0.9, latency_ms=100)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("answer",) for e in errors)

    def test_route_required(self):
        with pytest.raises(ValidationError) as exc_info:
            ChatResponse(answer="test", confidence=0.9, latency_ms=100)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("route",) for e in errors)

    def test_confidence_required(self):
        with pytest.raises(ValidationError):
            ChatResponse(answer="test", route="DIRECT", latency_ms=100)

    def test_confidence_below_zero_rejected(self):
        with pytest.raises(ValidationError):
            ChatResponse(
                answer="test", route="DIRECT",
                confidence=-0.1, latency_ms=100,
            )

    def test_confidence_above_one_rejected(self):
        with pytest.raises(ValidationError):
            ChatResponse(
                answer="test", route="DIRECT",
                confidence=1.1, latency_ms=100,
            )

    def test_confidence_boundary_zero(self):
        resp = ChatResponse(
            answer="test", route="DIRECT",
            confidence=0.0, latency_ms=100,
        )
        assert resp.confidence == 0.0

    def test_confidence_boundary_one(self):
        resp = ChatResponse(
            answer="test", route="DIRECT",
            confidence=1.0, latency_ms=100,
        )
        assert resp.confidence == 1.0

    def test_latency_required(self):
        with pytest.raises(ValidationError):
            ChatResponse(answer="test", route="DIRECT", confidence=0.9)

    def test_latency_negative_rejected(self):
        with pytest.raises(ValidationError):
            ChatResponse(
                answer="test", route="DIRECT",
                confidence=0.9, latency_ms=-1.0,
            )

    def test_sources_defaults_empty(self):
        resp = ChatResponse(
            answer="test", route="DIRECT",
            confidence=0.9, latency_ms=100,
        )
        assert resp.sources == []

    def test_serialization(self):
        resp = ChatResponse(
            answer="Revenue dropped.",
            route="RAG",
            confidence=0.85,
            sources=["doc_1"],
            conversation_id="conv_1",
            latency_ms=2500.0,
        )
        data = resp.model_dump()
        assert data["route"] == "RAG"
        assert data["sources"] == ["doc_1"]
        assert data["latency_ms"] == 2500.0

    def test_json_roundtrip(self):
        resp = ChatResponse(
            answer="test", route="DIRECT",
            confidence=0.9, latency_ms=100,
        )
        json_str = resp.model_dump_json()
        restored = ChatResponse.model_validate_json(json_str)
        assert restored == resp


# ═══════════════════════════════════════════════════════════════════════════
# ErrorResponse
# ═══════════════════════════════════════════════════════════════════════════

class TestErrorResponse:

    def test_valid_minimal(self):
        err = ErrorResponse(error="Something went wrong")
        assert err.error == "Something went wrong"
        assert err.detail is None

    def test_valid_with_detail(self):
        err = ErrorResponse(
            error="Internal error",
            detail="LLM timeout after 7 seconds",
        )
        assert err.detail == "LLM timeout after 7 seconds"

    def test_error_required(self):
        with pytest.raises(ValidationError):
            ErrorResponse()

    def test_serialization(self):
        err = ErrorResponse(error="Bad request", detail="Missing question")
        data = err.model_dump()
        assert data == {
            "error": "Bad request",
            "detail": "Missing question",
        }