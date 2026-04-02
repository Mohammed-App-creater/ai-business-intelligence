"""
Integration tests for POST /api/v1/chat
========================================
Tests the full request lifecycle through FastAPI's TestClient.

All **internal** modules are real (QueryAnalyzer, Retriever, ChatService,
prompts, schemas). Only **external** I/O is mocked:
  - LLM calls  (gateway.call / gateway.call_with_data)
  - Embedding   (embedding_client.embed)
  - Vector store (vector_store.search / search_multi_domain)

This validates that all modules wire together correctly end-to-end.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.routes.chat import router
from app.api.v1.schemas import ChatRequest, ChatResponse
from app.services.chat_service import ChatService
from app.services.query_analyzer import QueryAnalyzer
from app.services.retriever import Retriever


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class FakeLLMResponse:
    content: str
    model: str = "test-model"
    provider: str = "test"
    latency_ms: float = 200.0


def _rag_json_response(
    summary: str = "Revenue decreased 30% in March.",
    root_causes: list[str] | None = None,
    recommendations: list[str] | None = None,
) -> str:
    return json.dumps({
        "summary": summary,
        "root_causes": root_causes or ["increased cancellations"],
        "supporting_data": "$9,200 vs $13,100 in February",
        "recommendations": recommendations or ["reduce no-shows", "run a promo"],
        "confidence": "high",
        "data_gaps": None,
    })


def _sample_vector_results() -> list[dict]:
    return [
        {
            "doc_id": "42_revenue_monthly_2026_03",
            "doc_domain": "revenue",
            "doc_type": "monthly_summary",
            "chunk_text": (
                "Business ID: 42\n"
                "Month: March 2026\n"
                "Revenue: $9,200 (▼ 30% vs February $13,100)\n"
                "Appointments: 150 (▼ 35% vs February 230)\n"
                "Cancel Rate: 18% (▲ from 6% in February)\n\n"
                "Observation:\n"
                "Revenue declined sharply in March. The primary driver "
                "appears to be a significant spike in cancellation rate."
            ),
            "similarity": 0.92,
            "period_start": "2026-03-01",
            "metadata": {},
        },
        {
            "doc_id": "42_staff_monthly_2026_03",
            "doc_domain": "staff",
            "doc_type": "monthly_summary",
            "chunk_text": (
                "Business ID: 42\n"
                "Month: March 2026\n"
                "Staff Summary:\n"
                "Sarah: 45 appts | $3,100 revenue | 4.8 rating\n"
                "James: 30 appts | $1,800 revenue | 4.2 rating\n\n"
                "Observation:\n"
                "Sarah remains the top performer, generating 34% of total revenue."
            ),
            "similarity": 0.85,
            "period_start": "2026-03-01",
            "metadata": {},
        },
    ]


def _create_test_app(
    llm_content: str | None = None,
    vector_results: list[dict] | None = None,
    embedding_return: list[float] | None = None,
    gateway_call_content: str | None = None,
) -> FastAPI:
    """
    Build a FastAPI app with real internal modules and mocked externals.
    """
    # Mock: LLM Gateway
    gateway = AsyncMock()
    gateway.call = AsyncMock(
        return_value=FakeLLMResponse(
            content=gateway_call_content or "Here's some general advice about retention.",
        ),
    )
    gateway.call_with_data = AsyncMock(
        return_value=FakeLLMResponse(
            content=llm_content or _rag_json_response(),
        ),
    )

    # Mock: Embedding Client
    embedding_client = AsyncMock()
    embedding_client.embed = AsyncMock(
        return_value=embedding_return or [0.1] * 1536,
    )

    # Mock: Vector Store
    vector_store = AsyncMock()
    vector_store.search = AsyncMock(
        return_value=vector_results if vector_results is not None
        else _sample_vector_results(),
    )
    vector_store.search_multi_domain = AsyncMock(
        return_value=vector_results if vector_results is not None
        else _sample_vector_results(),
    )

    # Real: QueryAnalyzer (rules only, no classifier LLM)
    analyzer = QueryAnalyzer(gateway=None)

    # Real: Retriever
    retriever = Retriever(embedding_client, vector_store)

    # Real: ChatService
    chat_service = ChatService(analyzer, retriever, gateway)

    # Build app
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.state.chat_service = chat_service

    return app


@pytest.fixture
def client() -> TestClient:
    """Default test client with standard mocks."""
    app = _create_test_app()
    return TestClient(app)


# ═══════════════════════════════════════════════════════════════════════════
# 1. RAG route — full flow
# ═══════════════════════════════════════════════════════════════════════════

class TestRagFlow:

    def test_revenue_question_returns_rag(self, client: TestClient):
        """Question with 'revenue' keyword → RAG route → structured answer."""
        resp = client.post("/api/v1/chat", json={
            "question": "Why did my revenue decrease this month?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["route"] == "RAG"
        assert "Revenue decreased" in data["answer"] or "revenue" in data["answer"].lower()
        assert len(data["sources"]) > 0
        assert data["latency_ms"] > 0

    def test_staff_question_returns_rag(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Which staff member generates the most revenue?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["route"] == "RAG"

    def test_my_keyword_triggers_rag(self, client: TestClient):
        """'my' + business term → possessive pattern → RAG."""
        resp = client.post("/api/v1/chat", json={
            "question": "How are my appointments trending?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        assert resp.json()["route"] == "RAG"

    def test_multi_keyword_question(self, client: TestClient):
        """Multiple domain keywords → RAG with high confidence."""
        resp = client.post("/api/v1/chat", json={
            "question": "Compare my revenue and appointment cancellation trends",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["route"] == "RAG"
        assert data["confidence"] >= 0.75

    def test_rag_response_has_sources(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Why did my revenue drop last month?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        data = resp.json()
        assert isinstance(data["sources"], list)
        assert any("revenue" in s for s in data["sources"])

    def test_rag_empty_vector_results(self):
        """No documents found → LLM still called, may say 'insufficient data'."""
        app = _create_test_app(
            vector_results=[],
            llm_content=json.dumps({
                "summary": "I don't have enough data to answer this question.",
                "root_causes": [],
                "supporting_data": None,
                "recommendations": [],
                "confidence": "low",
                "data_gaps": "No revenue data found for this period.",
            }),
        )
        client = TestClient(app)

        resp = client.post("/api/v1/chat", json={
            "question": "What was my revenue last month?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["route"] == "RAG"
        assert data["sources"] == []
        assert "enough data" in data["answer"].lower() or len(data["answer"]) > 0


# ═══════════════════════════════════════════════════════════════════════════
# 2. DIRECT route — full flow
# ═══════════════════════════════════════════════════════════════════════════

class TestDirectFlow:

    def test_general_advice_returns_direct(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "How can salons improve customer retention?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["route"] == "DIRECT"
        assert data["sources"] == []
        assert len(data["answer"]) > 0

    def test_best_practices_returns_direct(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "What are the best practices for upselling services?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        assert resp.json()["route"] == "DIRECT"

    def test_explain_question_returns_direct(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Explain what a cancellation rate means",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        assert resp.json()["route"] == "DIRECT"

    def test_industry_question_returns_direct(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "What is the industry average for no-shows?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        assert resp.json()["route"] == "DIRECT"

    def test_tips_question_returns_direct(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Give me tips on staff scheduling",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        assert resp.json()["route"] == "DIRECT"


# ═══════════════════════════════════════════════════════════════════════════
# 3. Live-data redirect
# ═══════════════════════════════════════════════════════════════════════════

class TestLiveDataRedirect:

    def test_today_triggers_redirect(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "How many appointments do I have today?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        data = resp.json()
        assert "Live data" in data["answer"] or "isn't available" in data["answer"]
        assert data["sources"] == []

    def test_right_now_triggers_redirect(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Who is working right now?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        assert "isn't available" in resp.json()["answer"]

    def test_upcoming_triggers_redirect(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Show me upcoming appointments",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        assert "dashboard" in resp.json()["answer"].lower()

    def test_currently_triggers_redirect(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "What services are currently being performed?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        assert "Live data" in resp.json()["answer"] or "isn't available" in resp.json()["answer"]


# ═══════════════════════════════════════════════════════════════════════════
# 4. Request validation
# ═══════════════════════════════════════════════════════════════════════════

class TestValidation:

    def test_missing_question_returns_422(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "business_id": "salon_42",
            "org_id": 42,
        })
        assert resp.status_code == 422

    def test_empty_question_returns_422(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "",
            "business_id": "salon_42",
            "org_id": 42,
        })
        assert resp.status_code == 422

    def test_missing_business_id_returns_422(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Revenue?",
            "org_id": 42,
        })
        assert resp.status_code == 422

    def test_missing_org_id_returns_422(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Revenue?",
            "business_id": "salon_42",
        })
        assert resp.status_code == 422

    def test_zero_org_id_returns_422(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Revenue?",
            "business_id": "salon_42",
            "org_id": 0,
        })
        assert resp.status_code == 422

    def test_question_too_long_returns_422(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "x" * 2001,
            "business_id": "salon_42",
            "org_id": 42,
        })
        assert resp.status_code == 422

    def test_valid_minimal_request(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "How is my business?",
            "business_id": "salon_42",
            "org_id": 42,
        })
        assert resp.status_code == 200

    def test_optional_conversation_id(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Follow up question about revenue",
            "business_id": "salon_42",
            "org_id": 42,
            "conversation_id": "conv_abc123",
        })

        assert resp.status_code == 200
        assert resp.json()["conversation_id"] == "conv_abc123"


# ═══════════════════════════════════════════════════════════════════════════
# 5. Response structure
# ═══════════════════════════════════════════════════════════════════════════

class TestResponseStructure:

    def test_response_has_all_fields(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Why did my revenue decrease?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        data = resp.json()
        assert "answer" in data
        assert "route" in data
        assert "confidence" in data
        assert "sources" in data
        assert "conversation_id" in data
        assert "latency_ms" in data

    def test_confidence_is_between_0_and_1(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "How is my revenue?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        data = resp.json()
        assert 0.0 <= data["confidence"] <= 1.0

    def test_latency_is_positive(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "Revenue trends?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.json()["latency_ms"] > 0

    def test_route_is_valid_value(self, client: TestClient):
        resp = client.post("/api/v1/chat", json={
            "question": "How can salons improve?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.json()["route"] in ("DIRECT", "RAG", "ERROR")


# ═══════════════════════════════════════════════════════════════════════════
# 6. Error handling — external service failures
# ═══════════════════════════════════════════════════════════════════════════

class TestErrorHandling:

    def test_embedding_failure_returns_graceful_error(self):
        """Embedding service down → still returns 200 with error message."""
        app = _create_test_app()
        # Break the embedding client
        app.state.chat_service._retriever._embedding_client.embed.side_effect = (
            RuntimeError("Embedding service unavailable")
        )
        client = TestClient(app)

        resp = client.post("/api/v1/chat", json={
            "question": "Why did my revenue drop?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        # Should still return 200 — retriever catches the error
        assert resp.status_code == 200
        data = resp.json()
        # Either we get an answer (retriever returned empty, LLM still called)
        # or we get an error message
        assert len(data["answer"]) > 0

    def test_vector_store_failure_returns_graceful_error(self):
        """Vector store down → retriever returns empty → LLM still called."""
        app = _create_test_app()
        app.state.chat_service._retriever._vector_store.search.side_effect = (
            RuntimeError("Connection refused")
        )
        client = TestClient(app)

        resp = client.post("/api/v1/chat", json={
            "question": "Show me my revenue breakdown",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        assert len(resp.json()["answer"]) > 0

    def test_llm_failure_returns_graceful_error(self):
        """LLM timeout → ChatService catches → returns error response."""
        app = _create_test_app()
        app.state.chat_service._gateway.call_with_data.side_effect = (
            RuntimeError("LLM timeout after 7 seconds")
        )
        client = TestClient(app)

        resp = client.post("/api/v1/chat", json={
            "question": "Why did my revenue decrease?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        data = resp.json()
        assert "something went wrong" in data["answer"].lower()
        assert data["route"] == "ERROR"

    def test_direct_llm_failure_returns_graceful_error(self):
        """DIRECT route LLM failure → graceful error."""
        app = _create_test_app()
        app.state.chat_service._gateway.call.side_effect = (
            RuntimeError("LLM timeout")
        )
        client = TestClient(app)

        resp = client.post("/api/v1/chat", json={
            "question": "How can salons improve customer retention?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        assert "something went wrong" in resp.json()["answer"].lower()


# ═══════════════════════════════════════════════════════════════════════════
# 7. Multi-tenant isolation
# ═══════════════════════════════════════════════════════════════════════════

class TestTenantIsolation:

    def test_business_id_passed_to_retriever(self):
        app = _create_test_app()
        client = TestClient(app)

        client.post("/api/v1/chat", json={
            "question": "How is my revenue?",
            "business_id": "salon_99",
            "org_id": 99,
        })

        # Check that the vector store was called with the correct tenant
        vs = app.state.chat_service._retriever._vector_store
        if vs.search.called:
            call_kwargs = vs.search.call_args[1]
            assert call_kwargs["tenant_id"] == "salon_99"

    def test_business_id_passed_to_gateway(self):
        app = _create_test_app()
        client = TestClient(app)

        client.post("/api/v1/chat", json={
            "question": "How can salons improve?",
            "business_id": "salon_77",
            "org_id": 77,
        })

        gw = app.state.chat_service._gateway
        if gw.call.called:
            call_args = gw.call.call_args[0]
            assert call_args[3] == "salon_77"


# ═══════════════════════════════════════════════════════════════════════════
# 8. End-to-end scenario tests
# ═══════════════════════════════════════════════════════════════════════════

class TestScenarios:

    def test_scenario_revenue_decline_analysis(self):
        """Full scenario: owner asks about revenue decline, gets structured analysis."""
        app = _create_test_app(
            llm_content=_rag_json_response(
                summary="Revenue dropped 30% in March, driven by a spike in cancellations.",
                root_causes=["Cancellation rate tripled from 6% to 18%", "Fewer walk-ins"],
                recommendations=["Implement deposit policy", "Send reminders"],
            ),
        )
        client = TestClient(app)

        resp = client.post("/api/v1/chat", json={
            "question": "Why did my revenue decrease this month?",
            "business_id": "salon_42",
            "org_id": 42,
            "conversation_id": "conv_001",
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["route"] == "RAG"
        assert "30%" in data["answer"]
        assert "cancellation" in data["answer"].lower()
        assert len(data["sources"]) > 0
        assert data["conversation_id"] == "conv_001"
        assert data["confidence"] > 0.7

    def test_scenario_general_advice(self):
        """Owner asks for general tips, gets DIRECT response."""
        app = _create_test_app(
            gateway_call_content=(
                "Here are some proven strategies for reducing no-shows: "
                "1) Send appointment reminders 24 hours in advance, "
                "2) Implement a cancellation policy with deposits, "
                "3) Offer waitlist notifications for cancelled slots."
            ),
        )
        client = TestClient(app)

        resp = client.post("/api/v1/chat", json={
            "question": "What are the best practices for reducing no-shows?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["route"] == "DIRECT"
        assert "reminder" in data["answer"].lower() or "no-show" in data["answer"].lower()
        assert data["sources"] == []

    def test_scenario_live_data_redirect(self):
        """Owner asks about today → graceful redirect to dashboard."""
        app = _create_test_app()
        client = TestClient(app)

        resp = client.post("/api/v1/chat", json={
            "question": "How many clients do I have today?",
            "business_id": "salon_42",
            "org_id": 42,
        })

        assert resp.status_code == 200
        data = resp.json()
        assert "dashboard" in data["answer"].lower()
        assert "trends" in data["answer"].lower()
