"""
Shared fixtures for LLM layer tests.
"""
from __future__ import annotations

import json
import pytest

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from app.services.llm.types import (
    LLMRequest, LLMResponse, OutputMode, Provider, TokenUsage, UseCase,
    LLMRateLimitError, LLMProviderError, LLMTimeoutError,
)
from app.services.llm.llm_client import LLMClient
from app.services.llm.llm_gateway import LLMGateway


# ---------------------------------------------------------------------------
# Canonical request fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def rag_request():
    return LLMRequest(
        use_case=UseCase.RAG_CHAT,
        system="You are an analyst.",
        user="Why did revenue drop?",
        business_id="salon_123",
        model="claude-sonnet-4-6",
        output_mode=OutputMode.STRUCTURED_JSON,
        max_tokens=1000,
    )

@pytest.fixture
def classifier_request():
    return LLMRequest(
        use_case=UseCase.CLASSIFIER,
        system="Classify this question.",
        user="Tell me about the drop.",
        business_id="salon_123",
        model="claude-haiku-4-5-20251001",
        output_mode=OutputMode.RAW,
        max_tokens=256,
    )


# ---------------------------------------------------------------------------
# Mock provider factory
# ---------------------------------------------------------------------------

def make_mock_provider(
    content: str = '{"summary": "Revenue dropped.", "root_causes": [], "supporting_data": "", "recommendations": [], "confidence": "high", "data_gaps": null}',
    provider: Provider = Provider.ANTHROPIC,
    input_tokens: int = 500,
    output_tokens: int = 150,
    native_json: bool = False,
    raises: Exception | None = None,
    call_count_tracker: list | None = None,
):
    """
    Returns an object satisfying BaseLLMProvider that can be configured
    to return a specific response or raise a specific exception.
    """

    class MockProvider:
        def __init__(self):
            self.calls = call_count_tracker if call_count_tracker is not None else []

        @property
        def provider_name(self):
            return provider.value

        def supports_native_json(self):
            return native_json

        async def complete(self, request: LLMRequest) -> LLMResponse:
            self.calls.append(request)
            if raises is not None:
                raise raises
            return LLMResponse(
                content=content,
                parsed=json.loads(content) if request.output_mode == OutputMode.STRUCTURED_JSON else None,
                usage=TokenUsage(input_tokens=input_tokens, output_tokens=output_tokens),
                model=request.model,
                provider=provider,
                latency_ms=120.0,
                was_retried=False,
                use_case=request.use_case,
            )

    return MockProvider()


def make_rate_limit_then_succeed_provider(fail_times: int = 1):
    """Provider that raises RateLimitError `fail_times` then succeeds."""
    call_count = [0]

    class Provider_:
        @property
        def provider_name(self):
            return "anthropic"

        def supports_native_json(self):
            return False

        async def complete(self, request: LLMRequest) -> LLMResponse:
            call_count[0] += 1
            if call_count[0] <= fail_times:
                raise LLMRateLimitError("429 rate limit")
            return LLMResponse(
                content="ok",
                parsed=None,
                usage=TokenUsage(100, 50),
                model=request.model,
                provider=Provider.ANTHROPIC,
                latency_ms=100.0,
                was_retried=False,
                use_case=request.use_case,
            )

    return Provider_(), call_count


@pytest.fixture
def mock_provider():
    return make_mock_provider()

@pytest.fixture
def llm_client(mock_provider):
    return LLMClient(provider=mock_provider, timeout_seconds=5.0, max_retries=3)
