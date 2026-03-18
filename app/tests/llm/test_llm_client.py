"""
test_llm_client.py
==================
Tests for LLMClient — retry, timeout, token logging, was_retried.

All tests use mock providers — no real API calls.
"""
from __future__ import annotations

import asyncio
import pytest

from app.services.llm.llm_client import LLMClient, DEFAULT_TIMEOUT_SECONDS
from app.services.llm.types import (
    LLMRateLimitError, LLMProviderError, LLMTimeoutError,
    LLMJsonParseError, OutputMode, UseCase,
)
from conftest import make_mock_provider, make_rate_limit_then_succeed_provider


class TestHappyPath:

    async def test_returns_response_on_success(self, llm_client, rag_request):
        result = await llm_client.call(rag_request)
        assert result.content is not None

    async def test_was_retried_false_on_first_attempt(self, llm_client, rag_request):
        result = await llm_client.call(rag_request)
        assert result.was_retried is False

    async def test_parsed_populated_for_structured_json(self, llm_client, rag_request):
        result = await llm_client.call(rag_request)
        assert result.parsed is not None
        assert "summary" in result.parsed

    async def test_usage_tokens_present(self, llm_client, rag_request):
        result = await llm_client.call(rag_request)
        assert result.usage.input_tokens  > 0
        assert result.usage.output_tokens > 0


class TestRetryLogic:

    async def test_retries_on_rate_limit_then_succeeds(self, rag_request):
        provider, call_count = make_rate_limit_then_succeed_provider(fail_times=1)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=3)
        result = await client.call(rag_request)
        assert call_count[0] == 2    # 1 failure + 1 success
        assert result.was_retried is True

    async def test_retries_up_to_max_retries(self, rag_request):
        provider, call_count = make_rate_limit_then_succeed_provider(fail_times=3)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=3)
        result = await client.call(rag_request)
        assert call_count[0] == 4   # 3 failures + 1 success
        assert result.was_retried is True

    async def test_raises_after_max_retries_exhausted(self, rag_request):
        provider, _ = make_rate_limit_then_succeed_provider(fail_times=10)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=2)
        with pytest.raises(LLMRateLimitError):
            await client.call(rag_request)

    async def test_call_count_matches_max_retries_plus_one(self, rag_request):
        tracker = []
        provider = make_mock_provider(
            raises=LLMRateLimitError("always"),
            call_count_tracker=tracker,
        )
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=2)
        with pytest.raises(LLMRateLimitError):
            await client.call(rag_request)
        assert len(tracker) == 3   # 1 initial + 2 retries

    async def test_provider_error_not_retried(self, rag_request):
        tracker = []
        provider = make_mock_provider(
            raises=LLMProviderError("500 server error"),
            call_count_tracker=tracker,
        )
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=3)
        with pytest.raises(LLMProviderError):
            await client.call(rag_request)
        assert len(tracker) == 1   # Must not retry non-rate-limit errors

    async def test_json_parse_error_not_retried(self, rag_request):
        from app.services.llm.types import LLMJsonParseError
        tracker = []
        provider = make_mock_provider(
            raises=LLMJsonParseError("bad json", raw_content="bad"),
            call_count_tracker=tracker,
        )
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=3)
        with pytest.raises(LLMJsonParseError):
            await client.call(rag_request)
        assert len(tracker) == 1

    async def test_zero_retries_raises_immediately_on_rate_limit(self, rag_request):
        provider, call_count = make_rate_limit_then_succeed_provider(fail_times=1)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=0)
        with pytest.raises(LLMRateLimitError):
            await client.call(rag_request)
        assert call_count[0] == 1


class TestTimeout:

    async def test_timeout_raises_llm_timeout_error(self, rag_request):
        async def _slow_complete(request):
            await asyncio.sleep(10)

        class SlowProvider:
            @property
            def provider_name(self): return "anthropic"
            def supports_native_json(self): return False
            async def complete(self, request): await asyncio.sleep(10)

        client = LLMClient(
            provider=SlowProvider(),
            timeout_seconds=0.05,   # 50ms — will always timeout
            max_retries=0,
        )
        with pytest.raises(LLMTimeoutError):
            await client.call(rag_request)

    async def test_timeout_not_retried(self, rag_request):
        call_count = [0]

        class SlowProvider:
            @property
            def provider_name(self): return "anthropic"
            def supports_native_json(self): return False
            async def complete(self, request):
                call_count[0] += 1
                await asyncio.sleep(10)

        client = LLMClient(
            provider=SlowProvider(),
            timeout_seconds=0.05,
            max_retries=3,
        )
        with pytest.raises(LLMTimeoutError):
            await client.call(rag_request)
        assert call_count[0] == 1   # timeout must not trigger retry

    async def test_fast_response_does_not_timeout(self, llm_client, rag_request):
        # Default timeout is 5s; mock completes instantly
        result = await llm_client.call(rag_request)
        assert result is not None


class TestWasRetried:

    async def test_was_retried_true_after_one_rate_limit(self, rag_request):
        provider, _ = make_rate_limit_then_succeed_provider(fail_times=1)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=3)
        result = await client.call(rag_request)
        assert result.was_retried is True

    async def test_was_retried_false_on_clean_success(self, llm_client, rag_request):
        result = await llm_client.call(rag_request)
        assert result.was_retried is False
