"""
test_llm_client.py
==================
Tests for LLMClient — retry, timeout, token logging, was_retried.

All tests use mock providers — no real API calls.
"""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from app.services.llm import llm_client as llm_client_mod
from app.services.llm.llm_client import LLMClient, DEFAULT_TIMEOUT_SECONDS
from app.services.llm.types import (
    LLMRateLimitError, LLMProviderError, LLMTimeoutError,
    LLMTransientError, LLMJsonParseError, OutputMode,
    Provider, LLMResponse, TokenUsage, UseCase,
)
from conftest import (
    make_mock_provider,
    make_rate_limit_then_succeed_provider,
    make_transient_then_succeed_provider,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _HangProvider:
    """Provider whose complete() blocks forever — used to drive timeouts."""
    def __init__(self):
        self.call_count = 0

    @property
    def provider_name(self):
        return "anthropic"

    def supports_native_json(self):
        return False

    async def complete(self, request):
        self.call_count += 1
        await asyncio.Future()  # blocks until cancelled


def _build_success_response(req):
    return LLMResponse(
        content="ok",
        parsed=None,
        usage=TokenUsage(100, 50),
        model=req.model,
        provider=Provider.ANTHROPIC,
        latency_ms=100.0,
        was_retried=False,
        use_case=req.use_case,
    )


# ---------------------------------------------------------------------------
# Existing happy-path coverage
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Existing retry coverage — semantics of `max_retries` are unchanged
# (1 initial + max_retries retries). Confirms existing call sites still work.
# ---------------------------------------------------------------------------

class TestRetryLogic:

    async def test_retries_on_rate_limit_then_succeeds(self, rag_request, monkeypatch):
        monkeypatch.setattr(llm_client_mod, "_wait_strategy", lambda rs: 0.0)
        provider, call_count = make_rate_limit_then_succeed_provider(fail_times=1)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=3)
        result = await client.call(rag_request)
        assert call_count[0] == 2    # 1 failure + 1 success
        assert result.was_retried is True

    async def test_retries_up_to_max_retries(self, rag_request, monkeypatch):
        monkeypatch.setattr(llm_client_mod, "_wait_strategy", lambda rs: 0.0)
        provider, call_count = make_rate_limit_then_succeed_provider(fail_times=3)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=3)
        result = await client.call(rag_request)
        assert call_count[0] == 4   # 3 failures + 1 success
        assert result.was_retried is True

    async def test_raises_after_max_retries_exhausted(self, rag_request, monkeypatch):
        monkeypatch.setattr(llm_client_mod, "_wait_strategy", lambda rs: 0.0)
        provider, _ = make_rate_limit_then_succeed_provider(fail_times=10)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=2)
        with pytest.raises(LLMRateLimitError):
            await client.call(rag_request)

    async def test_call_count_matches_max_retries_plus_one(self, rag_request, monkeypatch):
        monkeypatch.setattr(llm_client_mod, "_wait_strategy", lambda rs: 0.0)
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


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------

class TestTimeout:

    async def test_timeout_raises_llm_timeout_error(self, rag_request):
        client = LLMClient(
            provider=_HangProvider(),
            timeout_seconds=0.05,
            max_retries=0,
        )
        with pytest.raises(LLMTimeoutError):
            await client.call(rag_request)

    async def test_fast_response_does_not_timeout(self, llm_client, rag_request):
        result = await llm_client.call(rag_request)
        assert result is not None


# ---------------------------------------------------------------------------
# Spec-mandated tests T1–T8 (Phase 2)
# ---------------------------------------------------------------------------

class TestRetrySpec:

    # --- T1 -----------------------------------------------------------------
    async def test_T1_two_rate_limits_then_success(self, rag_request, monkeypatch):
        monkeypatch.setattr(llm_client_mod, "_wait_strategy", lambda rs: 0.0)
        provider, call_count = make_rate_limit_then_succeed_provider(fail_times=2)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=2)
        result = await client.call(rag_request)
        assert call_count[0] == 3  # 1 + 2 retries
        assert result.was_retried is True

    # --- T2 -----------------------------------------------------------------
    async def test_T2_always_rate_limit_exhausts_and_reraises(self, rag_request, monkeypatch):
        monkeypatch.setattr(llm_client_mod, "_wait_strategy", lambda rs: 0.0)
        tracker = []
        provider = make_mock_provider(
            raises=LLMRateLimitError("perma 429"),
            call_count_tracker=tracker,
        )
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=2)
        with pytest.raises(LLMRateLimitError):
            await client.call(rag_request)
        assert len(tracker) == 3  # 1 + 2 retries

    # --- T3 -----------------------------------------------------------------
    async def test_T3_bad_request_no_retry(self, rag_request):
        tracker = []
        provider = make_mock_provider(
            raises=LLMProviderError("400 bad request"),
            call_count_tracker=tracker,
        )
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=2)
        with pytest.raises(LLMProviderError):
            await client.call(rag_request)
        assert len(tracker) == 1

    # --- T4 -----------------------------------------------------------------
    async def test_T4_transient_error_then_success(self, rag_request, monkeypatch):
        monkeypatch.setattr(llm_client_mod, "_wait_strategy", lambda rs: 0.0)
        provider, call_count = make_transient_then_succeed_provider(fail_times=1)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=2)
        result = await client.call(rag_request)
        assert call_count[0] == 2
        assert result.was_retried is True

    # --- T5 -----------------------------------------------------------------
    async def test_T5_retry_after_header_overrides_backoff(self, rag_request, monkeypatch):
        sleeps: list[float] = []

        original_sleep = asyncio.sleep

        async def recording_sleep(d):
            sleeps.append(d)
            # Yield control once so the event loop stays healthy without real delay.
            await original_sleep(0)

        monkeypatch.setattr(asyncio, "sleep", recording_sleep)

        call_count = [0]

        class P:
            @property
            def provider_name(self): return "anthropic"
            def supports_native_json(self): return False
            async def complete(self, req):
                call_count[0] += 1
                if call_count[0] <= 2:
                    raise LLMRateLimitError("429", retry_after_seconds=2.5)
                return _build_success_response(req)

        client = LLMClient(provider=P(), timeout_seconds=5.0, max_retries=3)
        await client.call(rag_request)

        assert call_count[0] == 3
        # Two between-attempt sleeps, both honoring Retry-After=2.5s
        retry_sleeps = [s for s in sleeps if s > 0]
        assert len(retry_sleeps) == 2
        assert all(abs(s - 2.5) < 0.01 for s in retry_sleeps), retry_sleeps

    # --- T6 -----------------------------------------------------------------
    async def test_T6_exponential_backoff_without_retry_after(self, rag_request, monkeypatch):
        sleeps: list[float] = []

        original_sleep = asyncio.sleep

        async def recording_sleep(d):
            sleeps.append(d)
            await original_sleep(0)

        monkeypatch.setattr(asyncio, "sleep", recording_sleep)

        provider, _ = make_rate_limit_then_succeed_provider(fail_times=2)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=3)
        await client.call(rag_request)

        retry_sleeps = [s for s in sleeps if s > 0]
        assert len(retry_sleeps) == 2
        # All sleeps must be capped at 30s and non-negative.
        for s in retry_sleeps:
            assert 0 <= s <= 30

    # --- T7 -----------------------------------------------------------------
    async def test_T7_log_usage_called_once_on_success(self, llm_client, rag_request):
        with patch.object(llm_client, "_log_usage", wraps=llm_client._log_usage) as spy:
            result = await llm_client.call(rag_request)
        spy.assert_called_once()
        request_arg, response_arg = spy.call_args.args
        assert request_arg.business_id == "salon_123"
        assert response_arg.usage.input_tokens > 0
        assert response_arg.usage.output_tokens > 0
        assert result is response_arg

    # --- T8 -----------------------------------------------------------------
    async def test_T8_per_attempt_timeout_each_attempt_fresh_budget(
        self, rag_request, monkeypatch,
    ):
        # Skip retry backoff so test runs fast.
        monkeypatch.setattr(llm_client_mod, "_wait_strategy", lambda rs: 0.0)

        provider = _HangProvider()
        client = LLMClient(provider=provider, timeout_seconds=0.05, max_retries=2)

        with pytest.raises(LLMTimeoutError):
            await client.call(rag_request)

        # If the timeout were total (not per-attempt) we'd see 1 call.
        # Per-attempt → each retry gets a fresh budget → 1 + 2 retries = 3 calls.
        assert provider.call_count == 3


# ---------------------------------------------------------------------------
# was_retried flag
# ---------------------------------------------------------------------------

class TestWasRetried:

    async def test_was_retried_true_after_one_rate_limit(self, rag_request, monkeypatch):
        monkeypatch.setattr(llm_client_mod, "_wait_strategy", lambda rs: 0.0)
        provider, _ = make_rate_limit_then_succeed_provider(fail_times=1)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=3)
        result = await client.call(rag_request)
        assert result.was_retried is True

    async def test_was_retried_false_on_clean_success(self, llm_client, rag_request):
        result = await llm_client.call(rag_request)
        assert result.was_retried is False
