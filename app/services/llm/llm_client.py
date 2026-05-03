"""
llm_client.py
=============
The single HTTP wrapper for all LLM calls.

Responsibilities:
  - Per-attempt hard timeout via asyncio.wait_for (each retry gets a fresh budget)
  - tenacity-driven retry with exponential backoff + jitter on retryable errors
  - Honors Retry-After header from the provider when present (capped at 30s)
  - Structured token usage logging per business_id and model on every success
  - Sets was_retried=True on the response if any retry occurred
  - Provider-agnostic — works with any BaseLLMProvider implementation

NOT responsible for:
  - Model selection          (gateway)
  - Output mode selection    (gateway)
  - Prompt construction      (prompts/)
  - Quota enforcement        (gateway)

Retry policy (AI_BI_Architecture_v1.1 §5.7):
  - Total attempts = 1 initial + max_retries (default: 1 + 2 = 3)
  - Retry only on LLMRetryableError subclasses (rate limit / transient / timeout).
    LLMProviderError (BadRequest, Auth, etc.) propagates immediately — these are
    bugs in our request and retrying just hides the real problem.
  - Wait: Retry-After header (capped 30s) when present, otherwise
    wait_random_exponential(multiplier=1, max=30).
  - SDK-level retries are disabled at the provider layer (max_retries=0) so this
    is the single source of truth for retry behaviour.
"""
from __future__ import annotations

import asyncio
import logging

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from .base_provider import BaseLLMProvider
from .types import (
    LLMRequest,
    LLMResponse,
    LLMRetryableError,
    LLMTimeoutError,
)

logger = logging.getLogger(__name__)

# Default hard timeout in seconds — below P95 latency target of 8s,
# leaving headroom for the rest of the pipeline (Section 8.3).
# TODO(AI_BI_Architecture_v1.1 §5.7): spec mandates 30s; deferred.
DEFAULT_TIMEOUT_SECONDS = 7.0

# Retry config — applies to any LLMRetryableError. Total attempts = 1 + MAX_RETRIES.
MAX_RETRIES = 2

# Single-sleep cap — protects against runaway Retry-After values from misbehaving
# providers and bounds the exponential backoff envelope.
_MAX_SLEEP_SECONDS = 30.0

# Module-level wait strategy. Exposed so tests can monkeypatch to bypass real waits.
_DEFAULT_WAIT = wait_random_exponential(multiplier=1, max=_MAX_SLEEP_SECONDS)


def _wait_strategy(retry_state) -> float:
    """
    tenacity wait callable.

    If the most recent exception carries a retry_after_seconds (extracted by the
    provider from the Retry-After header), honor it — capped at _MAX_SLEEP_SECONDS.
    Otherwise fall back to exponential backoff with jitter.
    """
    outcome = retry_state.outcome
    exc = outcome.exception() if outcome is not None else None
    if isinstance(exc, LLMRetryableError) and exc.retry_after_seconds is not None:
        return min(max(exc.retry_after_seconds, 0.0), _MAX_SLEEP_SECONDS)
    return _DEFAULT_WAIT(retry_state)


class LLMClient:
    """
    Provider-agnostic LLM call executor.

    Usage
    -----
        client = LLMClient(provider=AnthropicProvider())
        response = await client.call(request)

    Parameters
    ----------
    provider:
        Any object satisfying BaseLLMProvider protocol.
    timeout_seconds:
        Per-attempt hard timeout. Each retry gets a fresh budget.
        Defaults to DEFAULT_TIMEOUT_SECONDS.
    max_retries:
        Retries after the initial attempt. Total attempts = 1 + max_retries.
        Defaults to MAX_RETRIES.
    """

    def __init__(
        self,
        provider: BaseLLMProvider,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self._provider = provider
        self._timeout  = timeout_seconds
        self._max_retries = max_retries

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def call(self, request: LLMRequest) -> LLMResponse:
        """
        Execute the LLM call with retry and per-attempt timeout.

        Raises
        ------
        LLMTimeoutError    if every attempt exceeds timeout_seconds
        LLMRateLimitError  if rate-limit retries are exhausted
        LLMTransientError  if transient retries are exhausted
        LLMProviderError   on non-retryable provider errors (no retry)
        LLMJsonParseError  if STRUCTURED_JSON output cannot be parsed (no retry)
        """
        total_attempts = 1 + self._max_retries
        response: LLMResponse | None = None
        final_attempt_number = 1

        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(total_attempts),
                retry=retry_if_exception_type(LLMRetryableError),
                wait=_wait_strategy,
                before_sleep=self._make_before_sleep(request, total_attempts),
                reraise=True,
            ):
                with attempt:
                    response = await self._execute_with_timeout(request)
                final_attempt_number = attempt.retry_state.attempt_number
        except LLMRetryableError:
            logger.error(
                "llm_client.exhausted business_id=%s provider=%s model=%s attempts=%d",
                request.business_id, self._provider.provider_name,
                request.model, total_attempts,
            )
            raise

        # AsyncRetrying with reraise=True guarantees response is set on success.
        assert response is not None
        if final_attempt_number > 1:
            response.was_retried = True
        self._log_usage(request, response)
        return response

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _execute_with_timeout(self, request: LLMRequest) -> LLMResponse:
        try:
            return await asyncio.wait_for(
                self._provider.complete(request),
                timeout=self._timeout,
            )
        except asyncio.TimeoutError as exc:
            # Translate to a retryable type so tenacity sees it.
            raise LLMTimeoutError(
                f"LLM call timed out after {self._timeout}s "
                f"(model={request.model}, use_case={request.use_case})"
            ) from exc

    def _make_before_sleep(self, request: LLMRequest, total_attempts: int):
        """Build a tenacity before_sleep callback bound to this request's context."""
        provider_name = self._provider.provider_name

        def _before_sleep(retry_state) -> None:
            outcome = retry_state.outcome
            exc = outcome.exception() if outcome is not None else None
            next_action = retry_state.next_action
            sleep_s = next_action.sleep if next_action is not None else 0.0
            logger.warning(
                "llm_client.retry business_id=%s provider=%s model=%s "
                "attempt=%d/%d exception=%s next_sleep=%.2fs",
                request.business_id, provider_name, request.model,
                retry_state.attempt_number, total_attempts,
                type(exc).__name__ if exc else "None", sleep_s,
            )

        return _before_sleep

    def _log_usage(self, request: LLMRequest, response: LLMResponse) -> None:
        logger.info(
            "llm_client.usage business_id=%s use_case=%s provider=%s model=%s "
            "input_tokens=%d output_tokens=%d total_tokens=%d latency_ms=%.1f retried=%s",
            request.business_id,
            request.use_case.value,
            response.provider.value,
            response.model,
            response.usage.input_tokens,
            response.usage.output_tokens,
            response.usage.total,
            response.latency_ms,
            response.was_retried,
        )
