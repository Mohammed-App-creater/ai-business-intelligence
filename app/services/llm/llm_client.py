"""
llm_client.py
=============
The single HTTP wrapper for all LLM calls.

Responsibilities:
  - Hard async timeout via asyncio.wait_for
  - Exponential backoff retry on 429 rate-limit errors (max 3 retries)
  - Structured token usage logging per business_id and model
  - Sets was_retried=True on the response if any retry occurred
  - Provider-agnostic — works with any BaseLLMProvider implementation

NOT responsible for:
  - Model selection          (gateway)
  - Output mode selection    (gateway)
  - Prompt construction      (prompts/)
  - Quota enforcement        (gateway)
"""
from __future__ import annotations

import asyncio
import logging


from .base_provider import BaseLLMProvider
from .types import (
    LLMError,
    LLMRateLimitError,
    LLMRequest,
    LLMResponse,
    LLMTimeoutError,
)

logger = logging.getLogger(__name__)

# Default hard timeout in seconds — below P95 latency target of 8s,
# leaving headroom for the rest of the pipeline (Section 8.3).
DEFAULT_TIMEOUT_SECONDS = 7.0

# Retry config — only for 429 rate-limit errors
MAX_RETRIES      = 3
BACKOFF_BASE_SEC = 1.0   # 1s → 2s → 4s


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
        Hard timeout per call attempt. Defaults to DEFAULT_TIMEOUT_SECONDS.
    max_retries:
        Maximum retry attempts on rate-limit errors. Defaults to MAX_RETRIES.
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
        Execute the LLM call with retry and timeout handling.

        Raises
        ------
        LLMTimeoutError    if the call exceeds timeout_seconds
        LLMRateLimitError  if rate-limit retries are exhausted
        LLMProviderError   on non-retryable provider errors
        LLMJsonParseError  if STRUCTURED_JSON output cannot be parsed
        """
        last_exc: LLMError | None = None
        retried = False

        for attempt in range(self._max_retries + 1):
            try:
                response = await self._execute_with_timeout(request)

                # Tag the response if we had to retry to get here
                if retried:
                    response.was_retried = True

                self._log_usage(request, response)
                return response

            except LLMRateLimitError as exc:
                last_exc = exc
                if attempt >= self._max_retries:
                    break
                backoff = BACKOFF_BASE_SEC * (2 ** attempt)
                logger.warning(
                    "llm_client.rate_limit business_id=%s attempt=%d/%d "
                    "backoff=%.1fs model=%s",
                    request.business_id, attempt + 1, self._max_retries,
                    backoff, request.model,
                )
                await asyncio.sleep(backoff)
                retried = True

            except LLMTimeoutError:
                # Timeouts are not retried — surface immediately
                logger.error(
                    "llm_client.timeout business_id=%s model=%s timeout=%.1fs",
                    request.business_id, request.model, self._timeout,
                )
                raise

            # All other errors (LLMProviderError, LLMJsonParseError) propagate immediately
        raise last_exc  # type: ignore[misc]

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
            raise LLMTimeoutError(
                f"LLM call timed out after {self._timeout}s "
                f"(model={request.model}, use_case={request.use_case})"
            ) from exc

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