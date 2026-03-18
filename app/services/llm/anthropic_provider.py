"""
anthropic_provider.py
=====================
Anthropic Claude provider implementation.

Differences from OpenAI handled here (not in the client):
  - System prompt is a separate `system` param, not a messages entry
  - No native response_format — JSON enforced via prompt (supports_native_json=False)
  - Token fields are `input_tokens` / `output_tokens`
  - Rate limit = HTTP 429 with RateLimitError from SDK
"""
from __future__ import annotations

import logging
import time

import anthropic

from .base_provider import BaseLLMProvider  # noqa: F401 (satisfies Protocol)
from .types import (
    LLMJsonParseError,
    LLMProviderError,
    LLMRateLimitError,
    LLMRequest,
    LLMResponse,
    OutputMode,
    Provider,
    TokenUsage,
)

logger = logging.getLogger(__name__)


class AnthropicProvider:
    """
    Anthropic Claude provider.

    Initialisation
    --------------
    api_key is read from the ANTHROPIC_API_KEY environment variable by
    the Anthropic SDK automatically. Pass it explicitly only in tests.
    """

    def __init__(self, api_key: str | None = None) -> None:
        self._client = anthropic.AsyncAnthropic(
            **({"api_key": api_key} if api_key else {})
        )

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    @property
    def provider_name(self) -> str:
        return Provider.ANTHROPIC.value

    def supports_native_json(self) -> bool:
        # Anthropic does not have a response_format parameter —
        # JSON output is enforced via the system prompt.
        return False

    async def complete(self, request: LLMRequest) -> LLMResponse:
        t0 = time.perf_counter()

        kwargs: dict = {
            "model":      request.model,
            "max_tokens": request.max_tokens,
            "system":     request.system,
            "messages":   [{"role": "user", "content": request.user}],
            "temperature": request.temperature,
        }

        try:
            response = await self._client.messages.create(**kwargs)
        except anthropic.RateLimitError as exc:
            raise LLMRateLimitError(f"Anthropic rate limit: {exc}") from exc
        except anthropic.APIStatusError as exc:
            raise LLMProviderError(
                f"Anthropic API error {exc.status_code}: {exc.message}"
            ) from exc
        except anthropic.APIConnectionError as exc:
            raise LLMProviderError(f"Anthropic connection error: {exc}") from exc

        latency_ms = (time.perf_counter() - t0) * 1000
        content    = response.content[0].text
        usage      = TokenUsage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        )

        parsed = None
        if request.output_mode == OutputMode.STRUCTURED_JSON:
            parsed = self._parse_json(content)

        return LLMResponse(
            content=content,
            parsed=parsed,
            usage=usage,
            model=response.model,
            provider=Provider.ANTHROPIC,
            latency_ms=latency_ms,
            was_retried=False,   # set by LLMClient if applicable
            use_case=request.use_case,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_json(content: str) -> dict:
        """
        Parse JSON from Anthropic response content.

        Anthropic models sometimes wrap JSON in markdown fences even
        when instructed not to. We strip those defensively before parsing.
        """
        import json
        import re

        cleaned = content.strip()
        # Strip ```json ... ``` or ``` ... ``` fences if present
        fence_match = re.search(r"```(?:json)?\s*([\s\S]+?)```", cleaned)
        if fence_match:
            cleaned = fence_match.group(1).strip()

        try:
            result = json.loads(cleaned)
            if not isinstance(result, dict):
                raise LLMJsonParseError(
                    f"Expected JSON object, got {type(result).__name__}",
                    raw_content=content,
                )
            return result
        except json.JSONDecodeError as exc:
            raise LLMJsonParseError(
                f"Invalid JSON from Anthropic: {exc}",
                raw_content=content,
            ) from exc