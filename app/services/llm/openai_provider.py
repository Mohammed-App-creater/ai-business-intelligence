"""
openai_provider.py
==================
OpenAI GPT provider implementation.

Differences from Anthropic handled here (not in the client):
  - System prompt is the first message with role="system"
  - Native JSON via response_format={"type": "json_object"} (supports_native_json=True)
  - Token fields are `prompt_tokens` / `completion_tokens` inside `usage`
  - Rate limit = HTTP 429 with RateLimitError from SDK
"""
from __future__ import annotations

import logging
import time

import openai

from .base_provider import BaseLLMProvider  # noqa: F401
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


class OpenAIProvider:
    """
    OpenAI GPT provider.

    api_key is read from OPENAI_API_KEY env var by the SDK automatically.
    Pass it explicitly only in tests.
    """

    def __init__(self, api_key: str | None = None) -> None:
        self._client = openai.AsyncOpenAI(
            **({"api_key": api_key} if api_key else {})
        )

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    @property
    def provider_name(self) -> str:
        return Provider.OPENAI.value

    def supports_native_json(self) -> bool:
        # OpenAI supports response_format={"type": "json_object"} natively.
        return True

    async def complete(self, request: LLMRequest) -> LLMResponse:
        t0 = time.perf_counter()

        system = request.system
        user = request.user

        # OpenAI requires the word "json" to appear in messages when using
        # response_format=json_object (e.g. DIRECT chat uses plain prompts).
        if request.output_mode == OutputMode.STRUCTURED_JSON:
            combined = (system or "") + (user or "")
            if "json" not in combined.lower():
                system = (system or "").rstrip() + (
                    "\n\nRespond with a valid JSON object."
                )

        messages = [
            {"role": "system",  "content": system},
            {"role": "user",    "content": user},
        ]

        kwargs: dict = {
            "model":       request.model,
            "max_tokens":  request.max_tokens,
            "messages":    messages,
            "temperature": request.temperature,
        }

        # Native JSON enforcement for structured output use cases
        if request.output_mode == OutputMode.STRUCTURED_JSON:
            kwargs["response_format"] = {"type": "json_object"}

        try:
            response = await self._client.chat.completions.create(**kwargs)
        except openai.RateLimitError as exc:
            raise LLMRateLimitError(f"OpenAI rate limit: {exc}") from exc
        except openai.APIStatusError as exc:
            raise LLMProviderError(
                f"OpenAI API error {exc.status_code}: {exc.message}"
            ) from exc
        except openai.APIConnectionError as exc:
            raise LLMProviderError(f"OpenAI connection error: {exc}") from exc

        latency_ms = (time.perf_counter() - t0) * 1000
        content    = response.choices[0].message.content or ""
        usage      = TokenUsage(
            input_tokens=response.usage.prompt_tokens,
            output_tokens=response.usage.completion_tokens,
        )

        parsed = None
        if request.output_mode == OutputMode.STRUCTURED_JSON:
            parsed = self._parse_json(content)

        return LLMResponse(
            content=content,
            parsed=parsed,
            usage=usage,
            model=response.model,
            provider=Provider.OPENAI,
            latency_ms=latency_ms,
            was_retried=False,
            use_case=request.use_case,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_json(content: str) -> dict:
        """
        Parse JSON from OpenAI response.

        With response_format=json_object this should always succeed,
        but we validate defensively.
        """
        import json

        try:
            result = json.loads(content)
            if not isinstance(result, dict):
                raise LLMJsonParseError(
                    f"Expected JSON object, got {type(result).__name__}",
                    raw_content=content,
                )
            return result
        except json.JSONDecodeError as exc:
            raise LLMJsonParseError(
                f"Invalid JSON from OpenAI: {exc}",
                raw_content=content,
            ) from exc