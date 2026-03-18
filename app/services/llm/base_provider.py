"""
base_provider.py
================
Protocol (interface) that every LLM provider must implement.

The LLMClient depends only on this protocol — it never imports
AnthropicProvider or OpenAIProvider directly. This keeps the
provider implementations fully swappable.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from .types import LLMRequest, LLMResponse


@runtime_checkable
class BaseLLMProvider(Protocol):
    """
    Contract every provider must satisfy.

    Providers are responsible for:
      - Making the HTTP call to their specific API
      - Mapping their response format to LLMResponse
      - Extracting token usage into TokenUsage
      - Raising LLMRateLimitError on 429 so the client can retry
      - Raising LLMProviderError on all other non-retryable failures
      - Enforcing native structured JSON output if supported

    Providers are NOT responsible for:
      - Retries (handled by LLMClient)
      - Timeouts (handled by LLMClient)
      - Token logging (handled by LLMClient)
      - Model / output-mode resolution (handled by LLMGateway)
    """

    async def complete(self, request: LLMRequest) -> LLMResponse:
        """
        Execute a single completion call.

        Must NOT implement retry logic — raise immediately on error
        so LLMClient can decide whether to retry.
        """
        ...

    def supports_native_json(self) -> bool:
        """
        Returns True if the provider natively enforces JSON output
        (e.g. OpenAI response_format), False if JSON is prompt-enforced
        (e.g. Anthropic).

        LLMClient uses this to decide whether to add a JSON validation
        step after the response.
        """
        ...

    @property
    def provider_name(self) -> str:
        """Human-readable provider identifier for logging."""
        ...