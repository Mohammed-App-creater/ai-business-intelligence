"""
llm_gateway.py
==============
The single entry point for ALL LLM calls in the application.

Responsibilities:
  - Resolve the correct model for each (provider, use_case) pair
  - Resolve the correct output mode for each use_case
  - Enforce per-tenant daily token quotas
  - Build LLMRequest and delegate to LLMClient
  - Return a clean LLMResponse to call sites

Call sites (query_analyzer, RAG pipeline, ETL doc generation) only
import and use LLMGateway — they never touch LLMClient or providers directly.

Usage
-----
    gateway = LLMGateway.from_env()   # reads LLM_PROVIDER env var

    response = await gateway.call(
        use_case=UseCase.RAG_CHAT,
        system=system_prompt,
        user=user_prompt,
        business_id="salon_123",
    )
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from .llm_client import LLMClient
from .types import (
    LLMRequest,
    LLMResponse,
    OutputMode,
    Provider,
    UseCase,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Model map — one place to change model assignments
# ---------------------------------------------------------------------------

MODEL_MAP: dict[Provider, dict[UseCase, str]] = {
    Provider.ANTHROPIC: {
        UseCase.CLASSIFIER:     "claude-haiku-4-5-20251001",
        UseCase.RAG_CHAT:       "claude-sonnet-4-6",
        UseCase.DOC_GENERATION: "claude-haiku-4-5-20251001",
        UseCase.AGENT:          "claude-sonnet-4-6",
    },
    Provider.OPENAI: {
        UseCase.CLASSIFIER:     "gpt-4.1-mini",
        UseCase.RAG_CHAT:       "gpt-4.1",
        UseCase.DOC_GENERATION: "gpt-4.1-mini",
        UseCase.AGENT:          "gpt-4.1",
    },
}

# ---------------------------------------------------------------------------
# Output mode map — how each use case expects the response
# ---------------------------------------------------------------------------

OUTPUT_MODE_MAP: dict[UseCase, OutputMode] = {
    UseCase.CLASSIFIER:     OutputMode.RAW,
    UseCase.RAG_CHAT:       OutputMode.STRUCTURED_JSON,
    UseCase.DOC_GENERATION: OutputMode.RAW,
    UseCase.AGENT:          OutputMode.TOOL_CALLS,
}

# ---------------------------------------------------------------------------
# Token budget map — max tokens per response per use case
# ---------------------------------------------------------------------------

MAX_TOKENS_MAP: dict[UseCase, int] = {
    UseCase.CLASSIFIER:     256,    # Short classification response
    UseCase.RAG_CHAT:       1_000,  # Full structured JSON answer
    UseCase.DOC_GENERATION: 512,    # Narrative summary paragraph
    UseCase.AGENT:          2_000,  # Multi-step reasoning (V2)
}

# ---------------------------------------------------------------------------
# Per-tenant daily token quota (total tokens across all use cases)
# Set to None to disable quota enforcement (e.g. for ETL jobs)
# ---------------------------------------------------------------------------

DEFAULT_DAILY_TOKEN_QUOTA: Optional[int] = 50_000


# ---------------------------------------------------------------------------
# Gateway
# ---------------------------------------------------------------------------

class LLMGateway:
    """
    Routing layer between call sites and the LLM client.

    Parameters
    ----------
    client:
        Configured LLMClient instance.
    provider:
        Active provider enum — used to resolve model names from MODEL_MAP.
    daily_token_quota:
        Per-tenant daily token limit. Pass None to disable.
    quota_store:
        Optional async callable (business_id) -> int returning tokens used today.
        If None, quota enforcement is skipped even when daily_token_quota is set.
        In production, wire this to a Redis counter.
    """

    def __init__(
        self,
        client: LLMClient,
        provider: Provider,
        daily_token_quota: Optional[int] = DEFAULT_DAILY_TOKEN_QUOTA,
        quota_store=None,
    ) -> None:
        self._client = client
        self._provider = provider
        self._daily_token_quota = daily_token_quota
        self._quota_store = quota_store

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(
        cls,
        daily_token_quota: Optional[int] = DEFAULT_DAILY_TOKEN_QUOTA,
        quota_store=None,
    ) -> "LLMGateway":
        """
        Build a fully configured gateway from environment variables.

        Environment variables
        ---------------------
        LLM_PROVIDER       : "anthropic" (default) | "openai"
        ANTHROPIC_API_KEY  : required when provider=anthropic
        OPENAI_API_KEY     : required when provider=openai
        LLM_TIMEOUT        : float seconds (default 7.0)
        LLM_MAX_RETRIES    : int (default 3)
        """
        provider_name = os.getenv("LLM_PROVIDER", "anthropic").lower()

        try:
            provider = Provider(provider_name)
        except ValueError:
            raise ValueError(
                f"Unknown LLM_PROVIDER={provider_name!r}. "
                f"Valid values: {[p.value for p in Provider]}"
            )

        timeout     = float(os.getenv("LLM_TIMEOUT", "7.0"))
        max_retries = int(os.getenv("LLM_MAX_RETRIES", "3"))

        if provider == Provider.ANTHROPIC:
            from .anthropic_provider import AnthropicProvider
            raw_provider = AnthropicProvider()
        else:
            from .openai_provider import OpenAIProvider
            raw_provider = OpenAIProvider()

        client = LLMClient(
            provider=raw_provider,
            timeout_seconds=timeout,
            max_retries=max_retries,
        )
        return cls(client, provider, daily_token_quota, quota_store)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def call(
        self,
        use_case: UseCase,
        system: str,
        user: str,
        business_id: str,
        max_tokens: Optional[int] = None,
        temperature: float = 0.2,
    ) -> LLMResponse:
        """
        Execute an LLM call for the given use case.

        Parameters
        ----------
        use_case:     Determines model, output mode, and token budget.
        system:       System prompt string.
        user:         User turn string.
        business_id:  Tenant identifier for logging and quota.
        max_tokens:   Override the default for this use case (optional).
        temperature:  Override the default temperature (optional).

        Raises
        ------
        LLMQuotaExceededError  if tenant has exceeded their daily quota.
        LLMTimeoutError        if the call exceeds the timeout.
        LLMRateLimitError      if rate-limit retries are exhausted.
        LLMProviderError       on non-retryable provider errors.
        LLMJsonParseError      if structured JSON cannot be parsed.
        """
        await self._check_quota(business_id)

        model       = MODEL_MAP[self._provider][use_case]
        output_mode = OUTPUT_MODE_MAP[use_case]
        tokens      = max_tokens or MAX_TOKENS_MAP[use_case]

        request = LLMRequest(
            use_case=use_case,
            system=system,
            user=user,
            business_id=business_id,
            model=model,
            output_mode=output_mode,
            max_tokens=tokens,
            temperature=temperature,
        )

        logger.debug(
            "llm_gateway.call business_id=%s use_case=%s provider=%s model=%s",
            business_id, use_case.value, self._provider.value, model,
        )

        return await self._client.call(request)

    # ------------------------------------------------------------------
    # Quota enforcement
    # ------------------------------------------------------------------

    async def _check_quota(self, business_id: str) -> None:
        if self._daily_token_quota is None or self._quota_store is None:
            return

        try:
            used_today: int = await self._quota_store(business_id)
        except Exception as exc:  # noqa: BLE001
            # Quota store failure must never block the request
            logger.warning(
                "llm_gateway.quota_store_error business_id=%s error=%r — skipping quota check",
                business_id, exc,
            )
            return

        if used_today >= self._daily_token_quota:
            from .types import LLMError
            raise LLMQuotaExceededError(
                f"Tenant {business_id!r} has exceeded daily token quota "
                f"({used_today}/{self._daily_token_quota})"
            )


class LLMQuotaExceededError(Exception):
    """Tenant has exceeded their daily token quota."""