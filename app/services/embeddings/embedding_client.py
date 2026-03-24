# app/services/embeddings/embedding_client.py
"""
EmbeddingClient
===============
Single entry point for all embedding calls.

Responsibilities:
  - Auto-chunk batches that exceed provider max_batch_size
  - Retry on rate limit (exponential backoff, max 3 retries)
  - Hard timeout via asyncio.wait_for
  - Expose two clean methods: embed() and embed_batch()

Usage
-----
    client = EmbeddingClient.from_env()

    # Single text (retriever — embed the user question)
    vector = await client.embed("Why did my revenue drop?")

    # Many texts (ETL — embed monthly summaries)
    vectors = await client.embed_batch(["Jan summary...", "Feb summary..."])
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

from .types import (
    EMBEDDING_CONFIGS,
    EmbeddingConfig,
    EmbeddingProvider,
    EmbeddingRateLimitError,
    EmbeddingTimeoutError,
    EmbeddingProviderError,
)

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SECONDS = 10.0
_DEFAULT_MAX_RETRIES     = 3
_BACKOFF_SECONDS         = [1.0, 2.0, 4.0]


class EmbeddingClient:
    """
    Provider-agnostic embedding client.

    Parameters
    ----------
    provider:        Configured provider instance (Voyage or OpenAI).
    config:          EmbeddingConfig — holds model name and dimension count.
    timeout_seconds: Hard timeout per provider call.
    max_retries:     Max retries on rate limit (429 only).
    """

    def __init__(
        self,
        provider,
        config:          EmbeddingConfig,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        max_retries:     int   = _DEFAULT_MAX_RETRIES,
    ) -> None:
        self._provider        = provider
        self._config          = config
        self._timeout         = timeout_seconds
        self._max_retries     = max_retries

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def dimensions(self) -> int:
        """Dimension count of produced vectors — needed by pgvector schema."""
        return self._config.dimensions

    @property
    def provider_name(self) -> str:
        return self._config.provider.value

    async def embed(self, text: str) -> list[float]:
        """
        Embed a single text string.
        Used by the retriever to embed the user question at query time.
        """
        results = await self.embed_batch([text])
        return results[0]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """
        Embed a list of texts.
        Used by the ETL job to embed many summaries in one call.
        Automatically chunks into provider-safe batch sizes.
        """
        if not texts:
            return []

        chunks  = self._chunk(texts)
        results = []

        for chunk in chunks:
            vectors = await self._call_with_retry(chunk)
            results.extend(vectors)

        return results

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _chunk(self, texts: list[str]) -> list[list[str]]:
        """Split texts into provider-safe batch sizes."""
        size   = self._provider.max_batch_size
        return [texts[i:i + size] for i in range(0, len(texts), size)]

    async def _call_with_retry(self, texts: list[str]) -> list[list[float]]:
        last_exc: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                return await asyncio.wait_for(
                    self._provider.embed(texts),
                    timeout=self._timeout,
                )
            except asyncio.TimeoutError:
                raise EmbeddingTimeoutError(
                    f"Embedding timed out after {self._timeout}s "
                    f"(provider={self.provider_name})"
                )
            except EmbeddingRateLimitError as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    wait = _BACKOFF_SECONDS[min(attempt, len(_BACKOFF_SECONDS) - 1)]
                    logger.warning(
                        "embedding.rate_limit attempt=%d/%d — retrying in %.1fs",
                        attempt + 1, self._max_retries, wait,
                    )
                    await asyncio.sleep(wait)
            except EmbeddingProviderError:
                raise

        raise EmbeddingRateLimitError(
            f"Rate limit retries exhausted after {self._max_retries} attempts"
        ) from last_exc

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(
        cls,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        max_retries:     int   = _DEFAULT_MAX_RETRIES,
    ) -> "EmbeddingClient":
        """
        Build from environment variables.

        Environment variables
        ---------------------
        EMBEDDING_PROVIDER : "voyage" (default) | "openai"
        VOYAGE_API_KEY     : required when provider=voyage
        OPENAI_API_KEY     : required when provider=openai
        EMBEDDING_TIMEOUT  : float seconds (default 10.0)
        EMBEDDING_MAX_RETRIES : int (default 3)
        """
        provider_name = os.getenv("EMBEDDING_PROVIDER", "voyage").lower()

        try:
            provider_enum = EmbeddingProvider(provider_name)
        except ValueError:
            raise ValueError(
                f"Unknown EMBEDDING_PROVIDER={provider_name!r}. "
                f"Valid values: {[p.value for p in EmbeddingProvider]}"
            )

        config  = EMBEDDING_CONFIGS[provider_enum]
        timeout = float(os.getenv("EMBEDDING_TIMEOUT",      str(timeout_seconds)))
        retries = int(os.getenv("EMBEDDING_MAX_RETRIES", str(max_retries)))

        if provider_enum == EmbeddingProvider.VOYAGE:
            from .voyage_provider import VoyageEmbeddingProvider
            raw_provider = VoyageEmbeddingProvider()
        else:
            from .openai_provider import OpenAIEmbeddingProvider
            raw_provider = OpenAIEmbeddingProvider()

        return cls(raw_provider, config, timeout, retries)