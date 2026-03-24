# app/services/embeddings/base_provider.py
from __future__ import annotations
from typing import Protocol


class BaseEmbeddingProvider(Protocol):

    @property
    def provider_name(self) -> str: ...

    @property
    def max_batch_size(self) -> int: ...

    async def embed(self, texts: list[str]) -> list[list[float]]:
        """
        Embed a batch of texts. Returns one vector per text.
        Raises EmbeddingRateLimitError, EmbeddingProviderError, or EmbeddingTimeoutError.
        """
        ...