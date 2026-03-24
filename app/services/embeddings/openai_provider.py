# app/services/embeddings/openai_provider.py
from __future__ import annotations

import logging
import os

from .types import EmbeddingRateLimitError, EmbeddingProviderError

logger = logging.getLogger(__name__)

_MODEL      = "text-embedding-3-small"
_DIMENSIONS = 1536


class OpenAIEmbeddingProvider:

    def __init__(self, api_key: str | None = None) -> None:
        import openai
        self._client = openai.AsyncOpenAI(
            api_key=api_key or os.environ["OPENAI_API_KEY"]
        )

    @property
    def provider_name(self) -> str:
        return "openai"

    @property
    def max_batch_size(self) -> int:
        return 2048   # OpenAI limit per request

    async def embed(self, texts: list[str]) -> list[list[float]]:
        try:
            response = await self._client.embeddings.create(
                input=texts,
                model=_MODEL,
            )
            # OpenAI returns objects sorted by index
            return [item.embedding for item in response.data]
        except Exception as exc:
            msg = str(exc).lower()
            if "429" in msg or "rate" in msg:
                raise EmbeddingRateLimitError(str(exc)) from exc
            raise EmbeddingProviderError(str(exc)) from exc