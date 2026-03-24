# app/services/embeddings/voyage_provider.py
from __future__ import annotations

import logging
import os

from .types import EmbeddingRateLimitError, EmbeddingProviderError

logger = logging.getLogger(__name__)

_MODEL   = "voyage-3"
_DIMENSIONS = 1024


class VoyageEmbeddingProvider:

    def __init__(self, api_key: str | None = None) -> None:
        import voyageai
        self._client = voyageai.AsyncClient(
            api_key=api_key or os.environ["VOYAGE_API_KEY"]
        )

    @property
    def provider_name(self) -> str:
        return "voyage"

    @property
    def max_batch_size(self) -> int:
        return 128   # Voyage limit per request

    async def embed(self, texts: list[str]) -> list[list[float]]:
        try:
            result = await self._client.embed(
                texts,
                model=_MODEL,
                input_type="document",   # ETL docs + query both use "document"
            )
            return result.embeddings
        except Exception as exc:
            msg = str(exc).lower()
            if "429" in msg or "rate" in msg:
                raise EmbeddingRateLimitError(str(exc)) from exc
            raise EmbeddingProviderError(str(exc)) from exc