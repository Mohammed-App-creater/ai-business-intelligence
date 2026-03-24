# app/services/embeddings/types.py
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass


class EmbeddingProvider(str, Enum):
    VOYAGE  = "voyage"   # via Voyage AI (Anthropic-aligned)
    OPENAI  = "openai"


@dataclass(frozen=True)
class EmbeddingConfig:
    provider:   EmbeddingProvider
    model:      str
    dimensions: int   # must match pgvector column definition


# One config per provider — dimensions are fixed per model
EMBEDDING_CONFIGS: dict[EmbeddingProvider, EmbeddingConfig] = {
    EmbeddingProvider.VOYAGE: EmbeddingConfig(
        provider   = EmbeddingProvider.VOYAGE,
        model      = "voyage-3",
        dimensions = 1024,
    ),
    EmbeddingProvider.OPENAI: EmbeddingConfig(
        provider   = EmbeddingProvider.OPENAI,
        model      = "text-embedding-3-small",
        dimensions = 1536,
    ),
}


class EmbeddingError(Exception):
    """Base for all embedding errors."""

class EmbeddingRateLimitError(EmbeddingError):
    """429 — back off and retry."""

class EmbeddingProviderError(EmbeddingError):
    """Non-retryable provider error."""

class EmbeddingTimeoutError(EmbeddingError):
    """Call exceeded timeout."""