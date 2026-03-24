# app/tests/embeddings/conftest.py
from __future__ import annotations

import pytest
from app.services.embeddings.types import (
    EmbeddingConfig, EmbeddingProvider, EmbeddingRateLimitError,
)
from app.services.embeddings.embedding_client import EmbeddingClient


FAKE_DIMS = 4   # small fixed dimension for all test vectors


def make_mock_provider(
    dims:              int            = FAKE_DIMS,
    raises:            Exception | None = None,
    call_count_tracker: list | None   = None,
    max_batch:         int            = 128,
):
    class MockProvider:
        def __init__(self):
            self.calls = call_count_tracker if call_count_tracker is not None else []

        @property
        def provider_name(self) -> str:
            return "mock"

        @property
        def max_batch_size(self) -> int:
            return max_batch

        async def embed(self, texts: list[str]) -> list[list[float]]:
            self.calls.append(texts)
            if raises:
                raise raises
            return [[0.1 * (i + 1)] * dims for i in range(len(texts))]

    return MockProvider()


def make_rate_limit_then_succeed_provider(fail_times: int = 1, dims: int = FAKE_DIMS):
    count = [0]

    class Provider:
        @property
        def provider_name(self): return "mock"

        @property
        def max_batch_size(self): return 128

        async def embed(self, texts):
            count[0] += 1
            if count[0] <= fail_times:
                raise EmbeddingRateLimitError("429")
            return [[0.1] * dims for _ in texts]

    return Provider(), count


def make_client(provider=None, dims=FAKE_DIMS, timeout=5.0, max_retries=3):
    if provider is None:
        provider = make_mock_provider(dims=dims)
    config = EmbeddingConfig(
        provider=EmbeddingProvider.VOYAGE,
        model="voyage-3",
        dimensions=dims,
    )
    return EmbeddingClient(provider, config, timeout_seconds=timeout, max_retries=max_retries)