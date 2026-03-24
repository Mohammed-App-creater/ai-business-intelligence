# app/tests/embeddings/test_embedding_client.py
from __future__ import annotations

import pytest
from app.services.embeddings.embedding_client import EmbeddingClient
from app.services.embeddings.types import (
    EmbeddingProvider, EmbeddingRateLimitError,
    EmbeddingTimeoutError, EmbeddingProviderError,
)
from conftest import (
    make_mock_provider, make_client,
    make_rate_limit_then_succeed_provider, FAKE_DIMS,
)


# ---------------------------------------------------------------------------
# embed() — single text
# ---------------------------------------------------------------------------

class TestEmbed:

    async def test_returns_list_of_floats(self):
        client = make_client()
        result = await client.embed("hello")
        assert isinstance(result, list)
        assert all(isinstance(v, float) for v in result)

    async def test_returns_correct_dimensions(self):
        client = make_client(dims=FAKE_DIMS)
        result = await client.embed("hello")
        assert len(result) == FAKE_DIMS

    async def test_single_text_calls_provider_once(self):
        tracker = []
        client = make_client(provider=make_mock_provider(call_count_tracker=tracker))
        await client.embed("hello")
        assert len(tracker) == 1

    async def test_single_text_passes_text_to_provider(self):
        tracker = []
        client = make_client(provider=make_mock_provider(call_count_tracker=tracker))
        await client.embed("why did revenue drop?")
        assert tracker[0] == ["why did revenue drop?"]

    async def test_empty_string_is_accepted(self):
        client = make_client()
        result = await client.embed("")
        assert len(result) == FAKE_DIMS


# ---------------------------------------------------------------------------
# embed_batch() — multiple texts
# ---------------------------------------------------------------------------

class TestEmbedBatch:

    async def test_returns_one_vector_per_text(self):
        client = make_client()
        texts  = ["a", "b", "c"]
        result = await client.embed_batch(texts)
        assert len(result) == len(texts)

    async def test_each_vector_has_correct_dimensions(self):
        client = make_client(dims=FAKE_DIMS)
        result = await client.embed_batch(["a", "b"])
        for vec in result:
            assert len(vec) == FAKE_DIMS

    async def test_empty_list_returns_empty(self):
        client = make_client()
        result = await client.embed_batch([])
        assert result == []

    async def test_single_item_batch(self):
        client = make_client()
        result = await client.embed_batch(["only one"])
        assert len(result) == 1

    async def test_order_preserved(self):
        """Vectors come back in the same order as input texts."""
        tracker = []
        client  = make_client(provider=make_mock_provider(call_count_tracker=tracker))
        texts   = ["first", "second", "third"]
        result  = await client.embed_batch(texts)
        assert len(result) == 3
        # mock returns 0.1*(i+1) per position — first vec differs from third
        assert result[0] != result[2]

    async def test_large_batch_is_chunked(self):
        """Batches larger than max_batch_size must be split into multiple calls."""
        tracker = []
        provider = make_mock_provider(call_count_tracker=tracker, max_batch=3)
        client   = make_client(provider=provider)
        texts    = ["t"] * 7   # 7 texts, batch size 3 → 3 calls (3+3+1)
        result   = await client.embed_batch(texts)
        assert len(result) == 7
        assert len(tracker) == 3

    async def test_exact_batch_size_is_one_call(self):
        tracker  = []
        provider = make_mock_provider(call_count_tracker=tracker, max_batch=5)
        client   = make_client(provider=provider)
        result   = await client.embed_batch(["t"] * 5)
        assert len(result) == 5
        assert len(tracker) == 1

    async def test_batch_plus_one_is_two_calls(self):
        tracker  = []
        provider = make_mock_provider(call_count_tracker=tracker, max_batch=5)
        client   = make_client(provider=provider)
        result   = await client.embed_batch(["t"] * 6)
        assert len(result) == 6
        assert len(tracker) == 2


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------

class TestRetry:

    async def test_rate_limit_retries_and_succeeds(self):
        provider, count = make_rate_limit_then_succeed_provider(fail_times=1)
        client = make_client(provider=provider, max_retries=3)
        result = await client.embed("test")
        assert len(result) == FAKE_DIMS
        assert count[0] == 2   # 1 fail + 1 success

    async def test_retries_up_to_max(self):
        provider, count = make_rate_limit_then_succeed_provider(fail_times=3)
        client = make_client(provider=provider, max_retries=3)
        result = await client.embed("test")
        assert count[0] == 4   # 3 fails + 1 success

    async def test_exhausted_retries_raises_rate_limit_error(self):
        provider, _ = make_rate_limit_then_succeed_provider(fail_times=99)
        client = make_client(provider=provider, max_retries=2)
        with pytest.raises(EmbeddingRateLimitError):
            await client.embed("test")

    async def test_non_rate_limit_error_not_retried(self):
        tracker  = []
        provider = make_mock_provider(
            raises=EmbeddingProviderError("bad request"),
            call_count_tracker=tracker,
        )
        client = make_client(provider=provider, max_retries=3)
        with pytest.raises(EmbeddingProviderError):
            await client.embed("test")
        assert len(tracker) == 1   # no retries

    async def test_zero_retries_raises_immediately(self):
        provider, count = make_rate_limit_then_succeed_provider(fail_times=1)
        client = make_client(provider=provider, max_retries=0)
        with pytest.raises(EmbeddingRateLimitError):
            await client.embed("test")
        assert count[0] == 1


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------

class TestTimeout:

    async def test_timeout_raises_embedding_timeout_error(self):
        import asyncio

        class SlowProvider:
            @property
            def provider_name(self): return "slow"
            @property
            def max_batch_size(self): return 128
            async def embed(self, texts):
                await asyncio.sleep(10)
                return [[0.1] * FAKE_DIMS for _ in texts]

        from app.services.embeddings.types import EmbeddingConfig, EmbeddingProvider
        config = EmbeddingConfig(EmbeddingProvider.VOYAGE, "voyage-3", FAKE_DIMS)
        client = EmbeddingClient(SlowProvider(), config, timeout_seconds=0.01, max_retries=0)
        with pytest.raises(EmbeddingTimeoutError):
            await client.embed("test")


# ---------------------------------------------------------------------------
# dimensions property
# ---------------------------------------------------------------------------

class TestDimensions:

    def test_dimensions_matches_config(self):
        client = make_client(dims=1024)
        assert client.dimensions == 1024

    def test_dimensions_openai(self):
        from app.services.embeddings.types import EmbeddingConfig, EmbeddingProvider
        config = EmbeddingConfig(EmbeddingProvider.OPENAI, "text-embedding-3-small", 1536)
        client = EmbeddingClient(make_mock_provider(), config)
        assert client.dimensions == 1536


# ---------------------------------------------------------------------------
# from_env factory
# ---------------------------------------------------------------------------

class TestFromEnv:

    def test_unknown_provider_raises(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_PROVIDER", "cohere")
        with pytest.raises(ValueError, match="EMBEDDING_PROVIDER"):
            EmbeddingClient.from_env()

    def test_defaults_to_voyage(self, monkeypatch):
        monkeypatch.delenv("EMBEDDING_PROVIDER", raising=False)
        monkeypatch.setenv("VOYAGE_API_KEY", "test-key")
        client = EmbeddingClient.from_env()
        assert client.provider_name == "voyage"

    def test_openai_provider_from_env(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_PROVIDER", "openai")
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
        client = EmbeddingClient.from_env()
        assert client.provider_name == "openai"

    def test_dimensions_voyage(self, monkeypatch):
        monkeypatch.delenv("EMBEDDING_PROVIDER", raising=False)
        monkeypatch.setenv("VOYAGE_API_KEY", "test-key")
        client = EmbeddingClient.from_env()
        assert client.dimensions == 1024

    def test_dimensions_openai(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_PROVIDER", "openai")
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
        client = EmbeddingClient.from_env()
        assert client.dimensions == 1536

    def test_custom_timeout_from_env(self, monkeypatch):
        monkeypatch.delenv("EMBEDDING_PROVIDER", raising=False)
        monkeypatch.setenv("VOYAGE_API_KEY",       "test-key")
        monkeypatch.setenv("EMBEDDING_TIMEOUT",    "3.5")
        client = EmbeddingClient.from_env()
        assert client._timeout == 3.5

    def test_custom_retries_from_env(self, monkeypatch):
        monkeypatch.delenv("EMBEDDING_PROVIDER", raising=False)
        monkeypatch.setenv("VOYAGE_API_KEY",           "test-key")
        monkeypatch.setenv("EMBEDDING_MAX_RETRIES",    "5")
        client = EmbeddingClient.from_env()
        assert client._max_retries == 5
