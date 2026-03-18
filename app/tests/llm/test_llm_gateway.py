"""
test_llm_gateway.py
===================
Tests for LLMGateway — model map, output mode, quota enforcement, from_env.
"""
from __future__ import annotations


import pytest

from app.services.llm.llm_client import LLMClient
from app.services.llm.llm_gateway import (
    LLMGateway, LLMQuotaExceededError, MODEL_MAP, OUTPUT_MODE_MAP, MAX_TOKENS_MAP
)
from app.services.llm.types import (
    LLMResponse, OutputMode, Provider, UseCase,
)
from conftest import make_mock_provider


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_gateway(provider=Provider.ANTHROPIC, daily_quota=None, quota_store=None):
    mock = make_mock_provider(provider=provider)
    client = LLMClient(provider=mock, timeout_seconds=5.0, max_retries=0)
    return LLMGateway(
        client=client,
        provider=provider,
        daily_token_quota=daily_quota,
        quota_store=quota_store,
    )


# ---------------------------------------------------------------------------
# Model map correctness
# ---------------------------------------------------------------------------

class TestModelMap:

    def test_anthropic_rag_chat_is_sonnet(self):
        assert "sonnet" in MODEL_MAP[Provider.ANTHROPIC][UseCase.RAG_CHAT].lower()

    def test_anthropic_classifier_is_haiku(self):
        assert "haiku" in MODEL_MAP[Provider.ANTHROPIC][UseCase.CLASSIFIER].lower()

    def test_anthropic_doc_generation_is_haiku(self):
        assert "haiku" in MODEL_MAP[Provider.ANTHROPIC][UseCase.DOC_GENERATION].lower()

    def test_openai_rag_chat_is_gpt41(self):
        assert "gpt-4.1" in MODEL_MAP[Provider.OPENAI][UseCase.RAG_CHAT]

    def test_openai_classifier_is_mini(self):
        assert "mini" in MODEL_MAP[Provider.OPENAI][UseCase.CLASSIFIER]

    def test_all_use_cases_covered_for_both_providers(self):
        for provider in Provider:
            for use_case in UseCase:
                assert use_case in MODEL_MAP[provider], (
                    f"Missing model for {provider}/{use_case}"
                )


# ---------------------------------------------------------------------------
# Output mode map
# ---------------------------------------------------------------------------

class TestOutputModeMap:

    def test_rag_chat_is_structured_json(self):
        assert OUTPUT_MODE_MAP[UseCase.RAG_CHAT] == OutputMode.STRUCTURED_JSON

    def test_classifier_is_raw(self):
        assert OUTPUT_MODE_MAP[UseCase.CLASSIFIER] == OutputMode.RAW

    def test_doc_generation_is_raw(self):
        assert OUTPUT_MODE_MAP[UseCase.DOC_GENERATION] == OutputMode.RAW

    def test_agent_is_tool_calls(self):
        assert OUTPUT_MODE_MAP[UseCase.AGENT] == OutputMode.TOOL_CALLS

    def test_all_use_cases_have_output_mode(self):
        for use_case in UseCase:
            assert use_case in OUTPUT_MODE_MAP


# ---------------------------------------------------------------------------
# Gateway call — model resolution
# ---------------------------------------------------------------------------

class TestGatewayCall:

    async def test_call_resolves_correct_anthropic_model_for_rag(self):
        tracker = []
        provider = make_mock_provider(call_count_tracker=tracker)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=0)
        gw = LLMGateway(client=client, provider=Provider.ANTHROPIC)
        await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")
        assert tracker[0].model == MODEL_MAP[Provider.ANTHROPIC][UseCase.RAG_CHAT]

    async def test_call_resolves_correct_openai_model_for_classifier(self):
        tracker = []
        provider = make_mock_provider(provider=Provider.OPENAI, call_count_tracker=tracker)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=0)
        gw = LLMGateway(client=client, provider=Provider.OPENAI)
        await gw.call(UseCase.CLASSIFIER, "sys", "user", "salon_123")
        assert tracker[0].model == MODEL_MAP[Provider.OPENAI][UseCase.CLASSIFIER]

    async def test_call_resolves_output_mode(self):
        tracker = []
        provider = make_mock_provider(call_count_tracker=tracker)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=0)
        gw = LLMGateway(client=client, provider=Provider.ANTHROPIC)
        await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")
        assert tracker[0].output_mode == OutputMode.STRUCTURED_JSON

    async def test_max_tokens_override(self):
        tracker = []
        provider = make_mock_provider(call_count_tracker=tracker)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=0)
        gw = LLMGateway(client=client, provider=Provider.ANTHROPIC)
        await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123", max_tokens=500)
        assert tracker[0].max_tokens == 500

    async def test_default_max_tokens_from_map(self):
        tracker = []
        provider = make_mock_provider(call_count_tracker=tracker)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=0)
        gw = LLMGateway(client=client, provider=Provider.ANTHROPIC)
        await gw.call(UseCase.CLASSIFIER, "sys", "user", "salon_123")
        assert tracker[0].max_tokens == MAX_TOKENS_MAP[UseCase.CLASSIFIER]

    async def test_business_id_passed_through(self):
        tracker = []
        provider = make_mock_provider(call_count_tracker=tracker)
        client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=0)
        gw = LLMGateway(client=client, provider=Provider.ANTHROPIC)
        await gw.call(UseCase.RAG_CHAT, "sys", "user", "specific_tenant_42")
        assert tracker[0].business_id == "specific_tenant_42"

    async def test_returns_llm_response(self):
        gw = _make_gateway()
        result = await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")
        assert isinstance(result, LLMResponse)


# ---------------------------------------------------------------------------
# Quota enforcement
# ---------------------------------------------------------------------------

class TestQuotaEnforcement:

    async def test_quota_not_exceeded_allows_call(self):
        async def store(business_id): return 1000
        gw = _make_gateway(daily_quota=50_000, quota_store=store)
        result = await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")
        assert result is not None

    async def test_quota_exceeded_raises_error(self):
        async def store(business_id): return 50_001
        gw = _make_gateway(daily_quota=50_000, quota_store=store)
        with pytest.raises(LLMQuotaExceededError):
            await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")

    async def test_quota_exactly_at_limit_raises_error(self):
        async def store(business_id): return 50_000
        gw = _make_gateway(daily_quota=50_000, quota_store=store)
        with pytest.raises(LLMQuotaExceededError):
            await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")

    async def test_no_quota_store_skips_check(self):
        gw = _make_gateway(daily_quota=50_000, quota_store=None)
        result = await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")
        assert result is not None

    async def test_none_quota_skips_check(self):
        async def store(business_id): return 999_999
        gw = _make_gateway(daily_quota=None, quota_store=store)
        result = await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")
        assert result is not None

    async def test_quota_store_failure_does_not_block(self):
        async def broken_store(business_id):
            raise ConnectionError("Redis down")
        gw = _make_gateway(daily_quota=50_000, quota_store=broken_store)
        # Must not raise — quota store failure is non-fatal
        result = await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")
        assert result is not None


# ---------------------------------------------------------------------------
# from_env factory
# ---------------------------------------------------------------------------

class TestFromEnv:

    def test_unknown_provider_raises_value_error(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "gemini")
        with pytest.raises(ValueError, match="LLM_PROVIDER"):
            LLMGateway.from_env()

    def test_defaults_to_anthropic(self, monkeypatch):
        monkeypatch.delenv("LLM_PROVIDER", raising=False)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        gw = LLMGateway.from_env()
        assert gw._provider == Provider.ANTHROPIC

    def test_openai_provider_from_env(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
        gw = LLMGateway.from_env()
        assert gw._provider == Provider.OPENAI

    def test_custom_timeout_from_env(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER",  "anthropic")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setenv("LLM_TIMEOUT",   "3.5")
        gw = LLMGateway.from_env()
        assert gw._client._timeout == 3.5

    def test_custom_max_retries_from_env(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER",    "anthropic")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setenv("LLM_MAX_RETRIES", "5")
        gw = LLMGateway.from_env()
        assert gw._client._max_retries == 5
