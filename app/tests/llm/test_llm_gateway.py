"""
test_llm_gateway.py
===================
Tests for LLMGateway — model map, output mode, quota enforcement,
from_env, and call_with_data (prompt-layer integration).
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from app.services.llm.llm_client import LLMClient
from app.services.llm.llm_gateway import (
    LLMGateway,
    MODEL_MAP,
    OUTPUT_MODE_MAP,
    MAX_TOKENS_MAP,
)
from app.services.llm.types import (
    LLMQuotaExceededError,   # now lives in types.py
    LLMResponse,
    OutputMode,
    Provider,
    UseCase,
)
from app.prompts.types import (
    ClassifierData,
    RagChatData,
    DocGenData,
    RevenueEntry,
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


def _make_gateway_with_tracker(provider=Provider.ANTHROPIC):
    """Returns (gateway, tracker) — tracker accumulates LLMRequest objects."""
    tracker = []
    mock = make_mock_provider(provider=provider, call_count_tracker=tracker)
    client = LLMClient(provider=mock, timeout_seconds=5.0, max_retries=0)
    gw = LLMGateway(client=client, provider=provider)
    return gw, tracker


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def classifier_data():
    return ClassifierData(question="What was my revenue last month?")


@pytest.fixture
def rag_data():
    return RagChatData(
        business_id="salon_123",
        business_type="Hair Salon",
        analysis_period="March 2026",
        question="Why did revenue drop?",
        revenue=[
            RevenueEntry(period="Feb 2026", amount=12_000),
            RevenueEntry(period="Mar 2026", amount=9_500, change_pct=-20.8),
        ],
    )


@pytest.fixture
def doc_gen_data():
    return DocGenData(
        business_id="salon_123",
        business_type="Hair Salon",
        period="March 2026",
        doc_domain="revenue",
        doc_type="monthly_summary",
        kpi_block="Revenue: $9,500\nPrior: $12,000",
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

    def test_openai_rag_chat_uses_gpt4o_family(self):
        assert "gpt-4o" in MODEL_MAP[Provider.OPENAI][UseCase.RAG_CHAT]

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
# Gateway.call — model resolution (existing behaviour, unchanged)
# ---------------------------------------------------------------------------

class TestGatewayCall:

    async def test_call_resolves_correct_anthropic_model_for_rag(self):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")
        assert tracker[0].model == MODEL_MAP[Provider.ANTHROPIC][UseCase.RAG_CHAT]

    async def test_call_resolves_correct_openai_model_for_classifier(self):
        gw, tracker = _make_gateway_with_tracker(provider=Provider.OPENAI)
        await gw.call(UseCase.CLASSIFIER, "sys", "user", "salon_123")
        assert tracker[0].model == MODEL_MAP[Provider.OPENAI][UseCase.CLASSIFIER]

    async def test_call_resolves_output_mode(self):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")
        assert tracker[0].output_mode == OutputMode.STRUCTURED_JSON

    async def test_max_tokens_override(self):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123", max_tokens=500)
        assert tracker[0].max_tokens == 500

    async def test_default_max_tokens_from_map(self):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call(UseCase.CLASSIFIER, "sys", "user", "salon_123")
        assert tracker[0].max_tokens == MAX_TOKENS_MAP[UseCase.CLASSIFIER]

    async def test_business_id_passed_through(self):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call(UseCase.RAG_CHAT, "sys", "user", "specific_tenant_42")
        assert tracker[0].business_id == "specific_tenant_42"

    async def test_returns_llm_response(self):
        gw = _make_gateway()
        result = await gw.call(UseCase.RAG_CHAT, "sys", "user", "salon_123")
        assert isinstance(result, LLMResponse)


# ---------------------------------------------------------------------------
# Gateway.call_with_data — prompt dispatch integration
# ---------------------------------------------------------------------------

class TestCallWithData:

    # --- delegates to call() -------------------------------------------------

    async def test_call_with_data_returns_llm_response(self, rag_data):
        gw = _make_gateway()
        result = await gw.call_with_data(UseCase.RAG_CHAT, rag_data, "salon_123")
        assert isinstance(result, LLMResponse)

    async def test_call_with_data_uses_correct_use_case_model(self, rag_data):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call_with_data(UseCase.RAG_CHAT, rag_data, "salon_123")
        assert tracker[0].model == MODEL_MAP[Provider.ANTHROPIC][UseCase.RAG_CHAT]

    async def test_call_with_data_passes_business_id(self, rag_data):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call_with_data(UseCase.RAG_CHAT, rag_data, "tenant_xyz")
        assert tracker[0].business_id == "tenant_xyz"

    async def test_call_with_data_max_tokens_override(self, rag_data):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call_with_data(UseCase.RAG_CHAT, rag_data, "salon_123", max_tokens=333)
        assert tracker[0].max_tokens == 333

    async def test_call_with_data_default_max_tokens(self, classifier_data):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call_with_data(UseCase.CLASSIFIER, classifier_data, "salon_123")
        assert tracker[0].max_tokens == MAX_TOKENS_MAP[UseCase.CLASSIFIER]

    # --- prompt content is non-empty -----------------------------------------

    async def test_call_with_data_produces_non_empty_system(self, rag_data):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call_with_data(UseCase.RAG_CHAT, rag_data, "salon_123")
        assert len(tracker[0].system.strip()) > 0

    async def test_call_with_data_produces_non_empty_user(self, rag_data):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call_with_data(UseCase.RAG_CHAT, rag_data, "salon_123")
        assert len(tracker[0].user.strip()) > 0

    async def test_call_with_data_question_appears_in_user_prompt(self, rag_data):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call_with_data(UseCase.RAG_CHAT, rag_data, "salon_123")
        assert rag_data.question in tracker[0].user

    async def test_call_with_data_classifier_question_in_prompt(self, classifier_data):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call_with_data(UseCase.CLASSIFIER, classifier_data, "salon_123")
        # Question must appear somewhere in system or user
        full_prompt = tracker[0].system + tracker[0].user
        assert classifier_data.question in full_prompt

    async def test_call_with_data_doc_gen_period_in_prompt(self, doc_gen_data):
        gw, tracker = _make_gateway_with_tracker()
        await gw.call_with_data(UseCase.DOC_GENERATION, doc_gen_data, "salon_123")
        full_prompt = tracker[0].system + tracker[0].user
        assert doc_gen_data.period in full_prompt

    # --- provider switch changes prompt style --------------------------------

    async def test_anthropic_rag_prompt_uses_xml_tags(self, rag_data):
        gw, tracker = _make_gateway_with_tracker(provider=Provider.ANTHROPIC)
        await gw.call_with_data(UseCase.RAG_CHAT, rag_data, "salon_123")
        full_prompt = tracker[0].system + tracker[0].user
        assert "<rules>" in full_prompt or "<role>" in full_prompt

    async def test_openai_rag_prompt_uses_markdown_headers(self, rag_data):
        gw, tracker = _make_gateway_with_tracker(provider=Provider.OPENAI)
        await gw.call_with_data(UseCase.RAG_CHAT, rag_data, "salon_123")
        full_prompt = tracker[0].system + tracker[0].user
        assert "##" in full_prompt

    async def test_anthropic_classifier_prompt_has_xml_question_tag(self, classifier_data):
        gw, tracker = _make_gateway_with_tracker(provider=Provider.ANTHROPIC)
        await gw.call_with_data(UseCase.CLASSIFIER, classifier_data, "salon_123")
        assert "<question>" in tracker[0].user

    async def test_openai_classifier_user_is_plain_question(self, classifier_data):
        gw, tracker = _make_gateway_with_tracker(provider=Provider.OPENAI)
        await gw.call_with_data(UseCase.CLASSIFIER, classifier_data, "salon_123")
        # OpenAI classifier sends the raw question as user, no XML wrapping
        assert tracker[0].user == classifier_data.question

    # --- quota is still enforced via call_with_data --------------------------

    async def test_call_with_data_respects_quota(self, rag_data):
        async def store(business_id): return 50_001
        mock = make_mock_provider()
        client = LLMClient(provider=mock, timeout_seconds=5.0, max_retries=0)
        gw = LLMGateway(client=client, provider=Provider.ANTHROPIC,
                        daily_token_quota=50_000, quota_store=store)
        with pytest.raises(LLMQuotaExceededError):
            await gw.call_with_data(UseCase.RAG_CHAT, rag_data, "salon_123")


# ---------------------------------------------------------------------------
# _build_prompt — unit tests for the internal dispatch method
# ---------------------------------------------------------------------------

class TestBuildPrompt:
    """
    Tests _build_prompt in isolation — no LLM call is made.
    Verifies gateway dispatches to the right prompt module per provider.
    """

    def _gw(self, provider: Provider) -> LLMGateway:
        mock = make_mock_provider(provider=provider)
        client = LLMClient(provider=mock, timeout_seconds=5.0, max_retries=0)
        return LLMGateway(client=client, provider=provider)

    def test_returns_two_strings(self, rag_data):
        gw = self._gw(Provider.ANTHROPIC)
        system, user = gw._build_prompt(UseCase.RAG_CHAT, rag_data)
        assert isinstance(system, str) and isinstance(user, str)

    def test_system_is_non_empty_for_all_use_cases(
        self, classifier_data, rag_data, doc_gen_data
    ):
        gw = self._gw(Provider.ANTHROPIC)
        for use_case, data in [
            (UseCase.CLASSIFIER, classifier_data),
            (UseCase.RAG_CHAT, rag_data),
            (UseCase.DOC_GENERATION, doc_gen_data),
        ]:
            system, _ = gw._build_prompt(use_case, data)
            assert len(system.strip()) > 0, f"Empty system for {use_case}"

    def test_anthropic_and_openai_produce_different_system_prompts(self, rag_data):
        gw_a = self._gw(Provider.ANTHROPIC)
        gw_o = self._gw(Provider.OPENAI)
        sys_a, _ = gw_a._build_prompt(UseCase.RAG_CHAT, rag_data)
        sys_o, _ = gw_o._build_prompt(UseCase.RAG_CHAT, rag_data)
        assert sys_a != sys_o

    def test_build_prompt_called_with_lazy_import(self, rag_data):
        """_build_prompt must use a lazy import — not a module-level one."""
        gw = self._gw(Provider.ANTHROPIC)
        # Patch the import target — if it's module-level this would have no effect
        with patch("app.prompts.build_prompt") as mock_build:
            mock_build.return_value = ("sys", "usr")
            gw._build_prompt(UseCase.RAG_CHAT, rag_data)
            mock_build.assert_called_once_with(
                UseCase.RAG_CHAT, Provider.ANTHROPIC, rag_data
            )


# ---------------------------------------------------------------------------
# Quota enforcement (existing behaviour, unchanged)
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
        monkeypatch.setenv("LLM_PROVIDER",    "anthropic")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setenv("LLM_TIMEOUT",     "3.5")
        gw = LLMGateway.from_env()
        assert gw._client._timeout == 3.5

    def test_custom_max_retries_from_env(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER",      "anthropic")
        monkeypatch.setenv("ANTHROPIC_API_KEY",  "test-key")
        monkeypatch.setenv("LLM_MAX_RETRIES",   "5")
        gw = LLMGateway.from_env()
        assert gw._client._max_retries == 5