"""
test_types.py
=============
Tests for shared types — enums, dataclasses, error hierarchy.
"""
from app.services.llm.types import (
    UseCase, OutputMode, Provider, LLMResponse,
    TokenUsage, LLMError, LLMTimeoutError, LLMRateLimitError,
    LLMProviderError, LLMJsonParseError,
)


class TestEnums:

    def test_all_use_cases_defined(self):
        assert UseCase.CLASSIFIER
        assert UseCase.RAG_CHAT
        assert UseCase.DOC_GENERATION
        assert UseCase.AGENT

    def test_use_case_is_str(self):
        assert isinstance(UseCase.RAG_CHAT, str)

    def test_all_output_modes_defined(self):
        assert OutputMode.RAW
        assert OutputMode.STRUCTURED_JSON
        assert OutputMode.TOOL_CALLS

    def test_all_providers_defined(self):
        assert Provider.ANTHROPIC
        assert Provider.OPENAI

    def test_provider_values(self):
        assert Provider.ANTHROPIC == "anthropic"
        assert Provider.OPENAI    == "openai"


class TestTokenUsage:

    def test_total_property(self):
        usage = TokenUsage(input_tokens=500, output_tokens=150)
        assert usage.total == 650

    def test_total_zero(self):
        usage = TokenUsage(0, 0)
        assert usage.total == 0


class TestLLMRequest:

    def test_defaults(self, rag_request):
        assert rag_request.max_tokens == 1000
        assert rag_request.temperature == 0.2

    def test_fields_accessible(self, rag_request):
        assert rag_request.business_id == "salon_123"
        assert rag_request.use_case == UseCase.RAG_CHAT
        assert rag_request.output_mode == OutputMode.STRUCTURED_JSON


class TestLLMResponse:

    def test_fields_accessible(self):
        r = LLMResponse(
            content="hello",
            parsed={"key": "val"},
            usage=TokenUsage(100, 50),
            model="claude-sonnet-4-6",
            provider=Provider.ANTHROPIC,
            latency_ms=200.0,
            was_retried=False,
            use_case=UseCase.RAG_CHAT,
        )
        assert r.content == "hello"
        assert r.parsed == {"key": "val"}
        assert r.usage.total == 150
        assert r.was_retried is False


class TestErrorHierarchy:

    def test_all_errors_inherit_llm_error(self):
        assert issubclass(LLMTimeoutError,   LLMError)
        assert issubclass(LLMRateLimitError, LLMError)
        assert issubclass(LLMProviderError,  LLMError)
        assert issubclass(LLMJsonParseError, LLMError)

    def test_json_parse_error_carries_raw_content(self):
        err = LLMJsonParseError("bad json", raw_content='{"broken":')
        assert err.raw_content == '{"broken":'
        assert "bad json" in str(err)
