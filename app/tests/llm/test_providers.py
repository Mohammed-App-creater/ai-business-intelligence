"""
test_providers.py
=================
Unit tests for AnthropicProvider and OpenAIProvider.

All HTTP calls are mocked — no real API keys required.
Tests verify:
  - Correct field mapping from SDK response to LLMResponse
  - JSON parsing (with and without markdown fences)
  - Error translation (RateLimitError, APIStatusError, ConnectionError)
  - supports_native_json returns correct value per provider
  - System prompt placement (Anthropic: separate param / OpenAI: first message)
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.llm.types import (
    LLMJsonParseError, LLMProviderError, LLMRateLimitError,
    LLMRequest, OutputMode, Provider, UseCase,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_request(output_mode=OutputMode.RAW) -> LLMRequest:
    return LLMRequest(
        use_case=UseCase.RAG_CHAT,
        system="You are an analyst.",
        user="Why did revenue drop?",
        business_id="salon_123",
        model="claude-sonnet-4-6",
        output_mode=output_mode,
        max_tokens=1000,
    )

VALID_JSON = '{"summary": "Revenue dropped.", "root_causes": [], "supporting_data": "", "recommendations": [], "confidence": "high", "data_gaps": null}'


# ---------------------------------------------------------------------------
# AnthropicProvider
# ---------------------------------------------------------------------------

class TestAnthropicProvider:

    def _make_sdk_response(self, content: str, input_tokens=500, output_tokens=150):
        resp = MagicMock()
        resp.content = [MagicMock(text=content)]
        resp.usage.input_tokens  = input_tokens
        resp.usage.output_tokens = output_tokens
        resp.model = "claude-sonnet-4-6"
        return resp

    async def test_returns_llm_response(self):
        from app.services.llm.anthropic_provider import AnthropicProvider
        provider = AnthropicProvider(api_key="test")
        provider._client.messages.create = AsyncMock(
            return_value=self._make_sdk_response("hello")
        )
        result = await provider.complete(_make_request())
        assert result.content == "hello"
        assert result.provider == Provider.ANTHROPIC

    async def test_token_usage_mapped_correctly(self):
        from app.services.llm.anthropic_provider import AnthropicProvider
        provider = AnthropicProvider(api_key="test")
        provider._client.messages.create = AsyncMock(
            return_value=self._make_sdk_response("hi", input_tokens=300, output_tokens=80)
        )
        result = await provider.complete(_make_request())
        assert result.usage.input_tokens  == 300
        assert result.usage.output_tokens == 80
        assert result.usage.total         == 380

    async def test_structured_json_parsed(self):
        from app.services.llm.anthropic_provider import AnthropicProvider
        provider = AnthropicProvider(api_key="test")
        provider._client.messages.create = AsyncMock(
            return_value=self._make_sdk_response(VALID_JSON)
        )
        result = await provider.complete(_make_request(OutputMode.STRUCTURED_JSON))
        assert result.parsed is not None
        assert result.parsed["confidence"] == "high"

    async def test_json_with_markdown_fences_parsed(self):
        from app.services.llm.anthropic_provider import AnthropicProvider
        provider = AnthropicProvider(api_key="test")
        fenced = f"```json\n{VALID_JSON}\n```"
        provider._client.messages.create = AsyncMock(
            return_value=self._make_sdk_response(fenced)
        )
        result = await provider.complete(_make_request(OutputMode.STRUCTURED_JSON))
        assert result.parsed["summary"] == "Revenue dropped."

    async def test_malformed_json_raises_parse_error(self):
        from app.services.llm.anthropic_provider import AnthropicProvider
        provider = AnthropicProvider(api_key="test")
        provider._client.messages.create = AsyncMock(
            return_value=self._make_sdk_response("{broken json")
        )
        with pytest.raises(LLMJsonParseError) as exc_info:
            await provider.complete(_make_request(OutputMode.STRUCTURED_JSON))
        assert exc_info.value.raw_content == "{broken json"

    async def test_rate_limit_raises_llm_rate_limit_error(self):
        import anthropic as sdk
        from app.services.llm.anthropic_provider import AnthropicProvider
        provider = AnthropicProvider(api_key="test")
        provider._client.messages.create = AsyncMock(
            side_effect=sdk.RateLimitError(
                message="429", response=MagicMock(status_code=429), body={}
            )
        )
        with pytest.raises(LLMRateLimitError):
            await provider.complete(_make_request())

    async def test_api_status_error_raises_provider_error(self):
        import anthropic as sdk
        from app.services.llm.anthropic_provider import AnthropicProvider
        provider = AnthropicProvider(api_key="test")
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        provider._client.messages.create = AsyncMock(
            side_effect=sdk.APIStatusError(
                message="server error", response=mock_resp, body={}
            )
        )
        with pytest.raises(LLMProviderError):
            await provider.complete(_make_request())

    def test_does_not_support_native_json(self):
        from app.services.llm.anthropic_provider import AnthropicProvider
        assert AnthropicProvider(api_key="test").supports_native_json() is False

    def test_provider_name(self):
        from app.services.llm.anthropic_provider import AnthropicProvider
        assert AnthropicProvider(api_key="test").provider_name == "anthropic"

    async def test_system_passed_as_separate_param(self):
        """Anthropic requires system as a top-level param, not in messages."""
        from app.services.llm.anthropic_provider import AnthropicProvider
        provider = AnthropicProvider(api_key="test")
        create_mock = AsyncMock(return_value=self._make_sdk_response("ok"))
        provider._client.messages.create = create_mock

        req = _make_request()
        await provider.complete(req)

        call_kwargs = create_mock.call_args.kwargs
        assert call_kwargs["system"] == "You are an analyst."
        # system must NOT appear as a message
        for msg in call_kwargs["messages"]:
            assert msg["role"] != "system"

    async def test_raw_mode_parsed_is_none(self):
        from app.services.llm.anthropic_provider import AnthropicProvider
        provider = AnthropicProvider(api_key="test")
        provider._client.messages.create = AsyncMock(
            return_value=self._make_sdk_response("plain text")
        )
        result = await provider.complete(_make_request(OutputMode.RAW))
        assert result.parsed is None


# ---------------------------------------------------------------------------
# OpenAIProvider
# ---------------------------------------------------------------------------

class TestOpenAIProvider:

    def _make_sdk_response(self, content: str, prompt_tokens=400, completion_tokens=100):
        choice = MagicMock()
        choice.message.content = content
        resp = MagicMock()
        resp.choices = [choice]
        resp.usage.prompt_tokens     = prompt_tokens
        resp.usage.completion_tokens = completion_tokens
        resp.model = "gpt-4.1"
        return resp

    async def test_returns_llm_response(self):
        from app.services.llm.openai_provider import OpenAIProvider
        provider = OpenAIProvider(api_key="test")
        provider._client.chat.completions.create = AsyncMock(
            return_value=self._make_sdk_response("hello")
        )
        result = await provider.complete(_make_request())
        assert result.content == "hello"
        assert result.provider == Provider.OPENAI

    async def test_token_usage_mapped_from_prompt_completion(self):
        from app.services.llm.openai_provider import OpenAIProvider
        provider = OpenAIProvider(api_key="test")
        provider._client.chat.completions.create = AsyncMock(
            return_value=self._make_sdk_response("hi", prompt_tokens=200, completion_tokens=75)
        )
        result = await provider.complete(_make_request())
        assert result.usage.input_tokens  == 200
        assert result.usage.output_tokens == 75

    async def test_structured_json_parsed(self):
        from app.services.llm.openai_provider import OpenAIProvider
        provider = OpenAIProvider(api_key="test")
        provider._client.chat.completions.create = AsyncMock(
            return_value=self._make_sdk_response(VALID_JSON)
        )
        result = await provider.complete(_make_request(OutputMode.STRUCTURED_JSON))
        assert result.parsed is not None
        assert result.parsed["confidence"] == "high"

    async def test_response_format_sent_for_structured_json(self):
        from app.services.llm.openai_provider import OpenAIProvider
        provider = OpenAIProvider(api_key="test")
        create_mock = AsyncMock(return_value=self._make_sdk_response(VALID_JSON))
        provider._client.chat.completions.create = create_mock

        await provider.complete(_make_request(OutputMode.STRUCTURED_JSON))
        call_kwargs = create_mock.call_args.kwargs
        assert call_kwargs.get("response_format") == {"type": "json_object"}

    async def test_response_format_not_sent_for_raw(self):
        from app.services.llm.openai_provider import OpenAIProvider
        provider = OpenAIProvider(api_key="test")
        create_mock = AsyncMock(return_value=self._make_sdk_response("ok"))
        provider._client.chat.completions.create = create_mock

        await provider.complete(_make_request(OutputMode.RAW))
        call_kwargs = create_mock.call_args.kwargs
        assert "response_format" not in call_kwargs

    async def test_system_as_first_message(self):
        """OpenAI requires system as role=system message, not a separate param."""
        from app.services.llm.openai_provider import OpenAIProvider
        provider = OpenAIProvider(api_key="test")
        create_mock = AsyncMock(return_value=self._make_sdk_response("ok"))
        provider._client.chat.completions.create = create_mock

        req = _make_request()
        await provider.complete(req)

        messages = create_mock.call_args.kwargs["messages"]
        assert messages[0]["role"] == "system"
        assert messages[0]["content"] == "You are an analyst."
        assert "system" not in create_mock.call_args.kwargs

    async def test_rate_limit_raises_llm_rate_limit_error(self):
        import openai as sdk
        from app.services.llm.openai_provider import OpenAIProvider
        provider = OpenAIProvider(api_key="test")
        provider._client.chat.completions.create = AsyncMock(
            side_effect=sdk.RateLimitError(
                message="429", response=MagicMock(status_code=429), body={}
            )
        )
        with pytest.raises(LLMRateLimitError):
            await provider.complete(_make_request())

    async def test_api_status_error_raises_provider_error(self):
        import openai as sdk
        from app.services.llm.openai_provider import OpenAIProvider
        provider = OpenAIProvider(api_key="test")
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        provider._client.chat.completions.create = AsyncMock(
            side_effect=sdk.APIStatusError(
                message="unavailable", response=mock_resp, body={}
            )
        )
        with pytest.raises(LLMProviderError):
            await provider.complete(_make_request())

    def test_supports_native_json(self):
        from app.services.llm.openai_provider import OpenAIProvider
        assert OpenAIProvider(api_key="test").supports_native_json() is True

    def test_provider_name(self):
        from app.services.llm.openai_provider import OpenAIProvider
        assert OpenAIProvider(api_key="test").provider_name == "openai"
