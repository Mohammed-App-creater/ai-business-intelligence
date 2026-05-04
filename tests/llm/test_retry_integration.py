# app/tests/llm/test_retry_integration.py
import pytest
import httpx
from unittest.mock import patch, AsyncMock
from app.services.llm.anthropic_provider import AnthropicProvider
from app.services.llm.llm_client import LLMClient
from app.services.llm.types import LLMRetryableError

@pytest.mark.asyncio
async def test_real_sdk_rate_limit_triggers_retry(rag_request):
    """Patch the SDK transport to return 429s, verify our retry layer kicks in."""
    provider = AnthropicProvider(api_key="test", model="claude-3-5-sonnet-latest")
    client = LLMClient(provider=provider, timeout_seconds=5.0, max_retries=2)

    call_count = 0
    async def fake_post(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return httpx.Response(
            429,
            headers={"retry-after": "1"},
            json={"type": "error", "error": {"type": "rate_limit_error", "message": "rate limited"}},
            request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"),
        )

    with patch.object(provider._client._client, "send", side_effect=fake_post):
        with pytest.raises(LLMRetryableError):
            await client.call(rag_request)

    # 1 + max_retries = 3 attempts total. SDK retries are off.
    assert call_count == 3