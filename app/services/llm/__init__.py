"""
app.services.llm
================
Public API for the LLM layer.

Import from here — not from individual submodules.

    from app.services.llm import LLMGateway, UseCase, LLMResponse
"""
from .llm_gateway import LLMGateway, LLMQuotaExceededError
from .types import (
    LLMError,
    LLMJsonParseError,
    LLMProviderError,
    LLMRateLimitError,
    LLMResponse,
    LLMTimeoutError,
    OutputMode,
    Provider,
    TokenUsage,
    UseCase,
)

__all__ = [
    "LLMGateway",
    "LLMQuotaExceededError",
    "LLMError",
    "LLMJsonParseError",
    "LLMProviderError",
    "LLMRateLimitError",
    "LLMResponse",
    "LLMTimeoutError",
    "OutputMode",
    "Provider",
    "TokenUsage",
    "UseCase",
]
