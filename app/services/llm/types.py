"""
types.py
========
All shared types for the LLM layer.

These are the only types that cross module boundaries — providers,
client, gateway, and call sites all speak this language and nothing else.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Use cases — every place in the system that calls an LLM
# ---------------------------------------------------------------------------

class UseCase(str, Enum):
    CLASSIFIER      = "classifier"       # Query analyzer fallback (Section 5.4)
    RAG_CHAT        = "rag_chat"         # Main chat response      (Section 5.7)
    DOC_GENERATION  = "doc_generation"   # ETL narrative summaries (Section 6.5.1)
    AGENT           = "agent"            # V2 multi-step reasoning (Section 3.3)


# ---------------------------------------------------------------------------
# Output modes — how the response should be parsed
# ---------------------------------------------------------------------------

class OutputMode(str, Enum):
    RAW              = "raw"              # Return content string as-is
    STRUCTURED_JSON  = "structured_json"  # Parse and validate JSON response
    TOOL_CALLS       = "tool_calls"       # V2 agent tool use (stub)


# ---------------------------------------------------------------------------
# Provider identifiers
# ---------------------------------------------------------------------------

class Provider(str, Enum):
    ANTHROPIC = "anthropic"
    OPENAI    = "openai"


# ---------------------------------------------------------------------------
# Request / response types
# ---------------------------------------------------------------------------

@dataclass
class LLMRequest:
    """
    Fully resolved request — built by the gateway, consumed by the client.
    Call sites never construct this directly.
    """
    use_case:    UseCase
    system:      str
    user:        str
    business_id: str
    model:       str              # Resolved by gateway from MODEL_MAP
    output_mode: OutputMode       # Resolved by gateway from OUTPUT_MODE_MAP
    max_tokens:  int = 1_000
    temperature: float = 0.2      # Low for deterministic analytics answers


@dataclass
class TokenUsage:
    input_tokens:  int
    output_tokens: int

    @property
    def total(self) -> int:
        return self.input_tokens + self.output_tokens


@dataclass
class LLMResponse:
    """
    Normalised response returned to all call sites regardless of provider.
    """
    content:      str                    # Raw text from the model
    parsed:       Optional[dict]         # Populated when output_mode=STRUCTURED_JSON
    usage:        TokenUsage
    model:        str
    provider:     Provider
    latency_ms:   float
    was_retried:  bool
    use_case:     UseCase


# ---------------------------------------------------------------------------
# Errors — typed so call sites can catch specifically
# ---------------------------------------------------------------------------

class LLMError(Exception):
    """Base class for all LLM layer errors."""


class LLMTimeoutError(LLMError):
    """Request exceeded the configured hard timeout."""


class LLMRateLimitError(LLMError):
    """Provider returned 429 and retries were exhausted."""


class LLMProviderError(LLMError):
    """Provider returned a non-retryable error (4xx / 5xx)."""


class LLMJsonParseError(LLMError):
    """
    Model returned a response that could not be parsed as valid JSON.
    Only raised in STRUCTURED_JSON mode.
    """
    def __init__(self, message: str, raw_content: str):
        super().__init__(message)
        self.raw_content = raw_content
        
class LLMQuotaExceededError(Exception):
    """Tenant has exceeded their daily token quota."""