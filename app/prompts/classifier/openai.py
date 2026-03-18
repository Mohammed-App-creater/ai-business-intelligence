"""
prompts/classifier/openai.py
=============================
Classifier prompt optimised for OpenAI GPT.

OpenAI-specific choices:
  - Plain direct instructions — no XML tags needed
  - No explicit JSON instruction (response_format=json_object handles it)
  - Concise system prompt — GPT responds well to brevity
  - No chain-of-thought nudge needed for simple classification
"""
from __future__ import annotations

from ..types import ClassifierData

SYSTEM_PROMPT = """\
You are a routing classifier for a Business Intelligence assistant \
serving beauty and wellness businesses.

Classify the user question into exactly one of:
  RAG    — requires the business's own data (revenue, bookings, staff, clients, trends)
  DIRECT — general knowledge or advice, no business data needed

Respond with JSON: {"route": "RAG" | "DIRECT", "confidence": 0.0-1.0, "reasoning": "one sentence"}"""


def build(data: ClassifierData) -> tuple[str, str]:
    """
    Returns (system, user) ready for gateway.call(UseCase.CLASSIFIER, ...).
    response_format=json_object is applied by the gateway/provider automatically.
    """
    return SYSTEM_PROMPT, data.question