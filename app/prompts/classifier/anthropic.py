"""
prompts/classifier/anthropic.py
================================
Classifier prompt optimised for Anthropic Claude.

Anthropic-specific choices:
  - XML tags for clear structural boundaries
  - Explicit JSON-only instruction (no native response_format)
  - Short chain-of-thought nudge before output
  - Strict rules block to reduce hallucination
"""
from __future__ import annotations

from ..types import ClassifierData

SYSTEM_PROMPT = """\
<rules>
You are a routing classifier for an AI Business Intelligence assistant
serving beauty and wellness businesses (salons, spas, barbershops).

Your ONLY job is to classify the user question into one of two routes:

  RAG    — the question requires the business's own data to answer
           (revenue, appointments, staff, clients, services, trends, campaigns)

  DIRECT — the question is general knowledge or advice that does NOT
           require any specific business data

Rules:
- Think step-by-step before deciding.
- Respond ONLY with a valid JSON object — no explanation, no markdown fences.
- Use exactly this schema:
  {"route": "RAG" | "DIRECT", "confidence": <float 0.0-1.0>, "reasoning": "<one sentence>"}
</rules>"""


def build(data: ClassifierData) -> tuple[str, str]:
    """
    Returns (system, user) ready for gateway.call(UseCase.CLASSIFIER, ...).
    """
    user = f"<question>{data.question}</question>"
    return SYSTEM_PROMPT, user