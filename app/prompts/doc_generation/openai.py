"""
prompts/doc_generation/openai.py
==================================
Document generation prompt optimised for OpenAI GPT.

Same contract as anthropic.py — receives a pre-formatted KPI block,
returns a 2-3 sentence observation paragraph (OutputMode.RAW).
"""
from __future__ import annotations
from ..types import DocGenData

SYSTEM_PROMPT = """\
You write concise performance summaries for beauty and wellness business owners.
Summaries are stored and later retrieved to answer owner questions.

Rules:
- Write exactly 2-3 sentences.
- Plain language — owner is not a data analyst.
- Use only the data provided — no invented figures.
- Lead with the single most notable insight.
- Third person: "The business..." not "You..."
- Flowing prose only — no bullet points or headers.
- No advice or recommendations — observation only."""


def build(data: DocGenData) -> tuple[str, str]:
    """
    Returns (system, user) ready for gateway.call_with_data(UseCase.DOC_GENERATION, ...).
    """
    domain_label = data.doc_domain.replace("_", " ").title()
    type_label   = data.doc_type.replace("_", " ").title()

    lines = [
        f"Business ID   : {data.business_id}",
        f"Business Type : {data.business_type}",
        f"Period        : {data.period}",
        f"Domain        : {domain_label}",
        f"Summary Type  : {type_label}",
    ]
    if data.entity_name:
        lines.append(f"Entity        : {data.entity_name}")

    lines.append("")
    lines.append(data.kpi_block.strip())
    lines.append("")
    lines.append(
        f"Write a 2-3 sentence observation about this {domain_label} period. "
        f"Lead with the most notable insight from the data above."
    )

    user = "\n".join(lines)
    return SYSTEM_PROMPT, user
